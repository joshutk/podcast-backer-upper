#!/usr/bin/env python3
"""
Podcast Backer-Upper

A tool to backup podcast feeds locally with full metadata preservation.
Designed for archival and potential re-hosting purposes.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock
from urllib.parse import urlparse

import feedparser
import requests


@dataclass
class BackupStats:
    """Track statistics during backup."""
    downloaded: int = 0
    skipped_existing: int = 0
    metadata_only: int = 0
    errors: int = 0
    bytes_downloaded: int = 0
    _lock: Lock = field(default_factory=Lock)

    def increment(self, field_name: str, value: int = 1):
        with self._lock:
            setattr(self, field_name, getattr(self, field_name) + value)

    def summary(self) -> str:
        lines = [
            f"  Downloaded:      {self.downloaded} episodes ({format_size(self.bytes_downloaded)})",
            f"  Already existed: {self.skipped_existing} episodes",
            f"  Metadata only:   {self.metadata_only} episodes",
            f"  Errors/skipped:  {self.errors} episodes",
        ]
        return "\n".join(lines)


def format_size(bytes_count: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024
    return f"{bytes_count:.1f} PB"
from mutagen.id3 import (
    ID3, APIC, TIT2, TPE1, TALB, TYER, TDRC, COMM, TCON, TRCK, TPOS, TXXX, USLT
)
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3
from mutagen import File as MutagenFile

# Track user decisions for error handling
_error_decisions = {}  # Maps error type string to 'skip_all' or None


def get_error_key(error: Exception) -> str:
    """Extract a consistent key from an error for grouping similar errors."""
    error_str = str(error)
    # Extract the core error message (remove file-specific parts)
    if "can't sync to" in error_str.lower():
        return "sync_to_frame"
    if "not a valid" in error_str.lower():
        return "invalid_file"
    # Default: use the exception type
    return type(error).__name__


def handle_error_interactive(error: Exception, context: str, interactive: bool = True) -> str:
    """
    Handle an error interactively, asking user what to do.
    Returns: 'skip' to skip item, 'skip_all' to skip all of this type,
             'continue' to proceed without this component, 'abort' to stop.
    If interactive=False, always returns 'skip_all'.
    """
    error_key = get_error_key(error)

    # Non-interactive mode: always skip
    if not interactive:
        return 'skip_all'

    # Check if user already made a decision for this error type
    if error_key in _error_decisions:
        return _error_decisions[error_key]

    # First occurrence - ask the user
    print(f"\n  ERROR: {error}")
    print(f"  Context: {context}")
    print()
    print("  What would you like to do?")
    print("  [1] Skip this item entirely and continue")
    print("  [2] Skip all items with this error type")
    print("  [3] Save metadata anyway (no audio file)")
    print("  [4] Save metadata for all items with this error")
    print("  [5] Abort backup")
    print()

    while True:
        try:
            choice = input("  Enter choice (1-5): ").strip()
            if choice == '1':
                return 'skip'
            elif choice == '2':
                _error_decisions[error_key] = 'skip_all'
                return 'skip_all'
            elif choice == '3':
                return 'continue'
            elif choice == '4':
                _error_decisions[error_key] = 'continue'
                return 'continue'
            elif choice == '5':
                return 'abort'
            else:
                print("  Please enter 1, 2, 3, 4, or 5")
        except (EOFError, KeyboardInterrupt):
            print("\n  Aborting...")
            return 'abort'


def is_likely_audio_url(url: str) -> tuple[bool, str]:
    """
    Check if a URL looks like it points to an audio file.
    Returns (is_valid, reason) tuple.
    """
    if not url:
        return False, "No URL provided"

    url_lower = url.lower()

    # Check for common audio extensions
    audio_extensions = ('.mp3', '.m4a', '.aac', '.ogg', '.wav', '.flac', '.opus')
    has_audio_ext = any(ext in url_lower for ext in audio_extensions)

    # Check for suspicious patterns that indicate non-audio pages
    suspicious_patterns = [
        ('media.php', 'URL appears to be a PHP page, not an audio file'),
        ('pageID=', 'URL appears to be a webpage with page ID'),
        ('.html', 'URL appears to be an HTML page'),
        ('.htm', 'URL appears to be an HTML page'),
        ('view=', 'URL appears to be a webpage view'),
    ]

    for pattern, reason in suspicious_patterns:
        if pattern in url:
            return False, reason

    # If no audio extension and no suspicious patterns, it might still be valid
    # (some CDNs don't use extensions)
    if not has_audio_ext and '.' in url.split('/')[-1]:
        # Has some extension but not audio
        return False, "URL doesn't appear to have an audio file extension"

    return True, "OK"


def sanitize_filename(name: str, max_length: int = 100) -> str:
    """Create a safe filename from a string."""
    # Remove or replace problematic characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = name.replace(' ', '-')
    # Truncate if needed
    if len(name) > max_length:
        name = name[:max_length].rsplit('-', 1)[0]
    return name or 'untitled'


def download_file(url: str, dest_path: Path, chunk_size: int = 8192,
                  show_progress: bool = True) -> int:
    """
    Download a file with atomic write (safe for synced folders).
    Returns bytes downloaded, or -1 if file already existed.
    """
    if dest_path.exists():
        return -1

    # Use temp file for atomic write
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_fd, temp_path = tempfile.mkstemp(dir=dest_path.parent, suffix='.tmp')

    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0

        with os.fdopen(temp_fd, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                if show_progress and total_size:
                    pct = (downloaded / total_size) * 100
                    print(f"\r  Downloading: {pct:.1f}%", end='', flush=True)

        if show_progress:
            print()  # Newline after progress

        # Atomic rename
        shutil.move(temp_path, dest_path)
        return downloaded

    except Exception as e:
        # Clean up temp file on failure
        try:
            os.unlink(temp_path)
        except:
            pass
        raise e


def download_episode_parallel(args: tuple) -> dict:
    """Download a single episode (for parallel execution)."""
    ep_data, audio_path, stats = args
    result = {
        'ep_data': ep_data,
        'audio_path': audio_path,
        'success': False,
        'existed': False,
        'error': None,
        'bytes': 0,
    }

    try:
        bytes_downloaded = download_file(
            ep_data['enclosure_url'],
            audio_path,
            show_progress=False
        )
        if bytes_downloaded == -1:
            result['existed'] = True
            result['success'] = True
        else:
            result['success'] = True
            result['bytes'] = bytes_downloaded
    except Exception as e:
        result['error'] = str(e)

    return result


def download_image(url: str, dest_path: Path) -> bytes | None:
    """Download an image, save it, and return the bytes."""
    if not url:
        return None

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        image_data = response.content

        # Atomic write
        temp_fd, temp_path = tempfile.mkstemp(dir=dest_path.parent, suffix='.tmp')
        with os.fdopen(temp_fd, 'wb') as f:
            f.write(image_data)
        shutil.move(temp_path, dest_path)

        return image_data
    except Exception as e:
        print(f"  Warning: Could not download image: {e}")
        return None


def get_image_mime_type(url: str) -> str:
    """Guess image MIME type from URL."""
    url_lower = url.lower()
    if '.png' in url_lower:
        return 'image/png'
    elif '.gif' in url_lower:
        return 'image/gif'
    elif '.webp' in url_lower:
        return 'image/webp'
    return 'image/jpeg'  # Default to JPEG


def embed_metadata_full(audio_path: Path, episode: dict, channel: dict,
                        episode_num: int, total_episodes: int, artwork_data: bytes | None):
    """Embed full ID3 tags into an MP3 file (may fail on some files)."""
    try:
        audio = MP3(audio_path, ID3=ID3)
    except:
        audio = MP3(audio_path)

    # Create ID3 tag if it doesn't exist
    try:
        audio.add_tags()
    except:
        pass  # Tags already exist

    tags = audio.tags

    # Basic tags
    tags['TIT2'] = TIT2(encoding=3, text=episode.get('title', 'Unknown'))
    tags['TPE1'] = TPE1(encoding=3, text=episode.get('author', channel.get('author', 'Unknown')))
    tags['TALB'] = TALB(encoding=3, text=channel.get('title', 'Unknown Podcast'))
    tags['TCON'] = TCON(encoding=3, text=channel.get('category', 'Podcast'))
    tags['TRCK'] = TRCK(encoding=3, text=f"{episode_num}/{total_episodes}")

    # Date
    pub_date = episode.get('published_parsed')
    if pub_date:
        year = pub_date.tm_year
        tags['TYER'] = TYER(encoding=3, text=str(year))
        tags['TDRC'] = TDRC(encoding=3, text=str(year))

    # Description as comment
    description = episode.get('summary', episode.get('description', ''))
    if description:
        # Strip HTML for the comment
        clean_desc = re.sub(r'<[^>]+>', '', description)
        tags['COMM'] = COMM(encoding=3, lang='eng', desc='Description', text=clean_desc[:4000])

    # Custom tags for podcast-specific metadata
    if episode.get('guid'):
        tags['TXXX:GUID'] = TXXX(encoding=3, desc='GUID', text=episode['guid'])
    if episode.get('duration'):
        tags['TXXX:DURATION'] = TXXX(encoding=3, desc='DURATION', text=str(episode['duration']))
    if episode.get('subtitle'):
        tags['TXXX:SUBTITLE'] = TXXX(encoding=3, desc='SUBTITLE', text=episode['subtitle'])

    # Embed artwork
    if artwork_data:
        mime_type = 'image/jpeg'  # Most common
        tags['APIC'] = APIC(
            encoding=3,
            mime=mime_type,
            type=3,  # Cover (front)
            desc='Cover',
            data=artwork_data
        )

    audio.save()


def embed_metadata_simple(audio_path: Path, episode: dict, channel: dict,
                          episode_num: int, total_episodes: int, artwork_data: bytes | None):
    """Fallback: embed basic metadata using EasyID3, then try to add artwork separately."""
    try:
        audio = EasyID3(audio_path)
    except:
        # Create ID3 tag if none exists
        audio = MutagenFile(audio_path, easy=True)
        if audio is None:
            raise ValueError("Cannot read audio file")
        audio.add_tags()
        audio = EasyID3(audio_path)

    audio['title'] = episode.get('title', 'Unknown')
    audio['artist'] = episode.get('author', channel.get('author', 'Unknown'))
    audio['album'] = channel.get('title', 'Unknown Podcast')
    audio['genre'] = channel.get('category', 'Podcast')
    audio['tracknumber'] = f"{episode_num}/{total_episodes}"

    pub_date = episode.get('published_parsed')
    if pub_date:
        audio['date'] = str(pub_date.tm_year)

    audio.save()

    # Try to add artwork using ID3 directly (may work even when full method fails)
    artwork_added = False
    if artwork_data:
        try:
            tags = ID3(audio_path)
            tags['APIC'] = APIC(
                encoding=3,
                mime='image/jpeg',
                type=3,
                desc='Cover',
                data=artwork_data
            )
            tags.save(audio_path)
            artwork_added = True
        except:
            pass  # Artwork embedding failed, but basic tags are saved

    return artwork_added


def embed_metadata(audio_path: Path, episode: dict, channel: dict,
                   episode_num: int, total_episodes: int, artwork_data: bytes | None):
    """Embed ID3 tags with fallback for problematic files."""
    try:
        embed_metadata_full(audio_path, episode, channel, episode_num, total_episodes, artwork_data)
        return "full"
    except Exception as e:
        # Try simpler fallback with artwork attempt
        try:
            artwork_added = embed_metadata_simple(audio_path, episode, channel, episode_num, total_episodes, artwork_data)
            return "simple_with_art" if artwork_added else "simple"
        except Exception as e2:
            raise Exception(f"Full method: {e}; Simple method: {e2}")


def parse_duration(duration_str: str | int | None) -> int | None:
    """Parse duration from various formats to seconds."""
    if duration_str is None:
        return None
    if isinstance(duration_str, int):
        return duration_str

    duration_str = str(duration_str).strip()

    # Already seconds
    if duration_str.isdigit():
        return int(duration_str)

    # HH:MM:SS or MM:SS format
    parts = duration_str.split(':')
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except ValueError:
        pass

    return None


def extract_episode_data(entry: dict) -> dict:
    """Extract relevant data from a feed entry."""
    # Find the audio enclosure
    enclosure_url = None
    enclosure_type = None
    enclosure_size = None

    for link in entry.get('links', []):
        if link.get('rel') == 'enclosure' or 'audio' in link.get('type', ''):
            enclosure_url = link.get('href')
            enclosure_type = link.get('type')
            enclosure_size = link.get('length')
            break

    # Also check enclosures list
    for enc in entry.get('enclosures', []):
        if 'audio' in enc.get('type', ''):
            enclosure_url = enc.get('href') or enc.get('url')
            enclosure_type = enc.get('type')
            enclosure_size = enc.get('length')
            break

    # Get iTunes-specific fields
    itunes_duration = entry.get('itunes_duration')
    itunes_image = entry.get('image', {}).get('href') if isinstance(entry.get('image'), dict) else entry.get('image')
    itunes_author = entry.get('itunes_author') or entry.get('author')
    itunes_subtitle = entry.get('itunes_subtitle')
    itunes_order = entry.get('itunes_order')

    return {
        'title': entry.get('title', 'Untitled'),
        'guid': entry.get('id') or entry.get('guid'),
        'description': entry.get('description', ''),
        'summary': entry.get('summary', ''),
        'content': entry.get('content', [{}])[0].get('value', '') if entry.get('content') else '',
        'author': itunes_author,
        'subtitle': itunes_subtitle,
        'published': entry.get('published'),
        'published_parsed': entry.get('published_parsed'),
        'duration': parse_duration(itunes_duration),
        'enclosure_url': enclosure_url,
        'enclosure_type': enclosure_type,
        'enclosure_size': enclosure_size,
        'image_url': itunes_image,
        'order': itunes_order,
        'link': entry.get('link'),
    }


def extract_channel_data(feed: dict) -> dict:
    """Extract channel-level metadata from feed."""
    channel = feed.get('feed', {})

    # Get image URL
    image_url = None
    if channel.get('image'):
        if isinstance(channel['image'], dict):
            image_url = channel['image'].get('href')
        else:
            image_url = channel['image']

    # iTunes image takes precedence
    itunes_image = channel.get('itunes_image')
    if itunes_image:
        if isinstance(itunes_image, dict):
            image_url = itunes_image.get('href', image_url)
        else:
            image_url = itunes_image

    # Category
    category = 'Podcast'
    if channel.get('itunes_category'):
        cat = channel['itunes_category']
        if isinstance(cat, dict):
            category = cat.get('text', category)
        elif isinstance(cat, list) and cat:
            category = cat[0].get('text', category) if isinstance(cat[0], dict) else str(cat[0])

    return {
        'title': channel.get('title', 'Unknown Podcast'),
        'description': channel.get('description', ''),
        'subtitle': channel.get('itunes_subtitle', ''),
        'author': channel.get('itunes_author') or channel.get('author', ''),
        'link': channel.get('link', ''),
        'image_url': image_url,
        'language': channel.get('language', 'en'),
        'copyright': channel.get('rights', ''),
        'category': category,
        'owner_name': channel.get('itunes_owner', {}).get('name', '') if isinstance(channel.get('itunes_owner'), dict) else '',
        'owner_email': channel.get('itunes_owner', {}).get('email', '') if isinstance(channel.get('itunes_owner'), dict) else '',
        'explicit': channel.get('itunes_explicit', 'no'),
    }


def save_original_feed(feed_url: str, output_dir: Path):
    """Download and save the original RSS feed XML."""
    try:
        response = requests.get(feed_url, timeout=30)
        response.raise_for_status()

        feed_path = output_dir / 'original_feed.xml'
        temp_fd, temp_path = tempfile.mkstemp(dir=output_dir, suffix='.tmp')
        with os.fdopen(temp_fd, 'wb') as f:
            f.write(response.content)
        shutil.move(temp_path, feed_path)

        print(f"Saved original feed: {feed_path}")
    except Exception as e:
        print(f"Warning: Could not save original feed: {e}")


def save_metadata_json(channel: dict, episodes: list, output_dir: Path):
    """Save a JSON manifest of all metadata for easy re-import."""
    manifest = {
        'backup_date': datetime.now().isoformat(),
        'tool': 'podcast-backer-upper',
        'version': '1.0.0',
        'channel': channel,
        'episodes': episodes,
    }

    manifest_path = output_dir / 'manifest.json'
    temp_fd, temp_path = tempfile.mkstemp(dir=output_dir, suffix='.tmp')
    with os.fdopen(temp_fd, 'w') as f:
        json.dump(manifest, f, indent=2, default=str)
    shutil.move(temp_path, manifest_path)

    print(f"Saved manifest: {manifest_path}")


def generate_import_feed(channel: dict, episodes: list, output_dir: Path, base_url: str = None):
    """Generate an RSS feed pointing to local files (useful for re-import)."""
    from xml.etree.ElementTree import Element, SubElement, tostring
    from xml.dom import minidom

    # Create RSS structure
    rss = Element('rss', {
        'version': '2.0',
        'xmlns:itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd',
        'xmlns:atom': 'http://www.w3.org/2005/Atom',
    })

    chan = SubElement(rss, 'channel')

    # Channel metadata
    SubElement(chan, 'title').text = channel['title']
    SubElement(chan, 'description').text = channel['description']
    SubElement(chan, 'link').text = channel.get('link', '')
    SubElement(chan, 'language').text = channel.get('language', 'en')
    SubElement(chan, '{http://www.itunes.com/dtds/podcast-1.0.dtd}author').text = channel.get('author', '')
    SubElement(chan, '{http://www.itunes.com/dtds/podcast-1.0.dtd}subtitle').text = channel.get('subtitle', '')

    if channel.get('image_url'):
        img = SubElement(chan, '{http://www.itunes.com/dtds/podcast-1.0.dtd}image')
        img.set('href', 'cover.jpg')  # Local reference

    # Episodes
    for ep in episodes:
        item = SubElement(chan, 'item')
        SubElement(item, 'title').text = ep['title']
        SubElement(item, 'description').text = ep.get('description', '')
        SubElement(item, '{http://www.itunes.com/dtds/podcast-1.0.dtd}author').text = ep.get('author', '')
        SubElement(item, '{http://www.itunes.com/dtds/podcast-1.0.dtd}duration').text = str(ep.get('duration', ''))
        SubElement(item, 'pubDate').text = ep.get('published', '')
        SubElement(item, 'guid').text = ep.get('guid', '')

        if ep.get('local_filename'):
            enc = SubElement(item, 'enclosure')
            if base_url:
                enc.set('url', f"{base_url}/{ep['local_filename']}")
            else:
                enc.set('url', ep['local_filename'])
            enc.set('type', ep.get('enclosure_type', 'audio/mpeg'))
            enc.set('length', str(ep.get('enclosure_size', 0)))

    # Pretty print
    xml_str = minidom.parseString(tostring(rss)).toprettyxml(indent='  ')

    feed_path = output_dir / 'import_feed.xml'
    temp_fd, temp_path = tempfile.mkstemp(dir=output_dir, suffix='.tmp')
    with os.fdopen(temp_fd, 'w') as f:
        f.write(xml_str)
    shutil.move(temp_path, feed_path)

    print(f"Generated import feed: {feed_path}")


def verify_backup(backup_dir: str, repair: bool = False, channel_artwork: bytes = None):
    """
    Verify an existing backup and optionally repair issues.

    Checks:
    - All files in manifest exist
    - Audio files are readable
    - Metadata is present in audio files
    """
    backup_path = Path(backup_dir)
    manifest_path = backup_path / 'manifest.json'
    episodes_dir = backup_path / 'episodes'

    if not manifest_path.exists():
        print(f"Error: No manifest.json found in {backup_dir}")
        return False

    with open(manifest_path) as f:
        manifest = json.load(f)

    channel = manifest.get('channel', {})
    episodes = manifest.get('episodes', [])

    print(f"Verifying backup: {channel.get('title', 'Unknown')}")
    print(f"Episodes in manifest: {len(episodes)}")
    print()

    # Load channel artwork for repairs
    if repair and channel_artwork is None:
        cover_path = backup_path / 'cover.jpg'
        if not cover_path.exists():
            cover_path = backup_path / 'cover.png'
        if cover_path.exists():
            channel_artwork = cover_path.read_bytes()

    issues = {
        'missing_files': [],
        'unreadable': [],
        'missing_metadata': [],
        'repaired': [],
    }

    for i, ep in enumerate(episodes, 1):
        filename = ep.get('local_filename')
        if not filename:
            continue

        # Skip episodes marked as metadata-only
        if ep.get('audio_missing'):
            continue

        audio_path = episodes_dir / filename
        title = ep.get('title', filename)

        print(f"\r[{i}/{len(episodes)}] Checking: {title[:50]}...", end='', flush=True)

        # Check file exists
        if not audio_path.exists():
            issues['missing_files'].append(filename)
            continue

        # Check file is readable and has metadata
        try:
            audio = MP3(audio_path, ID3=ID3)
            tags = audio.tags

            # Check for basic metadata
            has_title = 'TIT2' in tags if tags else False
            has_artist = 'TPE1' in tags if tags else False
            has_artwork = 'APIC:Cover' in tags or 'APIC:' in str(tags.keys()) if tags else False

            if not has_title or not has_artist:
                if repair:
                    try:
                        embed_metadata_full(
                            audio_path, ep, channel,
                            ep.get('episode_number', i), len(episodes),
                            channel_artwork
                        )
                        issues['repaired'].append(filename)
                    except:
                        issues['missing_metadata'].append(filename)
                else:
                    issues['missing_metadata'].append(filename)

        except Exception as e:
            issues['unreadable'].append((filename, str(e)))

    print("\r" + " " * 80 + "\r", end='')  # Clear line

    # Report results
    print("\n" + "=" * 50)
    print("Verification Results")
    print("=" * 50)

    total_issues = sum(len(v) for v in issues.values()) - len(issues['repaired'])

    if total_issues == 0 and not issues['repaired']:
        print("All files verified successfully!")
    else:
        if issues['missing_files']:
            print(f"\nMissing files ({len(issues['missing_files'])}):")
            for f in issues['missing_files'][:10]:
                print(f"  - {f}")
            if len(issues['missing_files']) > 10:
                print(f"  ... and {len(issues['missing_files']) - 10} more")

        if issues['unreadable']:
            print(f"\nUnreadable/corrupt files ({len(issues['unreadable'])}):")
            for f, err in issues['unreadable'][:10]:
                print(f"  - {f}: {err}")
            if len(issues['unreadable']) > 10:
                print(f"  ... and {len(issues['unreadable']) - 10} more")

        if issues['missing_metadata']:
            print(f"\nMissing metadata ({len(issues['missing_metadata'])}):")
            for f in issues['missing_metadata'][:10]:
                print(f"  - {f}")
            if len(issues['missing_metadata']) > 10:
                print(f"  ... and {len(issues['missing_metadata']) - 10} more")

        if issues['repaired']:
            print(f"\nRepaired ({len(issues['repaired'])}):")
            for f in issues['repaired'][:10]:
                print(f"  - {f}")
            if len(issues['repaired']) > 10:
                print(f"  ... and {len(issues['repaired']) - 10} more")

    print()
    return total_issues == 0


def estimate_download_size(episodes: list, episodes_dir: Path) -> tuple[int, int, int]:
    """
    Estimate total download size for episodes.
    Returns (total_bytes, episodes_to_download, episodes_existing).
    """
    total_bytes = 0
    to_download = 0
    existing = 0

    for ep in episodes:
        filename = ep.get('local_filename')
        if not filename:
            continue

        audio_path = episodes_dir / filename
        if audio_path.exists():
            existing += 1
        else:
            to_download += 1
            size = ep.get('enclosure_size')
            if size:
                try:
                    total_bytes += int(size)
                except (ValueError, TypeError):
                    pass

    return total_bytes, to_download, existing


def backup_podcast_parallel(entries_with_dates: list, episodes_dir: Path,
                            channel: dict, channel_artwork_data: bytes,
                            total_episodes: int, stats: BackupStats,
                            interactive: bool, parallel: int,
                            output_path: Path, generate_import: bool) -> bool:
    """Handle parallel downloading of episodes."""
    episodes = []

    # Prepare download tasks (only for episodes that need downloading)
    download_tasks = []
    for entry, ep_data in entries_with_dates:
        filename = ep_data['local_filename']
        audio_path = episodes_dir / filename

        if not ep_data.get('enclosure_url'):
            continue

        # Validate URL
        url_valid, url_reason = is_likely_audio_url(ep_data['enclosure_url'])
        if not url_valid:
            ep_data['audio_missing'] = True
            ep_data['audio_error'] = f"Invalid URL: {url_reason}"
            episodes.append(ep_data)
            stats.increment('metadata_only')
            continue

        if audio_path.exists():
            stats.increment('skipped_existing')
            episodes.append(ep_data)
        else:
            download_tasks.append((ep_data, audio_path, stats))

    print(f"Downloading {len(download_tasks)} episodes with {parallel} workers...")

    completed = 0
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {executor.submit(download_episode_parallel, task): task
                   for task in download_tasks}

        for future in as_completed(futures):
            result = future.result()
            completed += 1
            ep_data = result['ep_data']
            audio_path = result['audio_path']
            title = ep_data['title']

            if result['error']:
                print(f"[{completed}/{len(download_tasks)}] FAILED: {title[:50]} - {result['error']}")
                ep_data['audio_missing'] = True
                ep_data['audio_error'] = result['error']
                stats.increment('metadata_only')
            elif result['existed']:
                print(f"[{completed}/{len(download_tasks)}] EXISTS: {title[:50]}")
            else:
                print(f"[{completed}/{len(download_tasks)}] OK: {title[:50]} ({format_size(result['bytes'])})")
                stats.increment('downloaded')
                stats.increment('bytes_downloaded', result['bytes'])

                # Embed metadata
                try:
                    embed_metadata(
                        audio_path, ep_data, channel,
                        ep_data['episode_number'], total_episodes, channel_artwork_data
                    )
                except Exception as e:
                    print(f"  Warning: metadata embed failed: {e}")

            episodes.append(ep_data)

            # Small delay between completions to be nice to servers
            time.sleep(0.2)

    # Save manifest and import feed
    save_metadata_json(channel, episodes, output_path)
    if generate_import:
        generate_import_feed(channel, episodes, output_path)

    print(f"\n{'='*50}")
    print(f"Backup complete!")
    print(f"Location: {output_path}")
    print(f"\nStatistics:")
    print(stats.summary())
    print(f"\nFiles created:")
    print(f"  - manifest.json, import_feed.xml, episodes/")

    return True


def backup_podcast(feed_url: str, output_dir: str = None, limit: int = None,
                   skip_existing: bool = True, generate_import: bool = True,
                   interactive: bool = True, parallel: int = 1, yes: bool = False):
    """Main function to backup a podcast feed."""

    # Reset error decisions for this run
    global _error_decisions
    _error_decisions = {}

    # Initialize stats
    stats = BackupStats()

    print(f"Fetching feed: {feed_url}")
    feed = feedparser.parse(feed_url)

    if feed.bozo and not feed.entries:
        print(f"Error: Could not parse feed - {feed.bozo_exception}")
        return False

    # Extract channel data
    channel = extract_channel_data(feed)
    print(f"Podcast: {channel['title']}")
    print(f"Author: {channel['author']}")
    print(f"Episodes found: {len(feed.entries)}")

    # Set up output directory
    if output_dir:
        output_path = Path(output_dir)
    else:
        safe_name = sanitize_filename(channel['title'])
        output_path = Path.cwd() / safe_name

    output_path.mkdir(parents=True, exist_ok=True)
    episodes_dir = output_path / 'episodes'
    episodes_dir.mkdir(exist_ok=True)

    print(f"Output directory: {output_path}")

    # Save original feed
    save_original_feed(feed_url, output_path)

    # Download channel artwork
    channel_artwork_data = None
    if channel.get('image_url'):
        print("Downloading channel artwork...")
        artwork_ext = '.jpg'
        if '.png' in channel['image_url'].lower():
            artwork_ext = '.png'
        channel_artwork_data = download_image(
            channel['image_url'],
            output_path / f'cover{artwork_ext}'
        )

    # Process episodes
    episodes = []
    entries = feed.entries

    # Sort by date (oldest first) for proper numbering
    entries_with_dates = []
    for entry in entries:
        ep_data = extract_episode_data(entry)
        entries_with_dates.append((entry, ep_data))

    # Sort by published date, oldest first
    entries_with_dates.sort(
        key=lambda x: x[1].get('published_parsed') or (9999, 12, 31),
        reverse=False
    )

    total_episodes = len(entries_with_dates)
    if limit:
        # When limiting, take the most recent (reverse to get newest first, then limit)
        entries_with_dates = list(reversed(entries_with_dates))[:limit]
        entries_with_dates = list(reversed(entries_with_dates))

    # Prepare episode data with filenames for size estimation
    for idx, (entry, ep_data) in enumerate(entries_with_dates, 1):
        title = ep_data['title']
        safe_title = sanitize_filename(title, max_length=80)
        pub_date = ep_data.get('published_parsed')
        if pub_date:
            date_prefix = f"{pub_date.tm_year % 100:02d}{pub_date.tm_mon:02d}{pub_date.tm_mday:02d}"
        else:
            date_prefix = str(idx).zfill(6)
        ep_data['local_filename'] = f"{date_prefix}-{safe_title}.mp3"
        ep_data['episode_number'] = idx
        ep_data['date_prefix'] = date_prefix

    # Storage estimate
    est_bytes, to_download, already_exist = estimate_download_size(
        [ep for _, ep in entries_with_dates],
        episodes_dir
    )

    print(f"\nEpisodes to process: {len(entries_with_dates)}")
    print(f"  Need to download: {to_download} episodes (~{format_size(est_bytes)})")
    print(f"  Already exist:    {already_exist} episodes")

    if to_download > 0 and not yes and interactive:
        print()
        confirm = input("Continue with download? [Y/n]: ").strip().lower()
        if confirm and confirm not in ('y', 'yes'):
            print("Aborted by user.")
            return False

    print(f"\nProcessing {len(entries_with_dates)} episodes...")

    # Use parallel downloads if requested
    if parallel > 1 and to_download > 1:
        print(f"Using {parallel} parallel downloads...")
        return backup_podcast_parallel(
            entries_with_dates, episodes_dir, channel, channel_artwork_data,
            total_episodes, stats, interactive, parallel, output_path, generate_import
        )

    for idx, (entry, ep_data) in enumerate(entries_with_dates, 1):
        ep_num = ep_data['episode_number']
        title = ep_data['title']
        filename = ep_data['local_filename']
        audio_path = episodes_dir / filename

        print(f"\n[{idx}/{len(entries_with_dates)}] {title}")

        if not ep_data.get('enclosure_url'):
            print("  Warning: No audio URL found, skipping")
            continue

        # Validate URL looks like audio
        url_valid, url_reason = is_likely_audio_url(ep_data['enclosure_url'])
        if not url_valid:
            print(f"  Warning: {url_reason}")
            print(f"  URL: {ep_data['enclosure_url']}")
            decision = handle_error_interactive(
                ValueError(f"Invalid audio URL: {url_reason}"),
                f"URL validation for: {title}",
                interactive
            )
            if decision == 'abort':
                print("\nBackup aborted by user.")
                return False
            elif decision in ('skip', 'skip_all'):
                print("  Skipping episode entirely")
                continue
            else:  # 'continue' - save metadata without audio
                print("  Saving metadata only (no audio)")
                ep_data['audio_missing'] = True
                ep_data['audio_error'] = f"Invalid URL: {url_reason}"
                episodes.append(ep_data)
                continue

        # Download audio
        audio_downloaded = False
        try:
            if skip_existing and audio_path.exists():
                print("  Audio: already exists, skipping download")
                audio_downloaded = True
                stats.increment('skipped_existing')
            else:
                print(f"  Audio: {ep_data['enclosure_url'][:80]}...")
                bytes_dl = download_file(ep_data['enclosure_url'], audio_path)
                print("  Audio: downloaded")
                audio_downloaded = True
                stats.increment('downloaded')
                stats.increment('bytes_downloaded', bytes_dl)
                # Be polite to the server
                time.sleep(0.5)
        except Exception as e:
            decision = handle_error_interactive(e, f"Downloading: {title}", interactive)
            if decision == 'abort':
                print("\nBackup aborted by user.")
                return False
            elif decision in ('skip', 'skip_all'):
                print(f"  Audio: SKIPPED - {e}")
                stats.increment('errors')
                continue  # Skip this episode entirely
            else:  # 'continue' - save metadata without audio
                print(f"  Audio: FAILED ({e}) - saving metadata only")
                ep_data['audio_missing'] = True
                ep_data['audio_error'] = str(e)
                stats.increment('metadata_only')

        # Download episode-specific artwork if different from channel
        ep_artwork_data = channel_artwork_data
        if ep_data.get('image_url') and ep_data['image_url'] != channel.get('image_url'):
            # We'll just use it for embedding, not save separately
            try:
                response = requests.get(ep_data['image_url'], timeout=30)
                response.raise_for_status()
                ep_artwork_data = response.content
            except:
                pass  # Fall back to channel artwork

        # Embed metadata (only if we have an audio file)
        if audio_downloaded:
            try:
                method = embed_metadata(
                    audio_path, ep_data, channel,
                    ep_num, total_episodes, ep_artwork_data
                )
                if method == "simple":
                    print("  Metadata: embedded (basic tags only, no artwork)")
                elif method == "simple_with_art":
                    print("  Metadata: embedded (basic tags + artwork via fallback)")
                else:
                    print("  Metadata: embedded")
            except Exception as e:
                decision = handle_error_interactive(e, f"Embedding metadata for: {title}", interactive)
                if decision == 'abort':
                    print("\nBackup aborted by user.")
                    return False
                print(f"  Metadata: SKIPPED - {e}")
        else:
            print("  Metadata: saved to manifest (no audio file)")

        episodes.append(ep_data)

    # Save manifest
    save_metadata_json(channel, episodes, output_path)

    # Generate import feed
    if generate_import:
        generate_import_feed(channel, episodes, output_path)

    print(f"\n{'='*50}")
    print(f"Backup complete!")
    print(f"Location: {output_path}")
    print(f"\nStatistics:")
    print(stats.summary())
    print(f"\nFiles created:")
    print(f"  - original_feed.xml (original RSS for reference)")
    print(f"  - manifest.json (structured metadata)")
    print(f"  - import_feed.xml (RSS pointing to local files)")
    print(f"  - cover.jpg/png (channel artwork)")
    print(f"  - episodes/ (audio files with embedded metadata)")

    return True


def main():
    parser = argparse.ArgumentParser(
        description='Backup a podcast feed with full metadata preservation.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s https://example.com/feed.xml
  %(prog)s https://example.com/feed.xml -o ~/Podcasts/MyShow
  %(prog)s https://example.com/feed.xml --limit 10 --parallel 4
  %(prog)s --verify ~/Podcasts/MyShow --repair
        """
    )

    # Verify mode (alternative to feed_url)
    parser.add_argument('--verify', metavar='DIR',
                        help='Verify an existing backup directory instead of downloading')
    parser.add_argument('--repair', action='store_true',
                        help='Attempt to repair issues found during verification')

    # Normal backup arguments
    parser.add_argument('feed_url', nargs='?', help='URL of the podcast RSS feed')
    parser.add_argument('-o', '--output', help='Output directory (default: based on podcast name)')
    parser.add_argument('--limit', type=int, help='Limit number of episodes to download (most recent)')
    parser.add_argument('--no-skip', action='store_true', help='Re-download existing files')
    parser.add_argument('--no-import-feed', action='store_true', help='Skip generating import feed')
    parser.add_argument('--non-interactive', action='store_true',
                        help='Skip errors without prompting (for automated/background runs)')
    parser.add_argument('-y', '--yes', action='store_true',
                        help='Skip download confirmation prompt')
    parser.add_argument('-p', '--parallel', type=int, default=1, metavar='N',
                        help='Number of parallel downloads (default: 1)')

    args = parser.parse_args()

    # Handle verify mode
    if args.verify:
        success = verify_backup(args.verify, repair=args.repair)
        sys.exit(0 if success else 1)

    # Normal backup mode requires feed_url
    if not args.feed_url:
        parser.error('feed_url is required unless using --verify')

    success = backup_podcast(
        feed_url=args.feed_url,
        output_dir=args.output,
        limit=args.limit,
        skip_existing=not args.no_skip,
        generate_import=not args.no_import_feed,
        interactive=not args.non_interactive,
        parallel=args.parallel,
        yes=args.yes,
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
