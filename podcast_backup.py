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
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests
from mutagen.id3 import (
    ID3, APIC, TIT2, TPE1, TALB, TYER, TDRC, COMM, TCON, TRCK, TPOS, TXXX, USLT
)
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3
from mutagen import File as MutagenFile


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


def download_file(url: str, dest_path: Path, chunk_size: int = 8192) -> bool:
    """
    Download a file with atomic write (safe for synced folders).
    Returns True if downloaded, False if skipped (already exists).
    """
    if dest_path.exists():
        return False

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
                if total_size:
                    pct = (downloaded / total_size) * 100
                    print(f"\r  Downloading: {pct:.1f}%", end='', flush=True)

        print()  # Newline after progress

        # Atomic rename
        shutil.move(temp_path, dest_path)
        return True

    except Exception as e:
        # Clean up temp file on failure
        try:
            os.unlink(temp_path)
        except:
            pass
        raise e


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
                          episode_num: int, total_episodes: int):
    """Fallback: embed basic metadata using EasyID3 (works on more files, no artwork)."""
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


def embed_metadata(audio_path: Path, episode: dict, channel: dict,
                   episode_num: int, total_episodes: int, artwork_data: bytes | None):
    """Embed ID3 tags with fallback for problematic files."""
    try:
        embed_metadata_full(audio_path, episode, channel, episode_num, total_episodes, artwork_data)
        return "full"
    except Exception as e:
        # Try simpler fallback (no artwork, but at least basic tags)
        try:
            embed_metadata_simple(audio_path, episode, channel, episode_num, total_episodes)
            return "simple"
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


def backup_podcast(feed_url: str, output_dir: str = None, limit: int = None,
                   skip_existing: bool = True, generate_import: bool = True):
    """Main function to backup a podcast feed."""

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

    print(f"\nProcessing {len(entries_with_dates)} episodes...")

    for idx, (entry, ep_data) in enumerate(entries_with_dates, 1):
        ep_num = idx
        title = ep_data['title']
        safe_title = sanitize_filename(title, max_length=80)

        # Use YYMMDD date prefix for proper sorting
        pub_date = ep_data.get('published_parsed')
        if pub_date:
            date_prefix = f"{pub_date.tm_year % 100:02d}{pub_date.tm_mon:02d}{pub_date.tm_mday:02d}"
        else:
            # Fallback to sequential number if no date
            date_prefix = str(ep_num).zfill(6)

        filename = f"{date_prefix}-{safe_title}.mp3"
        ep_data['local_filename'] = filename
        ep_data['episode_number'] = ep_num
        ep_data['date_prefix'] = date_prefix

        audio_path = episodes_dir / filename

        print(f"\n[{idx}/{len(entries_with_dates)}] {title}")

        if not ep_data.get('enclosure_url'):
            print("  Warning: No audio URL found, skipping")
            continue

        # Download audio
        try:
            if skip_existing and audio_path.exists():
                print("  Audio: already exists, skipping download")
            else:
                print(f"  Audio: {ep_data['enclosure_url'][:80]}...")
                download_file(ep_data['enclosure_url'], audio_path)
                print("  Audio: downloaded")
                # Be polite to the server
                time.sleep(0.5)
        except Exception as e:
            print(f"  Error downloading audio: {e}")
            continue

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

        # Embed metadata
        try:
            method = embed_metadata(
                audio_path, ep_data, channel,
                ep_num, total_episodes, ep_artwork_data
            )
            if method == "simple":
                print("  Metadata: embedded (basic - no artwork due to file format)")
            else:
                print("  Metadata: embedded")
        except Exception as e:
            print(f"  Warning: Could not embed metadata: {e}")

        episodes.append(ep_data)

    # Save manifest
    save_metadata_json(channel, episodes, output_path)

    # Generate import feed
    if generate_import:
        generate_import_feed(channel, episodes, output_path)

    print(f"\n{'='*50}")
    print(f"Backup complete!")
    print(f"Location: {output_path}")
    print(f"Episodes: {len(episodes)}")
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
  %(prog)s https://example.com/feed.xml --limit 10
        """
    )

    parser.add_argument('feed_url', help='URL of the podcast RSS feed')
    parser.add_argument('-o', '--output', help='Output directory (default: based on podcast name)')
    parser.add_argument('--limit', type=int, help='Limit number of episodes to download (most recent)')
    parser.add_argument('--no-skip', action='store_true', help='Re-download existing files')
    parser.add_argument('--no-import-feed', action='store_true', help='Skip generating import feed')

    args = parser.parse_args()

    success = backup_podcast(
        feed_url=args.feed_url,
        output_dir=args.output,
        limit=args.limit,
        skip_existing=not args.no_skip,
        generate_import=not args.no_import_feed,
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
