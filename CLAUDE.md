# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Podcast Backer-Upper is a CLI tool that downloads and archives podcast feeds with full metadata preservation. It's designed for archival and potential re-hosting purposes.

## Running the Tool

The project lives in a pCloud-synced folder, which doesn't support symlinks or direct script execution. Use the bash wrapper:

```bash
bash ./backup-podcast "https://example.com/feed.xml"
bash ./backup-podcast "https://example.com/feed.xml" --limit 10
bash ./backup-podcast "https://example.com/feed.xml" -o ~/Podcasts/MyShow
bash ./backup-podcast "https://example.com/feed.xml" --parallel 4 -y
bash ./backup-podcast --verify ~/Podcasts/MyShow --repair
```

The virtual environment is stored at `~/.local/venvs/podcast-backup` (outside pCloud) and is auto-created by the wrapper script.

## Dependencies

Install manually if needed:
```bash
source ~/.local/venvs/podcast-backup/bin/activate
pip install feedparser requests mutagen
```

## Architecture

Single-file tool (`podcast_backup.py`) with these key components:

### Metadata Embedding (Two-tier fallback)
1. `embed_metadata_full()` - Uses mutagen's MP3/ID3 classes for complete tags including artwork
2. `embed_metadata_simple()` - Falls back to EasyID3 for basic tags, then attempts artwork via raw ID3
3. Some MP3s fail with "can't sync to MPEG frame" - the fallback handles these gracefully

### File Naming
Files are named `YYMMDD-Title.mp3` using the RSS `pubDate` field for chronological sorting.

### Atomic Writes
All file operations use temp files + rename for pCloud/sync folder safety. Pattern:
```python
temp_fd, temp_path = tempfile.mkstemp(dir=dest_path.parent, suffix='.tmp')
# write to temp_fd
shutil.move(temp_path, dest_path)
```

### Interactive Error Handling
The `_error_decisions` global tracks user choices per error type. `handle_error_interactive()` prompts once per error category, then remembers the decision.

### Parallel Downloads
Uses `ThreadPoolExecutor` for concurrent downloads. The `download_episode_parallel()` function is thread-safe. Stats tracking uses a `Lock` for thread safety.

### Verify/Repair Mode
`verify_backup()` checks existing backups: file existence, readability, metadata presence. With `--repair`, it re-embeds metadata for files missing it.

### Output Structure
```
Podcast-Name/
├── cover.jpg           # Channel artwork
├── original_feed.xml   # Raw RSS backup
├── manifest.json       # Structured metadata for all episodes
├── import_feed.xml     # RSS pointing to local files (for re-hosting)
└── episodes/
    └── YYMMDD-Title.mp3  # Episodes with embedded ID3 tags
```

## Key Libraries

- **feedparser**: RSS parsing, handles iTunes namespace extensions
- **mutagen**: ID3 tag reading/writing (MP3, EasyID3, ID3 classes)
- **requests**: HTTP downloads with streaming support
