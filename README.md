# YouTube Channel Analyzer & Markmap Exporter

Analyze any YouTube channel, auto-tag videos (Azure OpenAI optional), compute simple stats, and export a markmap mind map.

## Quick Start

1) Install

```
pip install -r requirements.txt
```

2) Configure env (create a .env file or set env variables)

```
YOUTUBE_API_KEY=your_key
# Optional if using Azure tagging
AZURE_OPENAI_KEY=...
AZURE_OPENAI_ENDPOINT=...
AZURE_OPENAI_DEPLOYMENT=...
AZURE_OPENAI_API_VERSION=2024-05-01-preview
```

3) Analyze a channel

```
python -m src.yt_tool.cli analyze --channel-id UCxxxx --data-dir data
```

Optional taxonomy filtering (file is optional; see data/taxonomy.json):

```
python -m src.yt_tool.cli analyze --channel-id UCxxxx --data-dir data \
  --taxonomy data/taxonomy.json --restrict-to-taxonomy
```

4) Export markmap

```
python -m src.yt_tool.cli export markmap --data-dir data \
  --root-title "YouTube Channel" --root-url "https://youtube.com/@handle"
```

5) Show stats

```
python -m src.yt_tool.cli stats --data-dir data
```

## Folder Structure

```
src/yt_tool/
  cli.py                # CLI commands: analyze, export markmap, stats
  config.py             # env + defaults
  storage/repo.py       # load/save videos.json, sync_metadata.json, stats
  youtube/client.py     # thin Google API wrapper + rate limiting
  youtube/analyzer.py   # fetch, batch tag, merge, sort
  tagging/provider.py   # interface for tagging providers
  tagging/azure_openai.py # Azure implementation (optional)
  taxonomy/loader.py    # load optional taxonomy.json
  taxonomy/match.py     # filter to taxonomy (optional)
  export/markmap.py     # parameterized root title/url
data/
  videos.json, speaker_stats.json, sync_metadata.json, taxonomy.json
```

## Taxonomy

- File: `data/taxonomy.json` (optional). If omitted, categories are free-form.
- Use `--restrict-to-taxonomy` to filter tags to known categories.
- Starter taxonomy is provided; edit as needed.

## GitHub Actions

Workflow: `.github/workflows/analyze.yml`

- Triggers: manual (`workflow_dispatch`) with inputs `channel_id`, `restrict_to_taxonomy`, `taxonomy_path`.
- Secrets required:
  - `YOUTUBE_API_KEY`
  - Optional: `AZURE_OPENAI_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT`, `AZURE_OPENAI_API_VERSION`

It will:
- Install dependencies
- Run `analyze` with the provided `channel_id`
- Export the markmap
- Upload `data/videos.json`, `data/speaker_stats.json`, `data/videos_markmap.md` as artifacts