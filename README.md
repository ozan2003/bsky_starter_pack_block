# Bluesky Starter Pack Blocker

Block every account listed in one or more Bluesky starter packs. The tool logs in with an app password, resolves each pack link to its backing list, loads the members, merges them into a **unique** set of accounts (by DID), skips your own account and people you already block, then creates block records for the rest.

**Requirements:** Python 3.12 or newer.

## Install

```bash
pip install -e .
# or, without installing: pip install "atproto>=0.0.65"
```

## Setup

1. In Bluesky: **Settings -> Privacy and security -> App passwords** - create an app password.
2. Prefer passing it via environment (avoids shell history and process lists):

   ```bash
   export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
   ```

## Usage

Run a **dry run** first (no blocks are created):

```bash
python3 bsky.py --handle your.handle.bsky.social --pack <starter-pack-link> --dry-run
```

Use more than one pack by passing several URLs (or AT URIs) after `--pack` (the union of members, deduplicated by account):

```bash
python3 bsky.py --handle your.handle.bsky.social --pack <pack-url-1> <pack-url-2> --dry-run
```

If the output looks right, run without `--dry-run`:

```bash
python3 bsky.py --handle your.handle.bsky.social --pack <starter-pack-link>
```

Optional flags: `--delay <seconds>` (default `0.5` between block operations), `--app-password` (if not using `BSKY_APP_PASSWORD`).

**Starter pack input** can be an AT URI (`at://.../app.bsky.graph.starterpack/...`) or a common bsky.app URL, including `bsky.app/start/...`, `bsky.app/starter-pack/...`, `bsky.app/starter-pack-short/...`, and `go.bsky.app/...` short links.

## License

See [LICENSE](LICENSE).
