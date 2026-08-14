# Bluesky Starter Pack / List Moderation

Apply a moderation action to every account contained in a Bluesky **starter
pack** or **user list**.

> [!CAUTION]
> **Blocking changes account relationships.** A large run can create
> **thousands** of blocks in seconds.
>
> - A block stops the relationship between your account and the target.
> - Bulk blocking can trigger abuse controls on your account.
> - A block list has no bulk-unblock operation.
> - Run `--dry-run --verbose` before every write action.
>
> The default action is block. Use `--mute`, `--unmute`, or `--unblock` to
> select another action.

The script logs in with an app password. It resolves each input to a list and
loads the list members. It merges members by DID and skips:

- your own account
- accounts already blocked when the action is `block`
- accounts without a block record when the action is `--unblock`

The script applies the selected action to the remaining accounts.

## Moderation actions

The default action is `block`. Select an alternate action with one flag:

| Action | API behavior |
| --- | --- |
| `block` | Creates an `app.bsky.graph.block` record. |
| `--mute` | Calls `app.bsky.graph.muteActor` for each account. |
| `--unmute` | Calls `app.bsky.graph.unmuteActor` for each account. |
| `--unblock` | Deletes each matching `app.bsky.graph.block` record. |

The AT Protocol has no separate unblock procedure. The script lists the
signed-in account's block records, finds the record for each selected DID, and
deletes that record with `com.atproto.repo.deleteRecord`.

The script skips the signed-in account for every action. The script skips an
unblock target when no matching block record exists.

Official API definitions:

- [`app.bsky.graph.muteActor`](https://raw.githubusercontent.com/bluesky-social/atproto/main/lexicons/app/bsky/graph/muteActor.json)
- [`app.bsky.graph.unmuteActor`](https://raw.githubusercontent.com/bluesky-social/atproto/main/lexicons/app/bsky/graph/unmuteActor.json)
- [`app.bsky.graph.block`](https://raw.githubusercontent.com/bluesky-social/atproto/main/lexicons/app/bsky/graph/block.json)

## Code structure

These functions implement the alternate actions in `bsky.py`:

| Function | Responsibility |
| --- | --- |
| `fetch_block_records()` | Reads all block records and maps each target DID to its record URI. |
| `_apply_action_once()` | Dispatches one block, mute, unmute, or unblock request. |
| `_apply_action_with_retries()` | Handles authentication, rate limits, retries, and failures for one request. |
| `apply_users()` | Processes skips, dry runs, progress, delays, and results for every action. |

The shared executor uses the same retry and summary logic for every action.
This keeps API dispatch separate from user processing and output.

## Requirements

- Python 3.12+
- `atproto>=0.0.65`

## Install

The project ships a `uv.lock`, so the recommended install is:

```bash
uv sync
```

Otherwise, install the dependency directly:

```bash
python3 -m pip install "atproto>=0.0.65"
```

## Setup

1. In Bluesky, open **Settings -> Privacy and security -> App passwords**.
2. Create an app password.
3. Set the app password in the environment:

   ```bash
   export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
   ```

4. Set your Bluesky handle in the environment:

   ```bash
   export BSKY_HANDLE="your.handle.bsky.social"
   ```

## Usage

Run a **dry run** first (no moderation changes are made):

```bash
uv run python bsky.py --input "<starter-pack-or-list-link>" --dry-run
```

If you want a line-per-account listing of planned actions, add `--verbose`:

```bash
uv run python bsky.py --input "<starter-pack-or-list-link>" --dry-run --verbose
```

If the output is correct, run without `--dry-run` (`--no-dry-run` is the explicit form):

```bash
uv run python bsky.py --input "<starter-pack-or-list-link>" --no-dry-run
```

The script asks for confirmation before it makes moderation changes. Use `-y`/`--yes` to skip the prompt in non-interactive runs:

```bash
uv run python bsky.py --input "<starter-pack-or-list-link>" --no-dry-run --yes
```

Mute the selected accounts instead of blocking them:

```bash
uv run python bsky.py --input "<starter-pack-or-list-link>" --mute --no-dry-run --yes
```

Unblock matching accounts from the selected sources:

```bash
uv run python bsky.py --input "<starter-pack-or-list-link>" --unblock --no-dry-run --yes
```

Use `--quiet` to suppress per-account lines. The script prints only operational messages and the final summary:

```bash
uv run python bsky.py --input "<starter-pack-or-list-link>" --no-dry-run --quiet
```

Use more than one source by putting one input per line in a file:

```bash
uv run python bsky.py --file inputs.txt --dry-run --verbose
```

## Flags

| Flag | Description |
| --- | --- |
| `-i`/`--input` | Single starter-pack/list URL or AT URI (mutually exclusive with `--file`). |
| `-f`/`--file` | UTF-8 text file with one URL/AT URI per line (max 10 MiB). |
| `--mute` | Mute each selected account. |
| `--unmute` | Unmute each selected account. |
| `--unblock` | Delete matching block records for selected accounts. |
| `--delay` | Seconds to sleep between successful moderation operations (default `0.5`). |
| `--dry-run`/`--no-dry-run` | Print planned actions without changing moderation state. Default: dry-run is off. |
| `-y`/`--yes` | Skip the confirmation prompt. Required for non-TTY runs. |
| `--quiet` | Suppress per-account output. The script prints only the summary and operational messages. |
| `-v`/`--verbose` | Show verbose per-account detail, such as dry-run action lines and error text. |

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Every member processed without failures. |
| 1 | At least one moderation operation failed. |
| 2 | The confirmation prompt was declined, or stdin was not a TTY without `--yes`. |
| 3 | The PDS-reported block-list cap was reached and the run was aborted. Unblock some accounts to continue. |

## Supported input formats

Inputs can be starter packs, lists, or short links (URLs can omit the `https://` scheme):

- Starter pack AT URI: `at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>`
- List AT URI: `at://<did-or-handle>/app.bsky.graph.list/<rkey>`
- Starter pack URLs:
  - `https://bsky.app/start/<did-or-handle>/<rkey>`
  - `https://bsky.app/starter-pack/<did-or-handle>/<rkey>`
- List URL:
  - `https://bsky.app/profile/<did-or-handle>/lists/<rkey>`
- Short links:
  - `https://bsky.app/starter-pack-short/<code>`
  - `https://go.bsky.app/<code>`

The script pauses and retries after rate limits and some temporary network or server errors. It re-authenticates once if the access token expires during a run.

## Notes

- Bluesky limits the number of accounts that one account can block. The script stops with exit code 3 when it reaches this limit. Unblock some accounts, then run the command again.
- Per-account output is suppressed by `--quiet`. Operational messages (rate-limit pauses, re-authentication notices, cap-reached aborts) always print.
- Progress is logged to stderr every 25 successful blocks. In dry-run mode, progress counts planned blocks. In `--quiet` mode, per-account output and progress are suppressed.

## License

See [LICENSE](LICENSE).
