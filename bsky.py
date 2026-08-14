#!/usr/bin/env python3
"""Apply moderation actions to accounts in Bluesky starter packs or lists.

This script logs in to Bluesky with an app password, resolves each supported
input to a list URI, loads every account across those lists (merging unique
members by account), and applies a moderation action to the remaining
accounts. Blocking is the default; ``--mute``, ``--unmute``, and
``--unblock`` select the other supported actions.

Supported inputs:
    - ``at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>``
    - ``at://<did-or-handle>/app.bsky.graph.list/<rkey>``
    - ``http(s)://bsky.app/start/<did-or-handle>/<rkey>``
    - ``http(s)://bsky.app/starter-pack/<did-or-handle>/<rkey>``
    - ``http(s)://bsky.app/profile/<did-or-handle>/lists/<rkey>``
    - ``http(s)://bsky.app/starter-pack-short/<code>``
    - ``http(s)://go.bsky.app/<code>``

Usage:
    First create an app password in Bluesky under Settings -> Privacy and
    Security -> App Passwords. Then set that password and your handle in the environment:

    ```
    export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
    export BSKY_HANDLE="your.handle.bsky.social"
    ```

    Run a dry run before changing moderation state:

    ``python3 bsky.py --input <url-or-at-uri> --dry-run``

    Or load pack inputs from a file (one input per line):

    ``python3 bsky.py --file inputs.txt --dry-run``

    ``--input`` and ``--file`` are mutually exclusive.

    If the dry run looks correct, run without ``--dry-run``:

    ``python3 bsky.py --input <url-or-at-uri>``

    Use ``--mute``, ``--unmute``, or ``--unblock`` to select another action.

    Use ``--delay`` to control the pause between moderation operations.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from importlib import metadata as importlib_metadata
from math import isinf, isnan
from pathlib import Path
from random import uniform
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from atproto import Client, models
from atproto.exceptions import (
    AtProtocolError,
    BadRequestError,
    NetworkError,
    RequestException,
    UnauthorizedError,
)

try:
    __version__ = importlib_metadata.version("bsky-starter-pack-block")
except importlib_metadata.PackageNotFoundError:
    __version__ = "0.0.0+unknown"

STARTER_PACK_COLLECTION = "app.bsky.graph.starterpack"
LIST_COLLECTION = "app.bsky.graph.list"
BLOCK_COLLECTION = "app.bsky.graph.block"
SUPPORTED_STARTER_PACK_PATHS = {"start", "starter-pack"}
PROFILE_PATH_SEGMENT = "profile"
LISTS_PATH_SEGMENT = "lists"
BSKY_APP_HOSTS = {"bsky.app", "www.bsky.app"}
BSKY_SHORT_LINK_HOST = "go.bsky.app"
STARTER_PACK_SHORT_PATH = "starter-pack-short"

# Socket timeout for urllib when resolving short links (go.bsky.app redirects).
SHORT_LINK_TIMEOUT = dt.timedelta(seconds=10.0)

# Maximum size of the input file in bytes; anything larger is rejected before
# opening so a 5GB paste cannot OOM the process.
MAX_INPUT_FILE_SIZE = 10 * 1024 * 1024  # 10 MiB

# Maximum length of a single AT URI segment (identifier, rkey). Keeps a
# hostile 1MB identifier from making the parser do useless work.
MAX_AT_URI_SEGMENT_LENGTH = 512

# A progress line is emitted to stderr every N successful moderation actions
# (or, in dry-run, every N planned actions). Set to 0 to disable output.
PROGRESS_EVERY = 25

# Maximum records per page for listRepos/listRecords-style reads.
LIST_PAGE_SIZE = 100

# Same pagination limit when enumerating existing blocks to skip already-blocked DIDs.
BLOCKS_PAGE_SIZE = 100

# Successful moderation actions are followed by ``time.sleep(delay)``.
# This is the CLI default when ``--delay`` is omitted.
DEFAULT_DELAY = dt.timedelta(seconds=0.5)

# Attempts per account use capped exponential backoff and jitter.
MAX_ATTEMPTS = 5  # Total attempts (initial + retries) per account.
# Wait ``min(MAX, BASE * 2**(attempt - 1))`` seconds before each retry.
BASE_BACKOFF = dt.timedelta(seconds=1.0)
# Upper bound so backoff does not grow past a lot.
MAX_BACKOFF = dt.timedelta(seconds=8.0)

# Uniform random extra delay on each backoff (seconds), reduces synchronized retries.
JITTER = (dt.timedelta(seconds=0.0), dt.timedelta(seconds=0.6))

# HTTP response codes treated as retryable alongside network errors (see ``is_transient_error``).
HTTP_STATUS_TOO_MANY_REQUESTS = 429
# Any status greater than or equal to this is treated as a transient server error.
HTTP_STATUS_SERVER_ERROR_MIN = 500

# Rate-limit pause: small buffer added to the computed wait so the retry lands after the window resets.
RATE_LIMIT_BUFFER = dt.timedelta(seconds=2.0)
# If the server asks to wait longer than this, abort instead of blocking the terminal.
RATE_LIMIT_MAX_WAIT = dt.timedelta(hours=3)
# XRPC error code returned by the PDS when the user has hit the maximum
# per-account block list size. Used to abort the run with a clear message
# instead of retrying forever.
BLOCK_LIST_CAP_ERROR_CODE = "BlockedAccountCountLimitExceeded"
# User agent sent when resolving short links. Identifies the script by name
# and version so the short-link service can apply appropriate rate limits.
USER_AGENT = f"bsky-starter-pack-block/{__version__}"

# Useful for comparisons.
ZERO_DURATION = dt.timedelta(0)


class BlockListCapError(Exception):
    """Raised when the PDS rejects a block because the user hit the per-account cap."""

    def __init__(self, did: str, handle: str) -> None:
        super().__init__(
            f"Block list cap reached while blocking {handle} ({did})"
        )
        self.did = did
        self.handle = handle


@dataclass(slots=True)
class Member:
    """Starter pack member selected for a moderation action.

    Attributes:
        did: Account DID used as the stable moderation target.
        handle: Current account handle, used only for human-readable output.
    """

    did: str
    handle: str


@dataclass(slots=True)
class ModerationSummary:
    """Counters collected while processing moderation targets.

    Attributes:
        discovered: Number of unique members loaded from the sources.
        skipped_self: Number of members skipped because they are the signed-in
            account.
        skipped_existing: Number of members skipped because the selected
            action has no work to do for them.
        skipped_invalid: Number of members skipped because the PDS rejected
            them as invalid targets.
        planned: Number of actions planned in dry-run mode.
        applied: Number of actions completed successfully.
        failed: Number of actions that failed.
        retries: Number of retry attempts used for transient failures.
        reauths: Number of re-authentications triggered by an expired session.
        cap_reached: Whether the block-list cap stopped the run.
    """

    discovered: int = 0
    skipped_self: int = 0
    skipped_existing: int = 0
    skipped_invalid: int = 0
    planned: int = 0
    applied: int = 0
    failed: int = 0
    retries: int = 0
    reauths: int = 0
    cap_reached: bool = False


@dataclass(frozen=True, slots=True)
class PackReference:
    """Canonical starter pack/list reference before DID normalization.

    Attributes:
        identifier: Starter-pack/list creator DID or handle.
        rkey: Record key.
        collection: AT Protocol collection for the reference.
    """

    identifier: str
    rkey: str
    collection: str


@dataclass(frozen=True, slots=True)
class ShortStarterPackLink:
    """Bluesky short link that must be resolved before API use.

    Attributes:
        url: Canonical short-link service URL.
    """

    url: str


type PackInput = PackReference | ShortStarterPackLink


class SourceKind(StrEnum):
    """Kind of source input."""

    STARTER_PACK = "starter_pack"
    LIST = "list"


class ModerationAction(StrEnum):
    """Moderation action applied to each selected account."""

    BLOCK = "block"
    MUTE = "mute"
    UNMUTE = "unmute"
    UNBLOCK = "unblock"


class ActionOutcome(StrEnum):
    """Outcome of one moderation request."""

    APPLIED = "applied"
    SKIPPED_INVALID = "skipped_invalid"
    FAILED = "failed"


def confirm_destructive(count: int, action: str = "block") -> bool:
    """Prompt the user before applying an action to ``count`` accounts.

    Returns ``True`` if the user typed ``y``/``yes`` (case-insensitive).
    Returns ``False`` for any other response.

    Raises:
        SystemExit: With code ``2`` if stdin is not a TTY (e.g. cron / CI),
            so a destructive run is never launched silently.
    """

    if not sys.stdin.isatty():
        print(
            f"ERROR: refusing to {action} {count} accounts without a TTY."
            " Pass --yes to confirm non-interactively.",
            file=sys.stderr,
        )
        raise SystemExit(2)

    print(f"About to {action} {count} accounts. Continue? [y/N]")
    response = input().strip().lower()
    return response in {"y", "yes"}


@dataclass(frozen=True, slots=True)
class ResolvedListTarget:
    """Resolved list target used by the fetch/block pipeline.

    Attributes:
        list_uri: Canonical list AT URI ready for ``app.bsky.graph.getList``.
        source_kind: Whether the input was a direct list or a starter pack.
    """

    list_uri: str
    source_kind: SourceKind


def parse_delay(value: str) -> dt.timedelta:
    """Parse a CLI delay value.

    Args:
        value: Raw command-line value for ``--delay``.

    Returns:
        A finite, non-negative delay.

    Raises:
        argparse.ArgumentTypeError: If the delay is negative, infinite, or NaN.
        ValueError: If ``value`` cannot be parsed as a float.
    """

    delay = float(value)
    if delay < 0:
        msg = "delay must be greater than or equal to 0"
        raise argparse.ArgumentTypeError(msg)
    if isinf(delay) or isnan(delay):
        msg = "delay must be a finite number"
        raise argparse.ArgumentTypeError(msg)
    return dt.timedelta(seconds=delay)


def parse_args() -> argparse.Namespace:
    """Parse command-line options.

    Returns:
        Parsed command-line arguments for login, one or more starter-pack/list
        inputs, throttling, and dry-run mode.
    """

    parser = argparse.ArgumentParser(
        description="Apply moderation actions to users from Bluesky starter packs or lists",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Needed enviroment variables:\n"
            "    - BSKY_APP_PASSWORD: Bluesky app password\n"
            "    - BSKY_HANDLE: Bluesky handle\n"
        ),
    )

    pack_input_group = parser.add_mutually_exclusive_group(required=True)

    pack_input_group.add_argument(
        "-i",
        "--input",
        type=str,
        help="Single starter-pack/list URL or AT URI",
    )

    pack_input_group.add_argument(
        "-f",
        "--file",
        type=str,
        help="Path to a UTF-8 text file with one input per line",
    )

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument(
        "--mute",
        dest="action",
        action="store_const",
        const=ModerationAction.MUTE,
        help="Mute each account selected by the input",
    )
    action_group.add_argument(
        "--unmute",
        dest="action",
        action="store_const",
        const=ModerationAction.UNMUTE,
        help="Unmute each account selected by the input",
    )
    action_group.add_argument(
        "--unblock",
        dest="action",
        action="store_const",
        const=ModerationAction.UNBLOCK,
        help="Unblock each selected account with an existing block record",
    )
    parser.set_defaults(action=ModerationAction.BLOCK)

    parser.add_argument(
        "--delay",
        type=parse_delay,
        default=DEFAULT_DELAY,
        help="Delay between moderation operations (seconds)",
    )

    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Print users without changing moderation state (use --no-dry-run to apply)",
    )

    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        default=False,
        help="Skip the confirmation prompt before changing moderation state",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress per-account lines, only print the summary",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print verbose output",
        default=False,
    )

    return parser.parse_args()


def load_inputs_from_file(path: str) -> list[str]:
    """Load starter-pack/list inputs from a UTF-8 text file.

    The file is stat-checked first and rejected if it exceeds
    :data:`MAX_INPUT_FILE_SIZE`, so a multi-gigabyte paste cannot OOM the
    process before the read begins.

    Note that this function does not normalize or validate the inputs.
    Refer to ``normalize_input_uri`` for more information.

    Args:
        path: File path containing one starter-pack/list input per line.

    Returns:
        Non-empty input lines with surrounding whitespace removed.

    Raises:
        ValueError: If the file is missing, exceeds the size cap, cannot be
            read, or contains no usable pack lines.
    """

    file_path = Path(path)
    try:
        size = file_path.stat().st_size
    except OSError as error:
        msg = f"Could not stat source file {file_path}: {error}"
        raise ValueError(msg) from error

    if size > MAX_INPUT_FILE_SIZE:
        msg = (
            f"Source file {file_path} is {size} bytes, exceeding the "
            f"{MAX_INPUT_FILE_SIZE}-byte cap. Refusing to read."
        )
        raise ValueError(msg)

    try:
        with file_path.open(encoding="utf-8") as fp:
            inputs = [stripped for line in fp if (stripped := line.strip())]
    except OSError as error:
        msg = f"Could not read source file {file_path}: {error}"
        raise ValueError(msg) from error

    if inputs:
        return inputs

    msg = f"Source file {file_path} does not contain any starter-pack/list inputs"
    raise ValueError(msg)


def resolve_handle() -> str:
    """Resolve the Bluesky handle from the environment."""
    handle = os.getenv("BSKY_HANDLE")
    if not handle:
        msg = "Missing Bluesky handle. Set BSKY_HANDLE."
        raise ValueError(msg)
    return handle


def resolve_app_password() -> str:
    """Resolve the Bluesky app password from the environment.

    Returns:
        The app password to use for login.

    Raises:
        ValueError: If no password was provided and ``BSKY_APP_PASSWORD`` is not
            set.
    """

    env_password = os.getenv("BSKY_APP_PASSWORD")
    if not env_password:
        msg = "Missing app password. Set BSKY_APP_PASSWORD."
        raise ValueError(msg)
    return env_password


def supported_input_format_error() -> str:
    """Build the supported input error message.

    Returns:
        A message listing every supported starter-pack/list input format.
    """

    return (
        "Input must be one of: "
        "at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>, "
        "at://<did-or-handle>/app.bsky.graph.list/<rkey>, "
        "http(s)://bsky.app/start/<did-or-handle>/<rkey>, "
        "http(s)://bsky.app/starter-pack/<did-or-handle>/<rkey>, "
        "http(s)://bsky.app/profile/<did-or-handle>/lists/<rkey>, "
        "http(s)://bsky.app/starter-pack-short/<code>, "
        "or http(s)://go.bsky.app/<code>"
    )


def parse_at_uri(raw: str) -> PackReference | None:
    """Parse a supported list/starter-pack AT URI.

    Args:
        raw: User-provided input after whitespace trimming.

    Returns:
        A canonical pack/list reference when ``raw`` is an AT URI, otherwise
        ``None``.

    Raises:
        ValueError: If ``raw`` is an AT URI but has the wrong shape, an
            unsupported collection, or an over-long segment.
    """

    if not raw.startswith("at://"):
        return None

    parts = [part for part in raw[len("at://") :].split("/") if part]
    if len(parts) != 3:
        msg = (
            f"AT URI must have exactly 3 segments (did-or-handle, collection,"
            f" rkey); got {len(parts)}: {raw!r}"
        )
        raise ValueError(msg)

    identifier, collection, rkey = parts
    if collection not in {STARTER_PACK_COLLECTION, LIST_COLLECTION}:
        msg = (
            f"AT URI must use collection {STARTER_PACK_COLLECTION} or"
            f" {LIST_COLLECTION}; got {collection!r}"
        )
        raise ValueError(msg)

    for label, value in (("identifier", identifier), ("rkey", rkey)):
        if len(value) > MAX_AT_URI_SEGMENT_LENGTH:
            msg = (
                f"AT URI {label} is {len(value)} chars, exceeding the "
                f"{MAX_AT_URI_SEGMENT_LENGTH}-char cap"
            )
            raise ValueError(msg)

    return PackReference(
        identifier=identifier,
        rkey=rkey,
        collection=collection,
    )


def parse_starter_pack_path(path_source: str) -> PackReference | None:
    """Parse a canonical Bluesky starter pack URL path.

    Args:
        path_source: URL path or path-like input.

    Returns:
        A starter pack reference for ``/start/.../...`` or
        ``/starter-pack/.../...`` paths, otherwise ``None``.

    Raises:
        ValueError: If the identifier or record key exceeds the segment
            length limit.
    """

    parts = [part for part in path_source.split("/") if part]
    if len(parts) != 3 or parts[0] not in SUPPORTED_STARTER_PACK_PATHS:
        return None

    _, identifier, rkey = parts
    for label, value in (("identifier", identifier), ("rkey", rkey)):
        if len(value) > MAX_AT_URI_SEGMENT_LENGTH:
            msg = (
                f"Starter pack path {label} is {len(value)} chars, exceeding "
                f"the {MAX_AT_URI_SEGMENT_LENGTH}-char cap"
            )
            raise ValueError(msg)
    return PackReference(
        identifier=identifier,
        rkey=rkey,
        collection=STARTER_PACK_COLLECTION,
    )


def parse_list_path(path_source: str) -> PackReference | None:
    """Parse a canonical Bluesky direct-list URL path.

    Args:
        path_source: URL path or path-like input.

    Returns:
        A list reference for ``/profile/.../lists/...`` paths, otherwise
        ``None``.
    """

    parts = [part for part in path_source.split("/") if part]
    if (
        len(parts) != 4
        or parts[0] != PROFILE_PATH_SEGMENT
        or parts[2] != LISTS_PATH_SEGMENT
    ):
        return None

    _, identifier, _, rkey = parts
    for label, value in (("identifier", identifier), ("rkey", rkey)):
        if len(value) > MAX_AT_URI_SEGMENT_LENGTH:
            msg = (
                f"List path {label} is {len(value)} chars, exceeding the "
                f"{MAX_AT_URI_SEGMENT_LENGTH}-char cap"
            )
            raise ValueError(msg)
    return PackReference(
        identifier=identifier,
        rkey=rkey,
        collection=LIST_COLLECTION,
    )


def parse_short_pack_path(
    host: str | None, path_source: str
) -> ShortStarterPackLink | None:
    """Parse a Bluesky starter pack short-link path.

    Args:
        host: Normalized URL host, if one was present.
        path_source: URL path or path-like input.

    Returns:
        A short-link reference for ``go.bsky.app/<code>`` or
        ``bsky.app/starter-pack-short/<code>``, otherwise ``None``.

    Raises:
        ValueError: If the short-link code exceeds the segment length limit.
    """

    parts = [part for part in path_source.split("/") if part]
    if not parts:
        return None

    if host == BSKY_SHORT_LINK_HOST and len(parts) == 1:
        code = parts[0]
    elif (
        host in BSKY_APP_HOSTS
        and len(parts) == 2
        and parts[0] == STARTER_PACK_SHORT_PATH
    ):
        code = parts[1]
    else:
        return None

    if len(code) > MAX_AT_URI_SEGMENT_LENGTH:
        msg = (
            f"Short link code is {len(code)} chars, exceeding the "
            f"{MAX_AT_URI_SEGMENT_LENGTH}-char cap"
        )
        raise ValueError(msg)

    return ShortStarterPackLink(url=f"https://{BSKY_SHORT_LINK_HOST}/{code}")


def parse_pack_input(pack_input: str) -> PackInput:
    """Parse any supported starter-pack/list input format.

    Args:
        pack_input: Starter-pack/list AT URI, canonical Bluesky URL, or short
            link.

    Returns:
        A canonical pack reference, or a short-link reference that still needs
        network resolution.

    Raises:
        ValueError: If the input is empty, uses an unsupported scheme or host,
            or does not match a supported format.
    """

    raw = pack_input.strip()
    if not raw:
        msg = "Input cannot be empty"
        raise ValueError(msg)

    # AT URIs are already the format required by the Bluesky API, except that
    # handles are still accepted here and normalized to DIDs later.
    at_reference = parse_at_uri(raw)
    if at_reference is not None:
        return at_reference

    parsed = urlparse(raw)
    host = parsed.hostname.lower() if parsed.hostname else None
    path_source = parsed.path if parsed.scheme else raw

    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        raise ValueError(supported_input_format_error())

    if host is not None and host not in {
        *BSKY_APP_HOSTS,
        BSKY_SHORT_LINK_HOST,
    }:
        msg = f"Unsupported Bluesky URL host: {host}"
        raise ValueError(msg)

    if not parsed.scheme:
        # Users often paste links without the scheme, e.g.
        # bsky.app/starter-pack/user.bsky.social/rkey. Treat those as Bluesky
        # paths only when they start with a known host.
        for app_host in BSKY_APP_HOSTS:
            if path_source.startswith(f"{app_host}/"):
                host = app_host
                path_source = "/" + path_source[len(app_host) + 1 :]
                break
        if path_source.startswith(f"{BSKY_SHORT_LINK_HOST}/"):
            host = BSKY_SHORT_LINK_HOST
            path_source = "/" + path_source[len(BSKY_SHORT_LINK_HOST) + 1 :]

    reference = parse_starter_pack_path(path_source)
    if reference is not None:
        return reference

    list_reference = parse_list_path(path_source)
    if list_reference is not None:
        return list_reference

    # Short links do not contain the DID/rkey pair. Return a marker object so
    # normalization can resolve the link before trying to build an AT URI.
    short_link = parse_short_pack_path(host, path_source)
    if short_link is not None:
        return short_link

    raise ValueError(supported_input_format_error())


def resolve_short_starter_pack_url(short_link: ShortStarterPackLink) -> str:
    """Resolve a Bluesky short link to its canonical URL.

    Args:
        short_link: Validated Bluesky short link.

    Returns:
        The canonical URL returned by the short-link service.

    Raises:
        ValueError: If the short-link URL is not an HTTPS ``go.bsky.app`` URL.
        RuntimeError: If the short link cannot be resolved or resolves to an
            unusable response.
    """

    parsed = urlparse(short_link.url)
    if parsed.scheme != "https" or parsed.hostname != BSKY_SHORT_LINK_HOST:
        msg = f"Unsupported short link URL: {short_link.url}"
        raise ValueError(msg)

    # Bluesky's short-link service returns JSON when requested with this Accept
    # header. That avoids scraping HTML and gives us the canonical bsky.app URL.
    request = Request(  # noqa: S310
        short_link.url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )

    try:
        with urlopen(  # noqa: S310
            request,
            timeout=SHORT_LINK_TIMEOUT.total_seconds(),
        ) as response:
            body = response.read()
            content_type = response.headers.get("content-type", "")
            if body and "application/json" in content_type:
                raw = json.loads(body.decode("utf-8"))
                if isinstance(raw, dict):
                    payload = cast(dict[str, object], raw)
                    url = payload.get("url")
                    if isinstance(url, str) and url:
                        return url

            # Keep a redirect fallback for clients or environments where the
            # service responds with a normal HTTP redirect instead of JSON.
            final_url = response.geturl()
            if isinstance(final_url, str) and final_url != short_link.url:
                return final_url
    except HTTPError as error:
        msg = (
            f"Could not resolve short link {short_link.url}: HTTP {error.code}"
        )
        raise RuntimeError(msg) from error
    except (OSError, TimeoutError, URLError, json.JSONDecodeError) as error:
        msg = f"Could not resolve short link {short_link.url}: {error}"
        raise RuntimeError(msg) from error

    msg = f"Short link did not resolve to a supported URL: {short_link.url}"
    raise RuntimeError(msg)


def resolve_identifier_to_did(
    client: Client,
    identifier: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> str:
    """Resolve a handle-like identifier to a DID.

    Args:
        client: Authenticated AT Protocol client.
        identifier: DID or Bluesky handle.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        A DID suitable for building an AT URI.

    Raises:
        RuntimeError: If handle resolution succeeds but does not return a DID.
    """

    if identifier.startswith("did:"):
        return identifier

    response = call_with_rate_limit_retry(
        lambda: client.resolve_handle(identifier),
        context="resolve handle",
        reauth=reauth,
    )
    if response.did:
        return response.did

    msg = f"Could not resolve handle to DID: {identifier}"
    raise RuntimeError(msg)


def normalize_input_uri(
    client: Client,
    source_input: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> PackReference:
    """Normalize a supported input into a canonical ``PackReference``.

    Args:
        client: Authenticated AT Protocol client used for handle-to-DID
            resolution.
        source_input: Starter-pack/list AT URI, canonical Bluesky URL, or short
            link.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        A ``PackReference`` with the resolved DID, collection, and rkey.

    Raises:
        RuntimeError: If short-link resolution loops too many times.
        ValueError: If the input format is unsupported.
    """

    current_input = source_input
    for _ in range(3):
        parsed_input = parse_pack_input(current_input)
        if isinstance(parsed_input, ShortStarterPackLink):
            # A short link resolves to another supported input format, usually
            # https://bsky.app/start/<did>/<rkey>, so loop back through the parser.
            current_input = resolve_short_starter_pack_url(parsed_input)
            continue

        # The graph API requires the creator DID in the AT URI; web links
        # may contain a handle, so resolve that as the final parse step.
        did = resolve_identifier_to_did(
            client, parsed_input.identifier, reauth=reauth
        )
        return PackReference(
            identifier=did,
            rkey=parsed_input.rkey,
            collection=parsed_input.collection,
        )

    msg = f"Short link resolution loop exceeded for {source_input}"
    raise RuntimeError(msg)


def login(handle: str, app_password: str) -> tuple[Client, str]:
    """Log in to Bluesky and return the authenticated client.

    Args:
        handle: Bluesky handle or login identifier.
        app_password: Bluesky app password.

    Returns:
        A tuple containing the authenticated client and the signed-in account
        DID.

    Raises:
        RuntimeError: If login completes but the authenticated DID cannot be
            determined.
    """

    client = Client()
    profile = client.login(handle, app_password)

    did = profile.did
    if not did:
        did = getattr(getattr(client, "me", None), "did", None)

    if not isinstance(did, str) or not did:
        msg = "Unable to determine authenticated DID after login"
        raise RuntimeError(msg)

    return client, did


def _make_reauth_fn(
    client: Client,
    handle: str,
    app_password: str,
    summary: ModerationSummary,
) -> Callable[[], bool]:
    """Build a one-shot re-authentication callback for the current run.

    The access token can be revoked or expire mid-run (especially on long
    runs). The SDK raises ``UnauthorizedError`` (HTTP 401) when it tries to
    use a stale session. This helper re-issues ``client.login`` with the
    already-resolved env credentials, then lets the caller retry the failed
    operation.

    The once-per-run cap is implemented via a closure flag so a flapping PDS
    can't trap us in a re-auth loop. After a successful re-auth, subsequent
    401s propagate normally and are recorded as failures.

    Args:
        client: Authenticated AT Protocol client.
        handle: Bluesky handle used at startup.
        app_password: App password used at startup.
        summary: Run summary; ``reauths`` is incremented on success.

    Returns:
        A zero-arg callable returning ``True`` if a re-authentication was
        performed (caller should retry) and ``False`` if a re-auth was
        already done earlier in this run.
    """

    reauth_used = [False]

    def reauth() -> bool:
        if reauth_used[0]:
            return False
        client.login(handle, app_password)
        reauth_used[0] = True
        summary.reauths += 1
        _emit(
            "INFO: re-authenticated (session expired)",
            quiet=False,
            force=True,
        )
        return True

    return reauth


def fetch_starter_pack_list_uri(
    client: Client,
    at_uri: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> str:
    """Fetch the backing list URI for a starter pack.

    Args:
        client: Authenticated AT Protocol client.
        at_uri: Starter pack AT URI.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        The AT URI of the list that contains the starter pack accounts.

    Raises:
        RuntimeError: If the starter pack response is missing the expected
            list data.
    """

    params = models.AppBskyGraphGetStarterPack.Params(starter_pack=at_uri)
    response = call_with_rate_limit_retry(
        lambda: client.app.bsky.graph.get_starter_pack(params),
        context="fetch starter pack",
        reauth=reauth,
    )

    starter_pack = response.starter_pack
    list_view = starter_pack.list
    if list_view is None:
        msg = "Starter pack does not expose a backing list"
        raise RuntimeError(msg)

    list_uri = list_view.uri
    if not list_uri:
        msg = "Starter pack list URI is missing"
        raise RuntimeError(msg)

    return list_uri


def resolve_input_to_list_target(
    client: Client,
    source_input: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> ResolvedListTarget:
    """Resolve any supported input into a list URI target.

    Args:
        client: Authenticated AT Protocol client.
        source_input: Raw user input identifying a starter pack or list.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        A list target with source metadata for logging.

    Raises:
        RuntimeError: If starter-pack list resolution fails or normalization
            does not produce a valid AT URI.
        ValueError: If the input format is unsupported.
    """

    reference = normalize_input_uri(client, source_input, reauth=reauth)
    at_uri = (
        f"at://{reference.identifier}/{reference.collection}/{reference.rkey}"
    )

    if reference.collection == STARTER_PACK_COLLECTION:
        list_uri = fetch_starter_pack_list_uri(client, at_uri, reauth=reauth)
        return ResolvedListTarget(
            list_uri=list_uri, source_kind=SourceKind.STARTER_PACK
        )

    return ResolvedListTarget(list_uri=at_uri, source_kind=SourceKind.LIST)


def fetch_list_members(
    client: Client,
    list_uri: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> list[Member]:
    """Load all unique account members from a list.

    Args:
        client: Authenticated AT Protocol client.
        list_uri: List AT URI.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        Unique list members keyed by DID.
    """

    members_by_did: dict[str, Member] = {}
    cursor: str | None = None

    while True:
        params = models.AppBskyGraphGetList.Params(
            list=list_uri,
            limit=LIST_PAGE_SIZE,
            cursor=cursor,
        )
        response = call_with_rate_limit_retry(
            lambda params=params: client.app.bsky.graph.get_list(params),
            context="fetch members page",
            reauth=reauth,
        )
        for item in response.items:
            subject = item.subject
            did = subject.did
            if not did:
                continue

            handle = subject.handle if subject.handle else "<unknown>"
            members_by_did.setdefault(did, Member(did=did, handle=handle))

        next_cursor = response.cursor
        if next_cursor:
            cursor = next_cursor
            continue
        break

    return list(members_by_did.values())


def merge_unique_members(
    merged: dict[str, Member], new_members: list[Member]
) -> None:
    """Add members into ``merged``, keeping the first handle seen per DID.

    Args:
        merged: Mapping from account DID to member, updated in place.
        new_members: Members to insert when their DID is not already present.
    """

    for member in new_members:
        merged.setdefault(member.did, member)


def fetch_block_records(
    client: Client,
    repo_did: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> dict[str, str]:
    """Load the signed-in account's block records, keyed by subject DID.

    The Bluesky ``app.bsky.graph.getBlocks`` query returns profile views. It
    does not return the repository record key required to remove a block.
    This function uses the generated ``app.bsky.graph.block.list`` namespace
    instead. The namespace lists the signed-in repository's
    ``app.bsky.graph.block`` records and preserves each record URI.

    The function reads every page until the response has no cursor. It does
    not change the repository. If multiple records contain the same subject
    DID, the later record replaces the earlier mapping entry.

    Args:
        client: Authenticated AT Protocol client.
            The client must have an authenticated session.
        repo_did: DID of the signed-in account's repository. The value is
            passed to ``app.bsky.graph.block.list`` as the repository name.
        reauth: Optional callback that refreshes an expired session once.
            The callback returns ``True`` when the caller should retry the
            failed request.

    Returns:
        A dictionary that maps each blocked account DID to its block record
        URI. The URI has the form
        ``at://<repo-did>/app.bsky.graph.block/<rkey>``.

    Raises:
        AtProtocolError: If the PDS rejects a page request or the session
            cannot be refreshed.
        RuntimeError: If a rate-limit response requires a wait longer than
            ``RATE_LIMIT_MAX_WAIT``.
    """

    records_by_did: dict[str, str] = {}
    cursor: str | None = None

    while True:
        response = call_with_rate_limit_retry(
            lambda cursor=cursor: client.app.bsky.graph.block.list(
                repo_did,
                cursor=cursor,
                limit=BLOCKS_PAGE_SIZE,
            ),
            context="fetch block records page",
            reauth=reauth,
        )
        for uri, record in response.records.items():
            did = getattr(record, "subject", None)
            if isinstance(did, str) and did and isinstance(uri, str):
                records_by_did[did] = uri

        next_cursor = response.cursor
        if next_cursor:
            cursor = next_cursor
            continue
        break

    return records_by_did


def block_record_key(uri: str) -> str:
    """Extract and validate the record key from a block record URI."""

    parts = uri.split("/")
    if (
        len(parts) != 5
        or parts[0] != "at:"
        or parts[1] != ""
        or not parts[2]
        or parts[3] != BLOCK_COLLECTION
        or not parts[4]
    ):
        msg = f"Invalid block record URI: {uri!r}"
        raise ValueError(msg)

    return parts[4]


def current_time_iso(client: Client) -> str:
    """Return the timestamp format expected by Bluesky record creation.

    Args:
        client: Authenticated AT Protocol client. If the SDK exposes a
            timestamp helper, it is used first.

    Returns:
        Current UTC time as an RFC 3339 timestamp.
    """

    get_current_time_iso = getattr(client, "get_current_time_iso", None)
    if callable(get_current_time_iso):
        value = get_current_time_iso()
        if isinstance(value, str) and value:
            return value

    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class ModerationResult:
    """Result of a moderation run."""

    summary: ModerationSummary
    failures: list[str]
    skipped: list[str]
    cap_reached: bool = False


def _record_action_failure(
    *,
    action: ModerationAction,
    summary: ModerationSummary,
    failures: list[str],
    user: Member,
    error: Exception,
    is_verbose: bool,
    is_quiet: bool,
) -> None:
    """Record and print a failed moderation request."""

    summary.failed += 1
    failures.append(f"{action.value} {user.handle} ({user.did})")
    message = describe_error(error)
    suffix = f" -> {message}" if is_verbose else ""
    _emit(
        f"ERROR {action.value} {user.handle} ({user.did}){suffix}",
        quiet=is_quiet,
        force=True,
    )


def _apply_action_once(
    client: Client,
    *,
    action: ModerationAction,
    user: Member,
    self_did: str,
    block_record_uri: str | None,
) -> bool:
    """Apply one moderation request without retrying it.

    Block creation, mute, unmute, and unblock use different SDK calls. This
    function hides those differences from the retry and user-processing code.

    The caller supplies a block record URI for an unblock request. The URI is
    converted to a record key before the generated delete method is called.

    Args:
        client: Authenticated AT Protocol client used to send the request.
        action: Action to apply.
        user: Account that receives the moderation action.
        self_did: DID of the signed-in account. It is used as the repository
            name for block creation and deletion.
        block_record_uri: URI of the target's existing block record. It is
            required for unblock and ignored for mute and unmute.

    Returns:
        ``True`` when the SDK does not return an explicit ``False`` status.
        The SDK normally returns a boolean for these procedures. ``None`` is
        treated as success for compatible clients that return no body after a
        successful request.

    Raises:
        ValueError: If ``action`` is unsupported, or if ``block_record_uri``
            is not a valid block record URI.
        RuntimeError: If unblock is requested without a block record URI.
        AtProtocolError: If the PDS rejects the request. The caller handles
            retries and authentication recovery.
    """

    if action is ModerationAction.BLOCK:
        response = client.app.bsky.graph.block.create(
            self_did,
            models.AppBskyGraphBlock.Record(
                subject=user.did,
                created_at=current_time_iso(client),
            ),
        )
        return bool(getattr(response, "cid", None))
    if action is ModerationAction.MUTE:
        response = client.mute(user.did)
    elif action is ModerationAction.UNMUTE:
        response = client.unmute(user.did)
    elif action is ModerationAction.UNBLOCK:
        if block_record_uri is None:
            msg = f"No block record found for {user.did}"
            raise RuntimeError(msg)
        response = client.app.bsky.graph.block.delete(
            self_did,
            block_record_key(block_record_uri),
        )
    else:
        msg = f"Unsupported moderation action: {action.value}"
        raise ValueError(msg)

    # The SDK models these void procedures as bool. Treat an older or mocked
    # client returning None as success when the request itself did not fail.
    return response is not False


def _apply_action_with_retries(
    *,
    client: Client,
    action: ModerationAction,
    user: Member,
    self_did: str,
    block_record_uri: str | None,
    summary: ModerationSummary,
    failures: list[str],
    skipped: list[str],
    is_verbose: bool,
    is_quiet: bool,
    reauth: Callable[[], bool] | None,
) -> ActionOutcome:
    """Apply one moderation action with the shared retry policy."""

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            applied = _apply_action_once(
                client,
                action=action,
                user=user,
                self_did=self_did,
                block_record_uri=block_record_uri,
            )
        except UnauthorizedError as error:
            if reauth is not None and reauth():
                continue
            _record_action_failure(
                action=action,
                summary=summary,
                failures=failures,
                user=user,
                error=error,
                is_verbose=is_verbose,
                is_quiet=is_quiet,
            )
            return ActionOutcome.FAILED
        except BadRequestError as error:
            if action is ModerationAction.BLOCK and is_block_list_cap_error(
                error
            ):
                raise BlockListCapError(user.did, user.handle) from error
            summary.skipped_invalid += 1
            skipped.append(f"{action.value} {user.handle} ({user.did})")
            detail = f" -> {describe_error(error)}" if is_verbose else ""
            _emit(
                f"SKIP invalid {action.value} {user.handle} ({user.did})"
                f"{detail}",
                quiet=is_quiet,
            )
            return ActionOutcome.SKIPPED_INVALID
        except AtProtocolError as error:
            if not is_transient_error(error):
                _record_action_failure(
                    action=action,
                    summary=summary,
                    failures=failures,
                    user=user,
                    error=error,
                    is_verbose=is_verbose,
                    is_quiet=is_quiet,
                )
                return ActionOutcome.FAILED

            if attempt + 1 >= MAX_ATTEMPTS:
                _record_action_failure(
                    action=action,
                    summary=summary,
                    failures=failures,
                    user=user,
                    error=error,
                    is_verbose=is_verbose,
                    is_quiet=is_quiet,
                )
                return ActionOutcome.FAILED

            attempt += 1
            rate_limit_result = _pause_for_rate_limit_if_needed(
                error=error,
                user=user,
                summary=summary,
                failures=failures,
                is_quiet=is_quiet,
            )
            if rate_limit_result is False:
                return ActionOutcome.FAILED
            if rate_limit_result is True:
                continue

            _pause_before_retry(
                attempt=attempt,
                user=user,
                summary=summary,
            )
            continue
        except Exception as error:  # noqa: BLE001
            _record_action_failure(
                action=action,
                summary=summary,
                failures=failures,
                user=user,
                error=error,
                is_verbose=is_verbose,
                is_quiet=is_quiet,
            )
            return ActionOutcome.FAILED

        if not applied:
            _record_action_failure(
                action=action,
                summary=summary,
                failures=failures,
                user=user,
                error=RuntimeError(
                    f"{action.value} request returned an unsuccessful status"
                ),
                is_verbose=is_verbose,
                is_quiet=is_quiet,
            )
            return ActionOutcome.FAILED
        return ActionOutcome.APPLIED

    return ActionOutcome.FAILED


def _count_action_targets(
    action: ModerationAction,
    users: Sequence[Member],
    self_did: str,
    block_records: dict[str, str],
) -> int:
    """Count users that can receive the selected action."""

    return sum(
        1
        for user in users
        if user.did != self_did
        and (
            action
            not in {
                ModerationAction.BLOCK,
                ModerationAction.UNBLOCK,
            }
            or (
                action is ModerationAction.BLOCK
                and user.did not in block_records
            )
            or (
                action is ModerationAction.UNBLOCK
                and user.did in block_records
            )
        )
    )


def apply_users(
    client: Client,
    *,
    action: ModerationAction,
    users: Sequence[Member],
    self_did: str,
    block_records: dict[str, str],
    delay: dt.timedelta,
    dry_run: bool,
    is_verbose: bool,
    is_quiet: bool = False,
    reauth: Callable[[], bool] | None = None,
    summary: ModerationSummary,
) -> ModerationResult:
    """Apply one moderation action to each eligible account.

    The loop handles self and existing-state skips, dry-run output, retries,
    progress, delays, and block-list-cap handling for every action.
    """

    summary.discovered = len(users)
    failures: list[str] = []
    skipped: list[str] = []
    eligible_count = _count_action_targets(
        action,
        users,
        self_did,
        block_records,
    )

    for user in users:
        if user.did == self_did:
            summary.skipped_self += 1
            _emit(f"SKIP self {user.handle} ({user.did})", quiet=is_quiet)
            continue

        block_record_uri = block_records.get(user.did)
        if action is ModerationAction.BLOCK and block_record_uri is not None:
            summary.skipped_existing += 1
            _emit(
                f"SKIP already blocked {user.handle} ({user.did})",
                quiet=is_quiet,
            )
            continue
        if action is ModerationAction.UNBLOCK and block_record_uri is None:
            summary.skipped_existing += 1
            _emit(
                f"SKIP not blocked {user.handle} ({user.did})",
                quiet=is_quiet,
            )
            continue

        if dry_run:
            summary.planned += 1
            if is_verbose:
                _emit(
                    f"DRY {action.value.upper()} {user.handle} ({user.did})",
                    quiet=is_quiet,
                )
            _print_progress(
                action=action,
                completed=summary.planned,
                total=eligible_count,
                retries=summary.retries,
                skipped_invalid=summary.skipped_invalid,
                is_quiet=is_quiet,
                dry_run=True,
            )
            continue

        try:
            outcome = _apply_action_with_retries(
                client=client,
                action=action,
                user=user,
                self_did=self_did,
                block_record_uri=block_record_uri,
                summary=summary,
                failures=failures,
                skipped=skipped,
                is_verbose=is_verbose,
                is_quiet=is_quiet,
                reauth=reauth,
            )
        except BlockListCapError:
            summary.failed += 1
            summary.cap_reached = True
            failures.append(f"{action.value} {user.handle} ({user.did})")
            _emit(
                f"ERROR block list cap reached while blocking {user.handle} "
                f"({user.did})",
                quiet=is_quiet,
                force=True,
            )
            _emit(
                "Aborting: no further moderation requests will be made.",
                quiet=is_quiet,
                force=True,
            )
            break

        if outcome is ActionOutcome.APPLIED:
            summary.applied += 1
            _emit(
                f"{action.value.upper()} {user.handle} ({user.did})",
                quiet=is_quiet,
            )
            _print_progress(
                action=action,
                completed=summary.applied,
                total=eligible_count,
                retries=summary.retries,
                skipped_invalid=summary.skipped_invalid,
                is_quiet=is_quiet,
                dry_run=False,
            )
            if action is ModerationAction.BLOCK:
                block_records[user.did] = ""
            elif action is ModerationAction.UNBLOCK:
                block_records.pop(user.did, None)
            if delay > ZERO_DURATION:
                time.sleep(delay.total_seconds())

    return ModerationResult(
        summary=summary,
        failures=failures,
        skipped=skipped,
        cap_reached=summary.cap_reached,
    )


def extract_status_code(error: Exception) -> int | None:
    """Extract an HTTP status code from an SDK exception.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        The HTTP status code if available, otherwise ``None``.
    """

    response = getattr(error, "response", None)
    if response is None:
        return None

    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    return None


def extract_response_headers(error: Exception) -> dict[str, Any]:
    """Extract response headers from an SDK exception.

    The atproto SDK normalizes response headers to a lowercase-keyed
    ``dict[str, str]`` (see ``atproto_client.request._convert_headers_to_dict``),
    but the public type is ``dict[str, Any]``; this function preserves that type
    so callers do not have to cast.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        The response headers dict, or an empty dict when unavailable.
    """

    response = getattr(error, "response", None)
    if response is None:
        return {}

    headers = getattr(response, "headers", None)
    if isinstance(headers, dict):
        return headers
    return {}


def extract_rate_limit_wait(error: Exception) -> dt.timedelta | None:
    """Compute how long to sleep before the rate-limit window resets.

    Reads the ``ratelimit-reset`` header (Unix epoch seconds) first, then
    falls back to the standard ``retry-after`` header (delta seconds).  A
    small buffer is added so the retry lands safely after the window edge.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        Duration to wait (including buffer), or ``None`` when no usable
        rate-limit timing is available in the response.
    """

    headers = extract_response_headers(error)
    if not headers:
        return None

    reset_raw = headers.get("ratelimit-reset")
    if reset_raw is not None:
        try:
            reset_ts = float(reset_raw)
        except (TypeError, ValueError):
            pass
        else:
            wait_seconds = reset_ts - time.time()
            try:
                wait = dt.timedelta(seconds=wait_seconds) + RATE_LIMIT_BUFFER
            except (OverflowError, ValueError):
                pass
            else:
                return max(wait, RATE_LIMIT_BUFFER)

    retry_after_raw = headers.get("retry-after")
    if retry_after_raw is not None:
        try:
            retry_seconds = float(retry_after_raw)
        except (TypeError, ValueError):
            pass
        else:
            try:
                wait = dt.timedelta(seconds=retry_seconds) + RATE_LIMIT_BUFFER
            except (OverflowError, ValueError):
                pass
            else:
                return max(wait, RATE_LIMIT_BUFFER)

    return None


def describe_error(error: Exception) -> str:
    """Build a human-readable error message.

    Args:
        error: Exception to describe.

    Returns:
        The exception text, prefixed with an HTTP status code when available.
    """

    status_code = extract_status_code(error)
    if status_code is None:
        return str(error)
    return f"HTTP {status_code}: {error}"


def is_transient_error(error: BaseException) -> bool:
    """Decide whether a moderation failure should be retried.

    Uses SDK exception classes and the HTTP status code instead of fragile
    string matching. A failure is considered transient when:

    - it is a ``NetworkError`` (this includes ``InvokeTimeoutError``);
    - or it is a ``RequestException`` with a retryable status code (429, 5xx).

    All other exceptions (``BadRequestError``, ``UnauthorizedError``, unknown
    errors) are treated as permanent.

    Args:
        error: Exception raised during a block operation.

    Returns:
        ``True`` for transient failures; ``False`` otherwise.
    """

    if isinstance(error, NetworkError):
        return True

    if isinstance(error, RequestException):
        status_code = extract_status_code(error)
        if status_code == HTTP_STATUS_TOO_MANY_REQUESTS:
            return True
        if (
            isinstance(status_code, int)
            and status_code >= HTTP_STATUS_SERVER_ERROR_MIN
        ):
            return True

    return False


def is_block_list_cap_error(error: BaseException) -> bool:
    """Return ``True`` if ``error`` indicates the per-account block list cap.

    Checks the XRPC error code on the response payload first, then falls back
    to a substring match on the error message for SDK or server versions that
    do not surface the structured error.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        ``True`` when the error is the block-list-cap error, otherwise ``False``.
    """

    response = getattr(error, "response", None)
    content = (
        getattr(response, "content", None) if response is not None else None
    )

    error_code: str | None = None
    if isinstance(content, dict):
        code = content.get("error")
        if isinstance(code, str):
            error_code = code
    elif content is not None:
        code = getattr(content, "error", None)
        if isinstance(code, str):
            error_code = code

    if error_code == BLOCK_LIST_CAP_ERROR_CODE:
        return True

    message: str | None = None
    if isinstance(content, dict):
        msg = content.get("message")
        if isinstance(msg, str):
            message = msg
    elif content is not None:
        msg = getattr(content, "message", None)
        if isinstance(msg, str):
            message = msg
    if message is None:
        message = str(error)

    lowered = message.lower()
    return "block" in lowered and ("limit" in lowered or "exceeded" in lowered)


def is_input_unrecoverable(error: BaseException) -> bool:
    """Decide whether an input-resolution error should skip the input.

    Used in :func:`main` to recover from permanently-broken input sources
    (deleted starter pack, deleted list) without aborting the whole run.
    Treats the following as recoverable:

    - ``BadRequestError`` (HTTP 400: bad rkey, deleted record, etc.)
    - ``RequestException`` with status 404 (record not found / deleted)

    Args:
        error: Exception raised by the AT Protocol client.

    Returns:
        ``True`` when the input source is permanently broken and the
        caller should skip it and continue.
    """

    if isinstance(error, BadRequestError):
        return True
    if isinstance(error, RequestException):
        return extract_status_code(error) == 404
    return False


def call_with_rate_limit_retry[T](
    fn: Callable[[], T],
    *,
    context: str,
    reauth: Callable[[], bool] | None = None,
) -> T:
    """Call ``fn`` and transparently pause on HTTP 429 rate limits.

    On a 429 response the function reads ``ratelimit-reset`` (or
    ``retry-after``) from the response headers, sleeps until the window
    resets, and retries.  On an ``UnauthorizedError`` (HTTP 401) the optional
    ``reauth`` callback is invoked; if it returns ``True`` (a re-auth just
    succeeded) the function is retried once, otherwise the exception
    propagates.

    Args:
        fn: Zero-argument callable that performs a single API request.
        context: Human-readable label printed while pausing (e.g.
            ``"fetch members page"``).
        reauth: Optional callback that re-authenticates the client and
            returns ``True`` if the caller should retry. When ``None``,
            401s propagate as normal errors.

    Returns:
        The return value of ``fn`` on success.

    Raises:
        RuntimeError: If the rate-limit wait exceeds
            ``RATE_LIMIT_MAX_WAIT``.
    """

    while True:
        try:
            return fn()
        except Exception as error:
            if (
                isinstance(error, UnauthorizedError)
                and reauth is not None
                and reauth()
            ):
                continue
            status_code = extract_status_code(error)
            if status_code != HTTP_STATUS_TOO_MANY_REQUESTS:
                raise

            wait = extract_rate_limit_wait(error)
            if wait is None:
                raise

            resume_at = dt.datetime.fromtimestamp(
                time.time() + wait.total_seconds(),
                tz=dt.UTC,
            ).isoformat()
            if wait > RATE_LIMIT_MAX_WAIT:
                msg = (
                    f"Rate limit for {context} resets at {resume_at} "
                    f"({wait.total_seconds():.0f}s), exceeds max wait of "
                    f"{RATE_LIMIT_MAX_WAIT.total_seconds():.0f}s"
                )
                raise RuntimeError(msg) from error

            print(
                f"RATE LIMITED ({context}): pausing until {resume_at} ({wait.total_seconds():.0f}s)..."
            )
            time.sleep(wait.total_seconds())


def _pause_for_rate_limit_if_needed(
    *,
    error: Exception,
    user: Member,
    summary: ModerationSummary,
    failures: list[str],
    is_quiet: bool = False,
) -> bool | None:
    """Pause for a rate-limit response when retry timing is available.

    Args:
        error: Exception raised by the moderation operation.
        user: Account receiving the moderation operation.
        summary: Mutable run summary updated with retry or failure counts.
        failures: Mutable list receiving a failed account entry when the
            rate-limit wait exceeds ``RATE_LIMIT_MAX_WAIT``.
        is_quiet: When ``True``, suppress the per-user "aborting" error line.

    Returns:
        ``True`` when the caller should retry immediately after the pause,
        ``False`` when the wait was too long and the user was recorded as
        failed, or ``None`` when the error is not a usable rate-limit response.
    """

    status_code = extract_status_code(error)
    if status_code != HTTP_STATUS_TOO_MANY_REQUESTS:
        return None

    rate_limit_wait = extract_rate_limit_wait(error)
    if rate_limit_wait is None:
        return None

    resume_at = dt.datetime.fromtimestamp(
        time.time() + rate_limit_wait.total_seconds(),
        tz=dt.UTC,
    ).isoformat()
    if rate_limit_wait > RATE_LIMIT_MAX_WAIT:
        _emit(
            f"ERROR rate limit for {user.handle} ({user.did}) resets at {resume_at}"
            + f" ({rate_limit_wait.total_seconds():.0f}s), exceeds max wait of"
            + f" {RATE_LIMIT_MAX_WAIT.total_seconds():.0f}s — aborting",
            quiet=is_quiet,
            force=True,
        )
        summary.failed += 1
        failures.append(f"{user.handle} ({user.did})")
        return False

    _emit(
        f"RATE LIMITED: pausing until {resume_at} ({rate_limit_wait.total_seconds():.0f}s)...",
        quiet=is_quiet,
        force=True,
    )
    time.sleep(rate_limit_wait.total_seconds())
    summary.retries += 1
    return True


def _pause_before_retry(
    *,
    attempt: int,
    user: Member,
    summary: ModerationSummary,
) -> None:
    """Sleep before retrying a transient moderation failure.

    Args:
        attempt: One-based retry attempt number.
        user: Account receiving the moderation action.
        summary: Mutable run summary updated with the retry count.
    """

    summary.retries += 1
    base_seconds = BASE_BACKOFF.total_seconds()
    max_seconds = MAX_BACKOFF.total_seconds()
    backoff_seconds = min(max_seconds, base_seconds * (2 ** (attempt - 1)))
    backoff = dt.timedelta(seconds=backoff_seconds)
    jitter_seconds = uniform(*(sec.total_seconds() for sec in JITTER))
    wait = backoff + dt.timedelta(seconds=jitter_seconds)
    print(
        f"WARN transient error for {user.handle} ({user.did}); retry "
        + f"{attempt}/{MAX_ATTEMPTS} in {wait.total_seconds():.2f}s"
    )
    time.sleep(wait.total_seconds())


def _emit(msg: str, *, quiet: bool, force: bool = False) -> None:
    """Print ``msg`` to stdout unless ``quiet`` is set.

    Operational lines (rate limits, warnings, errors, cap-reached aborts) call
    with ``force=True`` so they always print even in ``--quiet`` mode.
    Per-account lines (BLOCK / SKIP / DRY BLOCK) call with ``force=False``
    (the default) and are suppressed in ``--quiet`` mode.
    """

    if not quiet or force:
        print(msg)


def _print_progress(
    *,
    action: ModerationAction,
    completed: int,
    total: int,
    retries: int,
    skipped_invalid: int,
    is_quiet: bool,
    dry_run: bool,
) -> None:
    """Write a periodic moderation progress line to stderr.

    Suppressed when ``is_quiet`` is true, when :data:`PROGRESS_EVERY` is 0,
    or when the interval has not yet been hit. In dry-run the count tracks
    planned actions rather than applied actions.

    The line is written with a trailing newline so progress history is
    preserved in non-TTY contexts (CI, redirected stderr).

    Args:
        action: Moderation action being processed.
        completed: Number of completed actions (or planned actions in dry-run).
        total: Total accounts eligible for the action.
        retries: Current retry count.
        skipped_invalid: Current skipped-invalid count.
        is_quiet: When ``True``, do nothing.
        dry_run: When ``True``, label the counter as planned.
    """

    if is_quiet or PROGRESS_EVERY <= 0 or total <= 0:
        return
    if completed == 0 or completed % PROGRESS_EVERY != 0:
        return

    label = f"would {action.value}" if dry_run else action.value
    line = (
        f"[{label} {completed}/{total} - retries {retries}"
        f" - skipped {skipped_invalid}]"
    )
    sys.stderr.write(line + "\n")
    sys.stderr.flush()


def print_summary(
    result: ModerationResult,
    action: ModerationAction,
    dry_run: bool,
) -> None:
    """Print one summary format for every moderation action."""

    summary = result.summary
    subject = "Members" if action is ModerationAction.BLOCK else "Accounts"
    print("\nSummary")
    print(f"{subject} discovered: {summary.discovered}")
    print(f"Skipped self: {summary.skipped_self}")
    if action is ModerationAction.BLOCK:
        print(f"Skipped already blocked: {summary.skipped_existing}")
    elif action is ModerationAction.UNBLOCK:
        print(f"Skipped not blocked: {summary.skipped_existing}")
    print(f"Skipped invalid: {summary.skipped_invalid}")

    if dry_run:
        print(f"Would {action.value}: {summary.planned}")
    else:
        verb = {
            ModerationAction.BLOCK: "Blocked",
            ModerationAction.MUTE: "Muted",
            ModerationAction.UNMUTE: "Unmuted",
            ModerationAction.UNBLOCK: "Unblocked",
        }[action]
        print(f"{verb} successfully: {summary.applied}")

    print(f"Failures: {summary.failed}")
    print(f"Retries used: {summary.retries}")
    print(f"Re-authentications: {summary.reauths}")

    if result.cap_reached:
        print(
            "\nBlock list cap reached: further moderation actions were skipped."
            " Unblock some accounts to continue."
        )

    if result.failures:
        print("\nFailed entries:")
        for failure in result.failures:
            print(f"- {failure}")

    if result.skipped:
        print("\nSkipped (invalid) entries:")
        for entry in result.skipped:
            print(f"- {entry}")


def main() -> None:
    """Run the command-line workflow.

    Exit codes:
        0 — every member processed without failures.
        1 — at least one moderation operation failed.
        2 — user aborted the destructive confirmation, or stdin was not a TTY
            without ``--yes``.
        3 — the PDS block-list cap was reached and the run was aborted.

    Raises:
        SystemExit: When the exit code is non-zero.
    """

    args = parse_args()

    handle = resolve_handle()
    app_password = resolve_app_password()
    client, self_did = login(handle, app_password)

    summary = ModerationSummary()
    reauth = _make_reauth_fn(client, handle, app_password, summary)

    source_inputs: list[str]
    if args.input is not None:
        source_inputs = [args.input]
    else:
        source_inputs = load_inputs_from_file(args.file)

    merged: dict[str, Member] = {}
    skipped_inputs: list[str] = []
    for s_input in source_inputs:
        try:
            target = resolve_input_to_list_target(
                client, s_input, reauth=reauth
            )
            print(f"INFO: Using {target.source_kind} {target.list_uri}")
            pack_members = fetch_list_members(
                client, target.list_uri, reauth=reauth
            )
        except Exception as error:
            if not is_input_unrecoverable(error):
                raise
            skipped_inputs.append(s_input)
            print(f"WARN: input {s_input} skipped: {describe_error(error)}")
            continue
        print(f"INFO:   Loaded {len(pack_members)} members from this input")
        merge_unique_members(merged, pack_members)
    users = list(merged.values())
    print(
        f"INFO: Loaded {len(users)} unique members across "
        f"{len(source_inputs)} input source(s)"
    )
    if skipped_inputs:
        print(
            f"INFO: Skipped {len(skipped_inputs)} input source(s) due to"
            " bad request errors"
        )

    block_records = (
        fetch_block_records(client, self_did, reauth=reauth)
        if args.action in {ModerationAction.BLOCK, ModerationAction.UNBLOCK}
        else {}
    )
    target_count = _count_action_targets(
        args.action,
        users,
        self_did,
        block_records,
    )

    if not args.dry_run and not args.yes:
        if target_count > 0 and not confirm_destructive(
            target_count, args.action.value
        ):
            print("Aborted by user.", file=sys.stderr)
            raise SystemExit(2)
        print(f"INFO: confirmed, {args.action.value} {target_count} accounts")

    result = apply_users(
        client,
        action=args.action,
        users=users,
        self_did=self_did,
        block_records=block_records,
        delay=args.delay,
        dry_run=args.dry_run,
        is_verbose=args.verbose,
        is_quiet=args.quiet,
        reauth=reauth,
        summary=summary,
    )
    print_summary(result, args.action, args.dry_run)

    if result.cap_reached:
        raise SystemExit(3)
    if result.summary.failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
