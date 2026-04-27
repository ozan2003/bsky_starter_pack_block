#!/usr/bin/env python3
"""Block every account listed in a Bluesky starter pack.

This script logs in to Bluesky with an app password, resolves a starter pack
link to its backing list, loads every account in that list, skips your own
account and accounts you already block, then creates block records for the
remaining accounts.

Supported starter pack inputs:
    - ``at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>``
    - ``https://bsky.app/start/<did-or-handle>/<rkey>``
    - ``https://bsky.app/starter-pack/<did-or-handle>/<rkey>``
    - ``https://bsky.app/starter-pack-short/<code>``
    - ``https://go.bsky.app/<code>``

Usage:
    First create an app password in Bluesky under Settings -> Privacy and
    Security -> App Passwords. Prefer storing it in an environment variable so
    it does not appear in shell history or process listings:

    ``export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"``

    Run a dry run before blocking:

    ``python3 bsky.py --handle user.bsky.social --pack <starter-pack-link> --dry-run``

    If the dry run looks correct, run without ``--dry-run``:

    ``python3 bsky.py --handle user.bsky.social --pack <starter-pack-link>``

    You can pass ``--delay`` to control the pause between block operations.
    ``--app-password`` is supported, but the environment variable is safer.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isinf, isnan
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from atproto import Client, models

STARTER_PACK_COLLECTION = "app.bsky.graph.starterpack"
SUPPORTED_STARTER_PACK_PATHS = {"start", "starter-pack"}
BSKY_APP_HOSTS = {"bsky.app", "www.bsky.app"}
BSKY_SHORT_LINK_HOST = "go.bsky.app"
STARTER_PACK_SHORT_PATH = "starter-pack-short"
SHORT_LINK_TIMEOUT_SECONDS = 10.0
LIST_PAGE_SIZE = 100
BLOCKS_PAGE_SIZE = 100
MAX_BLOCK_RETRIES = 4
BASE_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 8.0


@dataclass
class Member:
    """Starter pack member selected for possible blocking.

    Attributes:
        did: Account DID used as the stable block target.
        handle: Current account handle, used only for human-readable output.
    """

    did: str
    handle: str


@dataclass
class BlockSummary:
    """Counters collected while processing starter pack members.

    Attributes:
        discovered: Number of unique starter pack members loaded.
        skipped_self: Number of members skipped because they are the signed-in
            account.
        skipped_already_blocked: Number of members skipped because a block
            already exists.
        would_block: Number of members that would be blocked in dry-run mode.
        blocked: Number of block records created successfully.
        failed: Number of members that could not be blocked.
        retries: Number of retry attempts used for transient block failures.
    """

    discovered: int = 0
    skipped_self: int = 0
    skipped_already_blocked: int = 0
    would_block: int = 0
    blocked: int = 0
    failed: int = 0
    retries: int = 0


@dataclass(frozen=True)
class PackReference:
    """Canonical starter pack reference before DID normalization.

    Attributes:
        identifier: Starter pack creator DID or handle.
        rkey: Starter pack record key.
    """

    identifier: str
    rkey: str


@dataclass(frozen=True)
class ShortStarterPackLink:
    """Bluesky short link that must be resolved before API use.

    Attributes:
        url: Canonical short-link service URL.
    """

    url: str


type PackInput = PackReference | ShortStarterPackLink


def parse_delay(value: str) -> float:
    """Parse a CLI delay value.

    Args:
        value: Raw command-line value for ``--delay``.

    Returns:
        A finite, non-negative delay in seconds.

    Raises:
        argparse.ArgumentTypeError: If the delay is negative, infinite, or NaN.
        ValueError: If ``value`` cannot be parsed as a float.
    """

    delay = float(value)
    if delay < 0:
        msg = "--delay must be greater than or equal to 0"
        raise argparse.ArgumentTypeError(msg)
    if isinf(delay) or isnan(delay):
        msg = "--delay must be a finite number"
        raise argparse.ArgumentTypeError(msg)
    return delay


def parse_args() -> argparse.Namespace:
    """Parse command-line options.

    Returns:
        Parsed command-line arguments for login, starter pack selection,
        throttling, and dry-run mode.
    """

    parser = argparse.ArgumentParser(
        description="Block all users from a Bluesky starter pack",
    )

    parser.add_argument(
        "--handle",
        required=True,
        type=str,
        help="Bluesky handle (e.g. user.bsky.social)",
    )

    parser.add_argument(
        "--app-password",
        type=str,
        default=None,
        help="Bluesky app password (or set BSKY_APP_PASSWORD)",
    )

    parser.add_argument(
        "--pack",
        required=True,
        type=str,
        help="Starter pack URL or AT URI",
    )

    parser.add_argument(
        "--delay",
        type=parse_delay,
        default=0.5,
        help="Delay between blocks (seconds)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print users without blocking",
    )

    return parser.parse_args()


def resolve_app_password(cli_password: str | None) -> str:
    """Resolve the Bluesky app password from CLI input or environment.

    Args:
        cli_password: Password passed through ``--app-password``.

    Returns:
        The app password to use for login.

    Raises:
        ValueError: If no password was provided and ``BSKY_APP_PASSWORD`` is not
            set.
    """

    if cli_password:
        return cli_password

    env_password = os.getenv("BSKY_APP_PASSWORD")
    if env_password:
        return env_password

    msg = "Missing app password. Provide --app-password or set BSKY_APP_PASSWORD."
    raise ValueError(msg)


def as_dict(value: object) -> dict[str, object]:
    """Convert SDK model-like values to plain dictionaries.

    Args:
        value: A dictionary, Pydantic model, SDK model, or object with
            attributes.

    Returns:
        A plain dictionary representation of ``value``.

    Raises:
        TypeError: If ``value`` cannot be represented as a dictionary.
    """

    if isinstance(value, dict):
        return cast(dict[str, object], value)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(by_alias=True)
        if isinstance(dumped, dict):
            return cast(dict[str, object], dumped)

    dict_method = getattr(value, "dict", None)
    if callable(dict_method):
        try:
            dumped = dict_method(by_alias=True)
        except TypeError:
            dumped = dict_method()
        if isinstance(dumped, dict):
            return cast(dict[str, object], dumped)

    if hasattr(value, "__dict__"):
        return cast(dict[str, object], dict(vars(value)))

    msg = f"Unexpected response type: {type(value)!r}"
    raise TypeError(msg)


def starter_pack_format_error() -> str:
    """Build the supported starter pack input error message.

    Returns:
        A message listing every supported starter pack input format.
    """

    return (
        "Starter pack input must be one of: "
        "at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>, "
        "https://bsky.app/start/<did-or-handle>/<rkey>, "
        "https://bsky.app/starter-pack/<did-or-handle>/<rkey>, "
        "https://bsky.app/starter-pack-short/<code>, "
        "or https://go.bsky.app/<code>"
    )


def parse_at_uri(raw: str) -> PackReference | None:
    """Parse a starter pack AT URI.

    Args:
        raw: User-provided input after whitespace trimming.

    Returns:
        A starter pack reference when ``raw`` is an AT URI, otherwise ``None``.

    Raises:
        ValueError: If ``raw`` is an AT URI but has the wrong shape or
            collection.
    """

    if not raw.startswith("at://"):
        return None

    parts = [part for part in raw[len("at://") :].split("/") if part]
    if len(parts) != 3:
        msg = (
            "AT URI must be in the format "
            "at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>"
        )
        raise ValueError(msg)

    identifier, collection, rkey = parts
    if collection != STARTER_PACK_COLLECTION:
        msg = f"Starter pack AT URI must use collection {STARTER_PACK_COLLECTION}"
        raise ValueError(msg)

    return PackReference(identifier=identifier, rkey=rkey)


def parse_starter_pack_path(path_source: str) -> PackReference | None:
    """Parse a canonical Bluesky starter pack URL path.

    Args:
        path_source: URL path or path-like input.

    Returns:
        A starter pack reference for ``/start/.../...`` or
        ``/starter-pack/.../...`` paths, otherwise ``None``.
    """

    parts = [part for part in path_source.split("/") if part]
    if len(parts) != 3 or parts[0] not in SUPPORTED_STARTER_PACK_PATHS:
        return None

    _, identifier, rkey = parts
    return PackReference(identifier=identifier, rkey=rkey)


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
    """

    parts = [part for part in path_source.split("/") if part]
    if not parts:
        return None

    if host == BSKY_SHORT_LINK_HOST and len(parts) == 1:
        return ShortStarterPackLink(
            url=f"https://{BSKY_SHORT_LINK_HOST}/{parts[0]}"
        )

    if (
        host in BSKY_APP_HOSTS
        and len(parts) == 2
        and parts[0] == STARTER_PACK_SHORT_PATH
    ):
        return ShortStarterPackLink(
            url=f"https://{BSKY_SHORT_LINK_HOST}/{parts[1]}"
        )

    return None


def parse_pack_input(pack_input: str) -> PackInput:
    """Parse any supported starter pack input format.

    Args:
        pack_input: Starter pack AT URI, canonical Bluesky URL, or short link.

    Returns:
        A canonical pack reference, or a short-link reference that still needs
        network resolution.

    Raises:
        ValueError: If the input is empty, uses an unsupported scheme or host,
            or does not match a supported starter pack format.
    """

    raw = pack_input.strip()
    if not raw:
        msg = "Starter pack input cannot be empty"
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
        raise ValueError(starter_pack_format_error())

    if host is not None and host not in {
        *BSKY_APP_HOSTS,
        BSKY_SHORT_LINK_HOST,
    }:
        msg = f"Unsupported starter pack URL host: {host}"
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

    # Short links do not contain the DID/rkey pair. Return a marker object so
    # normalization can resolve the link before trying to build an AT URI.
    short_link = parse_short_pack_path(host, path_source)
    if short_link is not None:
        return short_link

    raise ValueError(starter_pack_format_error())


def resolve_short_starter_pack_url(short_link: ShortStarterPackLink) -> str:
    """Resolve a Bluesky short link to its canonical starter pack URL.

    Args:
        short_link: Validated Bluesky starter pack short link.

    Returns:
        The canonical URL returned by the short-link service.

    Raises:
        ValueError: If the short-link URL is not an HTTPS ``go.bsky.app`` URL.
        RuntimeError: If the short link cannot be resolved or resolves to an
            unusable response.
    """

    parsed = urlparse(short_link.url)
    if parsed.scheme != "https" or parsed.hostname != BSKY_SHORT_LINK_HOST:
        msg = f"Unsupported starter pack short link URL: {short_link.url}"
        raise ValueError(msg)

    # Bluesky's short-link service returns JSON when requested with this Accept
    # header. That avoids scraping HTML and gives us the canonical bsky.app URL.
    request = Request(  # noqa: S310
        short_link.url,
        headers={
            "Accept": "application/json",
            "User-Agent": "bsky-starter-pack-blocker/1.0",
        },
    )

    try:
        with urlopen(request, timeout=SHORT_LINK_TIMEOUT_SECONDS) as response:  # noqa: S310
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
        msg = f"Could not resolve starter pack short link {short_link.url}: HTTP {error.code}"
        raise RuntimeError(msg) from error
    except (OSError, TimeoutError, URLError, json.JSONDecodeError) as error:
        msg = f"Could not resolve starter pack short link {short_link.url}: {error}"
        raise RuntimeError(msg) from error

    msg = f"Short link did not resolve to a starter pack URL: {short_link.url}"
    raise RuntimeError(msg)


def resolve_identifier_to_did(client: Client, identifier: str) -> str:
    """Resolve a handle-like identifier to a DID.

    Args:
        client: Authenticated AT Protocol client.
        identifier: DID or Bluesky handle.

    Returns:
        A DID suitable for building an AT URI.

    Raises:
        RuntimeError: If handle resolution succeeds but does not return a DID.
    """

    if identifier.startswith("did:"):
        return identifier

    response = client.resolve_handle(identifier)
    response_dict = as_dict(response)
    did = response_dict.get("did")
    if isinstance(did, str) and did:
        return did

    msg = f"Could not resolve handle to DID: {identifier}"
    raise RuntimeError(msg)


def normalize_starter_pack_uri(client: Client, pack_input: str) -> str:
    """Normalize a starter pack input into the AT URI expected by the API.

    Args:
        client: Authenticated AT Protocol client used for handle-to-DID
            resolution.
        pack_input: Starter pack AT URI, canonical Bluesky URL, or short link.

    Returns:
        A canonical starter pack AT URI using the creator DID.

    Raises:
        RuntimeError: If short-link resolution loops too many times.
        ValueError: If the starter pack input format is unsupported.
    """

    current_input = pack_input
    for _ in range(3):
        parsed_input = parse_pack_input(current_input)
        if isinstance(parsed_input, PackReference):
            # The graph API requires the creator DID in the AT URI; web links
            # may contain a handle, so resolve that as the final parse step.
            did = resolve_identifier_to_did(client, parsed_input.identifier)
            return f"at://{did}/{STARTER_PACK_COLLECTION}/{parsed_input.rkey}"

        # A short link resolves to another supported input format, usually
        # https://bsky.app/start/<did>/<rkey>, so loop back through the parser.
        current_input = resolve_short_starter_pack_url(parsed_input)

    msg = f"Starter pack short link resolution loop exceeded for {pack_input}"
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

    profile_dict = as_dict(profile)
    did = profile_dict.get("did")
    if not isinstance(did, str) or not did:
        did = getattr(getattr(client, "me", None), "did", None)

    if not isinstance(did, str) or not did:
        msg = "Unable to determine authenticated DID after login"
        raise RuntimeError(msg)

    return client, did


def fetch_starter_pack_list_uri(client: Client, at_uri: str) -> str:
    """Fetch the backing list URI for a starter pack.

    Args:
        client: Authenticated AT Protocol client.
        at_uri: Starter pack AT URI.

    Returns:
        The AT URI of the list that contains the starter pack accounts.

    Raises:
        RuntimeError: If the starter pack response is missing the expected
            list data.
    """

    params = models.AppBskyGraphGetStarterPack.Params(starter_pack=at_uri)
    response = as_dict(client.app.bsky.graph.get_starter_pack(params))

    starter_pack = response.get("starterPack")
    if not isinstance(starter_pack, dict):
        msg = "Starter pack response is missing starterPack data"
        raise RuntimeError(msg)

    starter_pack_dict = cast(dict[str, object], starter_pack)
    list_view = starter_pack_dict.get("list")
    if not isinstance(list_view, dict):
        msg = "Starter pack does not expose a backing list"
        raise RuntimeError(msg)

    list_view_dict = cast(dict[str, object], list_view)
    list_uri = list_view_dict.get("uri")
    if not isinstance(list_uri, str) or not list_uri:
        msg = "Starter pack list URI is missing"
        raise RuntimeError(msg)

    return list_uri


def fetch_members(client: Client, at_uri: str) -> list[Member]:
    """Load all unique account members from a starter pack.

    Args:
        client: Authenticated AT Protocol client.
        at_uri: Starter pack AT URI.

    Returns:
        Unique starter pack members keyed by DID.

    Raises:
        RuntimeError: If the list API response is missing its items array.
    """

    list_uri = fetch_starter_pack_list_uri(client, at_uri)
    members_by_did: dict[str, Member] = {}
    cursor: str | None = None

    while True:
        params = models.AppBskyGraphGetList.Params(
            list=list_uri,
            limit=LIST_PAGE_SIZE,
            cursor=cursor,
        )
        response = as_dict(client.app.bsky.graph.get_list(params))
        items = response.get("items")
        if not isinstance(items, list):
            msg = "List response is missing items"
            raise RuntimeError(msg)

        list_items = cast(list[object], items)
        for item in list_items:
            item_dict = as_dict(item)
            subject = item_dict.get("subject")
            if subject is None:
                continue

            subject_dict = as_dict(subject)
            did = subject_dict.get("did")
            if not isinstance(did, str) or not did:
                continue

            handle_value = subject_dict.get("handle")
            handle = (
                handle_value
                if isinstance(handle_value, str) and handle_value
                else "<unknown>"
            )
            members_by_did.setdefault(did, Member(did=did, handle=handle))

        next_cursor = response.get("cursor")
        if isinstance(next_cursor, str) and next_cursor:
            cursor = next_cursor
            continue
        break

    return list(members_by_did.values())


def fetch_blocked_dids(client: Client) -> set[str]:
    """Load all DIDs already blocked by the signed-in account.

    Args:
        client: Authenticated AT Protocol client.

    Returns:
        DIDs for accounts that are already blocked.

    Raises:
        RuntimeError: If the blocks API response is missing its blocks array.
    """

    blocked_dids: set[str] = set()
    cursor: str | None = None

    while True:
        params = models.AppBskyGraphGetBlocks.Params(
            limit=BLOCKS_PAGE_SIZE,
            cursor=cursor,
        )
        response = as_dict(client.app.bsky.graph.get_blocks(params))
        blocks = response.get("blocks")
        if not isinstance(blocks, list):
            msg = "Blocks response is missing blocks"
            raise RuntimeError(msg)

        block_items = cast(list[object], blocks)
        for block in block_items:
            block_dict = as_dict(block)
            did = block_dict.get("did")
            if isinstance(did, str) and did:
                blocked_dids.add(did)

        next_cursor = response.get("cursor")
        if isinstance(next_cursor, str) and next_cursor:
            cursor = next_cursor
            continue
        break

    return blocked_dids


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

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def create_block_record(client: Client, did: str) -> None:
    """Create a Bluesky block record for one account.

    Args:
        client: Authenticated AT Protocol client.
        did: DID of the account to block.

    Raises:
        RuntimeError: If the client does not expose the signed-in repo DID when
            the low-level block fallback is needed.
    """

    block_method = getattr(client, "block", None)
    if callable(block_method):
        block_method(did)
        return

    repo_did = getattr(getattr(client, "me", None), "did", None)
    if not isinstance(repo_did, str) or not repo_did:
        msg = "Unable to determine repo DID for block operation"
        raise RuntimeError(msg)

    record_data: dict[str, str] = {
        "subject": did,
        "created_at": current_time_iso(client),
    }
    record = models.AppBskyGraphBlock.Record.model_validate(record_data)
    client.app.bsky.graph.block.create(
        repo_did,
        record,
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


def is_transient_error(error: Exception) -> bool:
    """Decide whether a block failure should be retried.

    Args:
        error: Exception raised during a block operation.

    Returns:
        ``True`` for network errors, timeouts, rate limits, and server errors;
        otherwise ``False``.
    """

    error_name = type(error).__name__
    if error_name in {"NetworkError", "InvokeTimeoutError"}:
        return True

    status_code = extract_status_code(error)
    if status_code == 429:
        return True
    if isinstance(status_code, int) and status_code >= 500:
        return True

    text = str(error).lower()
    return "rate limit" in text or "temporarily unavailable" in text


def block_users(
    client: Client,
    users: list[Member],
    self_did: str,
    blocked_dids: set[str],
    delay: float,
    dry_run: bool,
) -> tuple[BlockSummary, list[str]]:
    """Block each eligible starter pack member.

    Args:
        client: Authenticated AT Protocol client.
        users: Starter pack members to evaluate.
        self_did: DID of the signed-in account, which is always skipped.
        blocked_dids: Mutable set of DIDs already blocked before processing.
            Successfully blocked DIDs are added to this set.
        delay: Seconds to sleep after each successful block.
        dry_run: When ``True``, print intended actions without creating block
            records.

    Returns:
        A summary of the run and human-readable failure entries.
    """

    summary = BlockSummary(discovered=len(users))
    failures: list[str] = []

    for user in users:
        did = user.did
        handle = user.handle

        if did == self_did:
            summary.skipped_self += 1
            print(f"SKIP self {handle} ({did})")
            continue

        if did in blocked_dids:
            summary.skipped_already_blocked += 1
            print(f"SKIP already blocked {handle} ({did})")
            continue

        if dry_run:
            summary.would_block += 1
            print(f"DRY BLOCK {handle} ({did})")
            continue

        success = False
        attempt = 0
        while attempt <= MAX_BLOCK_RETRIES:
            try:
                create_block_record(client, did)
                success = True
                break
            except Exception as error:  # noqa: BLE001
                if (
                    not is_transient_error(error)
                    or attempt == MAX_BLOCK_RETRIES
                ):
                    error_text = describe_error(error)
                    summary.failed += 1
                    failures.append(f"{handle} ({did}) -> {error_text}")
                    print(f"ERROR {handle} ({did}) -> {error_text}")
                    break

                attempt += 1
                summary.retries += 1
                backoff = min(
                    MAX_BACKOFF_SECONDS,
                    BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                )
                wait_seconds = backoff
                print(
                    f"WARN transient error for {handle} ({did}); retry {attempt}/{MAX_BLOCK_RETRIES} in {wait_seconds:.2f}s"
                )
                time.sleep(wait_seconds)

        if success:
            blocked_dids.add(did)
            summary.blocked += 1
            print(f"BLOCK {handle} ({did})")
            if delay > 0:
                time.sleep(delay)

    return summary, failures


def print_summary(
    summary: BlockSummary, failures: list[str], dry_run: bool
) -> None:
    """Print the run summary and any failed entries.

    Args:
        summary: Counters collected while processing users.
        failures: Human-readable failure entries.
        dry_run: Whether the run was executed in dry-run mode.
    """

    print("\nSummary")
    print(f"Members discovered: {summary.discovered}")
    print(f"Skipped self: {summary.skipped_self}")
    print(f"Skipped already blocked: {summary.skipped_already_blocked}")

    if dry_run:
        print(f"Would block: {summary.would_block}")
    else:
        print(f"Blocked successfully: {summary.blocked}")

    print(f"Failures: {summary.failed}")
    print(f"Retries used: {summary.retries}")

    if failures:
        print("\nFailed entries:")
        for failure in failures:
            print(f"- {failure}")


def main() -> None:
    """Run the command-line workflow.

    Raises:
        SystemExit: If at least one block operation failed.
    """

    args = parse_args()

    app_password = resolve_app_password(args.app_password)
    client, self_did = login(args.handle, app_password)
    at_uri = normalize_starter_pack_uri(client, args.pack)
    print(f"Using starter pack {at_uri}")

    users = fetch_members(client, at_uri)
    print(f"Loaded {len(users)} unique starter pack members")

    blocked_dids = fetch_blocked_dids(client)
    print(f"Loaded {len(blocked_dids)} already-blocked accounts")

    summary, failures = block_users(
        client=client,
        users=users,
        self_did=self_did,
        blocked_dids=blocked_dids,
        delay=args.delay,
        dry_run=args.dry_run,
    )
    print_summary(summary, failures, args.dry_run)

    if summary.failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
