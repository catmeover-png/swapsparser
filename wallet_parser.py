#!/usr/bin/env python3
"""
Single-file Base wallet analytics parser → Google Sheets.

Reads:
- Config
- Wallets

Writes:
- Raw_Transactions
- Transfers
- Swaps
- Final

API:
- Alchemy JSON-RPC on Base mainnet
- alchemy_getAssetTransfers for transfers
- eth_blockNumber / eth_getBlockByNumber for date → block range

Required env vars for GitHub Actions:
- BASESCAN_API_KEY  (kept name for backwards compatibility; actually holds Alchemy key)
- GOOGLE_SHEET_ID or GSHEET_ID
- GOOGLE_SERVICE_ACCOUNT_JSON

Optional env vars:
- LMTS_ADDRESS
- USDC_ADDRESS
- RATE_LIMIT_RPS
- ALCHEMY_BASE_URL  (default: https://base-mainnet.g.alchemy.com/v2)
"""

import os
import json
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from collections import defaultdict
from typing import Any, Iterable

import requests
import gspread
from google.oauth2.service_account import Credentials


getcontext().prec = 50

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("wallet_parser")


# =========================
# ENV CONFIG
# =========================

ALCHEMY_API_KEY = os.getenv("BASESCAN_API_KEY", "").strip()
ALCHEMY_BASE_URL = os.getenv(
    "ALCHEMY_BASE_URL",
    "https://base-mainnet.g.alchemy.com/v2",
).strip()

RATE_LIMIT_RPS = int(os.getenv("RATE_LIMIT_RPS", "4"))

GOOGLE_SHEET_ID = (
    os.getenv("GOOGLE_SHEET_ID", "").strip()
    or os.getenv("GSHEET_ID", "").strip()
)
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

BASESCAN_TX_URL = "https://basescan.org/tx/"

DEFAULT_LMTS_ADDRESS = "0x9eadbe35f3ee3bf3e28180070c429298a1b02f93"
DEFAULT_USDC_ADDRESS = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


# =========================
# DATA MODELS
# =========================

@dataclass
class ParserConfig:
    start_utc: str
    end_utc: str
    start_ts: int
    end_ts: int
    chain: str
    token_main_symbol: str
    token_main_address: str
    token_quote_symbol: str
    token_quote_address: str


@dataclass
class Wallet:
    address: str
    label: str


@dataclass
class Event:
    datetime_utc: str
    ts: int
    wallet_label: str
    wallet_address: str
    tx_hash: str
    event_type: str
    asset: str
    amount: Decimal
    direction: str
    from_addr: str
    to_addr: str
    counterparty: str
    protocol: str = ""
    link: str = ""
    side: str = ""
    main_amount: Decimal | None = None
    quote_amount: Decimal | None = None
    price: Decimal | None = None


@dataclass
class WalletStats:
    wallet_label: str
    wallet_address: str
    period_start_utc: str
    period_end_utc: str

    total_main_bought: Decimal = Decimal(0)
    total_quote_spent: Decimal = Decimal(0)
    total_main_sold: Decimal = Decimal(0)
    total_quote_received_from_swaps: Decimal = Decimal(0)

    main_received_transfer: Decimal = Decimal(0)
    main_sent_transfer: Decimal = Decimal(0)
    quote_received_transfer: Decimal = Decimal(0)
    quote_sent_transfer: Decimal = Decimal(0)

    raw_events: list[Event] = field(default_factory=list)
    transfer_events: list[Event] = field(default_factory=list)
    swap_events: list[Event] = field(default_factory=list)

    @property
    def avg_buy_price(self) -> Decimal:
        return self.total_quote_spent / self.total_main_bought if self.total_main_bought else Decimal(0)

    @property
    def avg_sell_price(self) -> Decimal:
        return self.total_quote_received_from_swaps / self.total_main_sold if self.total_main_sold else Decimal(0)

    @property
    def matched_main(self) -> Decimal:
        return min(self.total_main_bought, self.total_main_sold)

    @property
    def realized_pnl_quote(self) -> Decimal:
        return self.matched_main * (self.avg_sell_price - self.avg_buy_price)

    @property
    def net_main_transfer(self) -> Decimal:
        return self.main_received_transfer - self.main_sent_transfer

    @property
    def net_quote_transfer(self) -> Decimal:
        return self.quote_received_transfer - self.quote_sent_transfer


# =========================
# BASIC HELPERS
# =========================

def norm_addr(addr: str) -> str:
    return str(addr or "").strip().lower()


def parse_utc_to_ts(value: str) -> int:
    value = str(value).strip()
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")

    for fmt in formats:
        try:
            return int(datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            pass

    raise ValueError(f"Cannot parse UTC datetime: {value!r}. Use YYYY-MM-DD HH:MM:SS")


def ts_to_utc_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def parse_iso_to_ts(iso_str: str) -> int:
    if not iso_str:
        return 0

    s = str(iso_str).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        return int(datetime.fromisoformat(s).timestamp())
    except ValueError:
        return 0


def dec_to_str(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, Decimal):
        s = format(value, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s and s != "-0" else "0"

    return str(value)


def hex_to_int(value: str | int | None) -> int:
    if value is None or value == "":
        return 0

    if isinstance(value, int):
        return value

    s = str(value).strip()
    if s.startswith("0x") or s.startswith("0X"):
        return int(s, 16)

    return int(s)


def int_to_hex_block(value: int) -> str:
    return hex(int(value))


def amount_from_raw_hex(raw_hex: str, decimals_hex: str) -> Decimal:
    raw_int = hex_to_int(raw_hex)
    dec_int = hex_to_int(decimals_hex) if decimals_hex else 0
    return Decimal(raw_int) / (Decimal(10) ** dec_int)


def direction_for_wallet(wallet: str, from_addr: str, to_addr: str) -> str:
    wallet = norm_addr(wallet)

    if norm_addr(to_addr) == wallet:
        return "IN"

    if norm_addr(from_addr) == wallet:
        return "OUT"

    return "OTHER"


# =========================
# ALCHEMY CLIENT
# =========================

class RateLimiter:
    def __init__(self, rps: int):
        self.min_interval = 1.0 / max(int(rps), 1)
        self.last = 0.0

    def wait(self):
        now = time.monotonic()
        delta = now - self.last

        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)

        self.last = time.monotonic()


_limiter = RateLimiter(RATE_LIMIT_RPS)
_request_id = 0


def _next_id() -> int:
    global _request_id
    _request_id += 1
    return _request_id


def alchemy_rpc(method: str, params: list[Any], max_retries: int = 5) -> Any:
    if not ALCHEMY_API_KEY:
        raise RuntimeError("BASESCAN_API_KEY is missing. Put your Alchemy key into this secret.")

    url = f"{ALCHEMY_BASE_URL.rstrip('/')}/{ALCHEMY_API_KEY}"
    backoff = 1.0

    for attempt in range(max_retries):
        payload = {
            "jsonrpc": "2.0",
            "id": _next_id(),
            "method": method,
            "params": params,
        }

        _limiter.wait()

        try:
            response = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=60,
            )
        except requests.RequestException as e:
            log.warning("Network error: %s | attempt=%s", e, attempt + 1)
            time.sleep(backoff)
            backoff *= 2
            continue

        if response.status_code == 429:
            log.warning("HTTP 429 rate limit, sleeping %.1fs", backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        if response.status_code >= 500:
            log.warning("HTTP %s server error, sleeping %.1fs", response.status_code, backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        response.raise_for_status()

        try:
            data = response.json()
        except ValueError as e:
            log.warning("Invalid JSON response: %s, sleeping %.1fs", e, backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        if data.get("error"):
            err = data["error"]
            err_msg = str(err.get("message", "")).lower()
            err_code = err.get("code")

            if "rate limit" in err_msg or "too many" in err_msg or err_code == 429:
                log.warning("RPC rate limit: %s, sleeping %.1fs", err, backoff)
                time.sleep(backoff)
                backoff *= 2
                continue

            raise RuntimeError(f"Alchemy RPC error: {err}")

        return data.get("result")

    raise RuntimeError(f"Alchemy request failed after {max_retries} retries: method={method}")


# =========================
# DATE → BLOCK RANGE
# =========================

def get_latest_block_number() -> int:
    result = alchemy_rpc("eth_blockNumber", [])
    return hex_to_int(result)


def get_block_timestamp(block_number: int) -> int:
    result = alchemy_rpc("eth_getBlockByNumber", [int_to_hex_block(block_number), False])

    if not result:
        return 0

    return hex_to_int(result.get("timestamp", "0x0"))


def find_block_by_timestamp(target_ts: int, closest: str = "before") -> int:
    if closest not in {"before", "after"}:
        raise ValueError("closest must be 'before' or 'after'")

    low = 0
    high = get_latest_block_number()

    candidate_after = high
    candidate_before = 0

    while low <= high:
        mid = (low + high) // 2
        mid_ts = get_block_timestamp(mid)

        if mid_ts == 0:
            high = mid - 1
            continue

        if mid_ts < target_ts:
            candidate_before = mid
            low = mid + 1
        elif mid_ts > target_ts:
            candidate_after = mid
            high = mid - 1
        else:
            return mid

    return candidate_after if closest == "after" else candidate_before


# =========================
# ALCHEMY TRANSFERS
# =========================

def get_asset_transfers_page(
    *,
    from_address: str | None = None,
    to_address: str | None = None,
    contract_addresses: list[str],
    from_block: str,
    to_block: str,
    page_key: str | None = None,
    max_count: str = "0x3e8",
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "category": ["erc20"],
        "contractAddresses": contract_addresses,
        "withMetadata": True,
        "excludeZeroValue": True,
        "maxCount": max_count,
        "order": "asc",
    }

    if from_address:
        params["fromAddress"] = from_address

    if to_address:
        params["toAddress"] = to_address

    if page_key:
        params["pageKey"] = page_key

    return alchemy_rpc("alchemy_getAssetTransfers", [params]) or {}


def fetch_all_transfers_one_direction(
    *,
    wallet: str,
    direction: str,
    contract_addresses: list[str],
    from_block: str,
    to_block: str,
) -> list[dict]:
    if direction not in {"from", "to"}:
        raise ValueError("direction must be 'from' or 'to'")

    rows: list[dict] = []
    page_key: str | None = None
    safety_pages = 0

    while True:
        kwargs: dict[str, Any] = {
            "contract_addresses": contract_addresses,
            "from_block": from_block,
            "to_block": to_block,
            "page_key": page_key,
        }

        if direction == "from":
            kwargs["from_address"] = wallet
        else:
            kwargs["to_address"] = wallet

        result = get_asset_transfers_page(**kwargs)
        page_rows = result.get("transfers") or []
        rows.extend(page_rows)

        page_key = result.get("pageKey") or None
        safety_pages += 1

        if not page_key:
            break

        if safety_pages > 200:
            raise RuntimeError(
                f"Pagination exceeded 200 pages for wallet={wallet}, direction={direction}"
            )

    return rows


def fetch_wallet_transfers(
    wallet: str,
    contract_addresses: list[str],
    from_block: str,
    to_block: str,
) -> list[dict]:
    out_rows = fetch_all_transfers_one_direction(
        wallet=wallet,
        direction="from",
        contract_addresses=contract_addresses,
        from_block=from_block,
        to_block=to_block,
    )

    in_rows = fetch_all_transfers_one_direction(
        wallet=wallet,
        direction="to",
        contract_addresses=contract_addresses,
        from_block=from_block,
        to_block=to_block,
    )

    seen: set[str] = set()
    merged: list[dict] = []

    for row in out_rows + in_rows:
        unique_id = row.get("uniqueId") or (
            f"{row.get('hash')}:{row.get('from')}:{row.get('to')}:"
            f"{row.get('rawContract', {}).get('address')}:"
            f"{row.get('rawContract', {}).get('value')}"
        )

        if unique_id in seen:
            continue

        seen.add(unique_id)
        merged.append(row)

    return merged


# =========================
# GOOGLE SHEETS I/O
# =========================

def gspread_client():
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("GOOGLE_SHEET_ID / GSHEET_ID is missing")

    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is missing")

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_sheet():
    return gspread_client().open_by_key(GOOGLE_SHEET_ID)


def ensure_ws(ss, title: str, rows: int = 1000, cols: int = 30):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def clear_and_write(ws, rows: list[list[Any]]):
    ws.clear()

    if rows:
        ws.update(values=rows, range_name="A1", value_input_option="USER_ENTERED")


def read_config(ss) -> ParserConfig:
    ws = ss.worksheet("Config")
    values = ws.get_all_values()

    if not values:
        raise RuntimeError("Config sheet is empty")

    kv: dict[str, str] = {}

    if len(values[0]) >= 2 and values[0][0].strip().lower() == "key":
        for row in values[1:]:
            if len(row) >= 2 and row[0].strip():
                kv[row[0].strip().lower()] = row[1].strip()

    elif len(values[0]) >= 2 and values[0][0].strip().lower() == "start_utc":
        headers = [h.strip().lower() for h in values[0]]
        first = values[1] if len(values) > 1 else []
        kv = {
            headers[i]: first[i].strip() if i < len(first) else ""
            for i in range(len(headers))
        }

    else:
        raise RuntimeError("Unsupported Config format. Use columns: key | value")

    start_utc = kv.get("start_utc", "")
    end_utc = kv.get("end_utc", "")

    if not start_utc or not end_utc:
        raise RuntimeError("Config must contain start_utc and end_utc")

    chain = kv.get("chain", "Base") or "Base"

    token_main_symbol = (
        kv.get("token_main_symbol")
        or kv.get("token_main")
        or "LMTS"
    ).upper()

    token_quote_symbol = (
        kv.get("token_quote_symbol")
        or kv.get("token_quote")
        or "USDC"
    ).upper()

    token_main_address = norm_addr(
        kv.get("token_main_contract")
        or kv.get("token_main_address")
        or os.getenv("LMTS_ADDRESS", DEFAULT_LMTS_ADDRESS)
    )

    token_quote_address = norm_addr(
        kv.get("token_quote_contract")
        or kv.get("token_quote_address")
        or os.getenv("USDC_ADDRESS", DEFAULT_USDC_ADDRESS)
    )

    return ParserConfig(
        start_utc=start_utc,
        end_utc=end_utc,
        start_ts=parse_utc_to_ts(start_utc),
        end_ts=parse_utc_to_ts(end_utc),
        chain=chain,
        token_main_symbol=token_main_symbol,
        token_main_address=token_main_address,
        token_quote_symbol=token_quote_symbol,
        token_quote_address=token_quote_address,
    )


def read_wallets(ss) -> list[Wallet]:
    ws = ss.worksheet("Wallets")
    records = ws.get_all_records()

    wallets: list[Wallet] = []

    for row in records:
        address = norm_addr(row.get("wallet_address", ""))
        label = str(row.get("label", "")).strip()

        if address:
            wallets.append(Wallet(address=address, label=label or address[:10]))

    if not wallets:
        raise RuntimeError("Wallets sheet has no wallet_address rows")

    return wallets


# =========================
# NORMALIZATION + CLASSIFICATION
# =========================

def normalize_alchemy_row(
    row: dict[str, Any],
    wallet: Wallet,
    cfg: ParserConfig,
) -> Event | None:
    raw_contract = row.get("rawContract") or {}
    contract = norm_addr(raw_contract.get("address", ""))

    if contract not in {cfg.token_main_address, cfg.token_quote_address}:
        return None

    metadata = row.get("metadata") or {}
    block_ts_iso = metadata.get("blockTimestamp", "")
    ts = parse_iso_to_ts(block_ts_iso)

    if ts == 0:
        return None

    if ts < cfg.start_ts or ts > cfg.end_ts:
        return None

    from_addr = norm_addr(row.get("from", ""))
    to_addr = norm_addr(row.get("to", ""))

    direction = direction_for_wallet(wallet.address, from_addr, to_addr)

    if direction == "OTHER":
        return None

    symbol = str(row.get("asset") or "").upper()

    if not symbol:
        if contract == cfg.token_main_address:
            symbol = cfg.token_main_symbol
        elif contract == cfg.token_quote_address:
            symbol = cfg.token_quote_symbol

    amount = amount_from_raw_hex(
        raw_contract.get("value", "0x0"),
        raw_contract.get("decimal", "0x0"),
    )

    counterparty = from_addr if direction == "IN" else to_addr
    tx_hash = str(row.get("hash", "")).lower()

    event = Event(
        datetime_utc=ts_to_utc_str(ts),
        ts=ts,
        wallet_label=wallet.label,
        wallet_address=wallet.address,
        tx_hash=tx_hash,
        event_type="RAW",
        asset=symbol,
        amount=amount,
        direction=direction,
        from_addr=from_addr,
        to_addr=to_addr,
        counterparty=counterparty,
        protocol="",
        link=f"{BASESCAN_TX_URL}{tx_hash}",
    )

    event.contract_address = contract
    return event


def filter_and_normalize_rows(
    raw_rows: Iterable[dict],
    wallet: Wallet,
    cfg: ParserConfig,
) -> list[Event]:
    events: list[Event] = []

    for row in raw_rows:
        event = normalize_alchemy_row(row, wallet, cfg)

        if event is not None:
            events.append(event)

    events.sort(key=lambda e: (e.ts, e.tx_hash, e.direction, e.asset))
    return events


def classify_wallet_events(
    wallet: Wallet,
    events: list[Event],
    cfg: ParserConfig,
) -> WalletStats:
    stats = WalletStats(
        wallet_label=wallet.label,
        wallet_address=wallet.address,
        period_start_utc=cfg.start_utc,
        period_end_utc=cfg.end_utc,
    )

    by_tx: dict[str, list[Event]] = defaultdict(list)

    for event in events:
        by_tx[event.tx_hash].append(event)

    sorted_txs = sorted(
        by_tx.items(),
        key=lambda item: min(event.ts for event in item[1]),
    )

    for tx_hash, legs in sorted_txs:
        main_in = Decimal(0)
        main_out = Decimal(0)
        quote_in = Decimal(0)
        quote_out = Decimal(0)

        for event in legs:
            contract = getattr(event, "contract_address", "")

            if contract == cfg.token_main_address:
                if event.direction == "IN":
                    main_in += event.amount
                elif event.direction == "OUT":
                    main_out += event.amount

            elif contract == cfg.token_quote_address:
                if event.direction == "IN":
                    quote_in += event.amount
                elif event.direction == "OUT":
                    quote_out += event.amount

        is_buy = main_in > 0 and quote_out > 0
        is_sell = main_out > 0 and quote_in > 0

        if is_buy or is_sell:
            first = min(legs, key=lambda event: event.ts)

            if is_buy:
                side = "BUY"
                main_amount = main_in
                quote_amount = quote_out
                price = quote_amount / main_amount if main_amount else Decimal(0)

                stats.total_main_bought += main_amount
                stats.total_quote_spent += quote_amount

            else:
                side = "SELL"
                main_amount = main_out
                quote_amount = quote_in
                price = quote_amount / main_amount if main_amount else Decimal(0)

                stats.total_main_sold += main_amount
                stats.total_quote_received_from_swaps += quote_amount

            for event in legs:
                event.event_type = f"SWAP_{side}"
                stats.raw_events.append(event)

            stats.swap_events.append(Event(
                datetime_utc=first.datetime_utc,
                ts=first.ts,
                wallet_label=wallet.label,
                wallet_address=wallet.address,
                tx_hash=tx_hash,
                event_type=f"SWAP_{side}",
                asset=f"{cfg.token_main_symbol}/{cfg.token_quote_symbol}",
                amount=main_amount,
                direction="",
                from_addr="",
                to_addr="",
                counterparty="",
                protocol="",
                link=first.link,
                side=side,
                main_amount=main_amount,
                quote_amount=quote_amount,
                price=price,
            ))

        else:
            for event in legs:
                event.event_type = "TRANSFER"
                stats.raw_events.append(event)
                stats.transfer_events.append(event)

                contract = getattr(event, "contract_address", "")

                if contract == cfg.token_main_address:
                    if event.direction == "IN":
                        stats.main_received_transfer += event.amount
                    elif event.direction == "OUT":
                        stats.main_sent_transfer += event.amount

                elif contract == cfg.token_quote_address:
                    if event.direction == "IN":
                        stats.quote_received_transfer += event.amount
                    elif event.direction == "OUT":
                        stats.quote_sent_transfer += event.amount

    return stats


# =========================
# SHEET WRITING
# =========================

def replace_sheet(ss, title: str, header: list[str], data_rows: list[list[Any]]):
    ws = ensure_ws(
        ss,
        title,
        rows=max(len(data_rows) + 10, 100),
        cols=max(len(header) + 2, 20),
    )

    matrix = [header] + data_rows
    clear_and_write(ws, matrix)

    log.info("Wrote %s rows to %s", len(data_rows), title)


def write_outputs(ss, stats_list: list[WalletStats], cfg: ParserConfig):
    all_raw: list[Event] = []
    all_transfers: list[Event] = []
    all_swaps: list[Event] = []

    for stats in stats_list:
        all_raw.extend(stats.raw_events)
        all_transfers.extend(stats.transfer_events)
        all_swaps.extend(stats.swap_events)

    all_raw.sort(key=lambda e: (e.ts, e.wallet_label, e.tx_hash))
    all_transfers.sort(key=lambda e: (e.ts, e.wallet_label, e.tx_hash))
    all_swaps.sort(key=lambda e: (e.ts, e.wallet_label, e.tx_hash))

    raw_header = [
        "datetime_utc", "wallet_label", "wallet_address", "tx_hash",
        "event_type", "asset", "amount", "direction", "from", "to",
        "counterparty", "protocol", "link",
    ]

    raw_rows = [
        [
            event.datetime_utc, event.wallet_label, event.wallet_address, event.tx_hash,
            event.event_type, event.asset, dec_to_str(event.amount), event.direction,
            event.from_addr, event.to_addr, event.counterparty, event.protocol, event.link,
        ]
        for event in all_raw
    ]

    replace_sheet(ss, "Raw_Transactions", raw_header, raw_rows)

    transfer_header = [
        "datetime_utc", "wallet_label", "wallet_address", "tx_hash",
        "asset", "direction", "amount", "from", "to", "counterparty", "link",
    ]

    transfer_rows = [
        [
            event.datetime_utc, event.wallet_label, event.wallet_address, event.tx_hash,
            event.asset, event.direction, dec_to_str(event.amount), event.from_addr,
            event.to_addr, event.counterparty, event.link,
        ]
        for event in all_transfers
    ]

    replace_sheet(ss, "Transfers", transfer_header, transfer_rows)

    swap_header = [
        "datetime_utc", "wallet_label", "wallet_address", "tx_hash", "side",
        f"{cfg.token_main_symbol.lower()}_amount",
        f"{cfg.token_quote_symbol.lower()}_amount",
        "price", "protocol", "link",
    ]

    swap_rows = [
        [
            event.datetime_utc, event.wallet_label, event.wallet_address, event.tx_hash,
            event.side, dec_to_str(event.main_amount), dec_to_str(event.quote_amount),
            dec_to_str(event.price), event.protocol, event.link,
        ]
        for event in all_swaps
    ]

    replace_sheet(ss, "Swaps", swap_header, swap_rows)

    final_header = [
        "wallet_label", "wallet_address", "period_start_utc", "period_end_utc",
        f"total_{cfg.token_main_symbol.lower()}_bought",
        f"total_{cfg.token_quote_symbol.lower()}_spent",
        "avg_buy_price",
        f"total_{cfg.token_main_symbol.lower()}_sold",
        f"total_{cfg.token_quote_symbol.lower()}_received",
        "avg_sell_price",
        f"matched_{cfg.token_main_symbol.lower()}",
        f"realized_pnl_{cfg.token_quote_symbol.lower()}",
        f"{cfg.token_main_symbol.lower()}_received_transfer",
        f"{cfg.token_main_symbol.lower()}_sent_transfer",
        f"net_{cfg.token_main_symbol.lower()}_transfer",
        f"{cfg.token_quote_symbol.lower()}_received_transfer",
        f"{cfg.token_quote_symbol.lower()}_sent_transfer",
        f"net_{cfg.token_quote_symbol.lower()}_transfer",
    ]

    final_rows = []

    for stats in stats_list:
        final_rows.append([
            stats.wallet_label, stats.wallet_address, stats.period_start_utc, stats.period_end_utc,
            dec_to_str(stats.total_main_bought),
            dec_to_str(stats.total_quote_spent),
            dec_to_str(stats.avg_buy_price),
            dec_to_str(stats.total_main_sold),
            dec_to_str(stats.total_quote_received_from_swaps),
            dec_to_str(stats.avg_sell_price),
            dec_to_str(stats.matched_main),
            dec_to_str(stats.realized_pnl_quote),
            dec_to_str(stats.main_received_transfer),
            dec_to_str(stats.main_sent_transfer),
            dec_to_str(stats.net_main_transfer),
            dec_to_str(stats.quote_received_transfer),
            dec_to_str(stats.quote_sent_transfer),
            dec_to_str(stats.net_quote_transfer),
        ])

    replace_sheet(ss, "Final", final_header, final_rows)


# =========================
# MAIN
# =========================

def main():
    ss = open_sheet()

    cfg = read_config(ss)
    wallets = read_wallets(ss)

    log.info("Loaded period: %s → %s UTC", cfg.start_utc, cfg.end_utc)
    log.info("Loaded wallets: %s", len(wallets))
    log.info(
        "Tracked tokens: %s=%s | %s=%s",
        cfg.token_main_symbol, cfg.token_main_address,
        cfg.token_quote_symbol, cfg.token_quote_address,
    )

    if cfg.end_ts < cfg.start_ts:
        raise RuntimeError("end_utc must be greater than or equal to start_utc")

    log.info("Finding block range for Config period...")
    start_block = find_block_by_timestamp(cfg.start_ts, closest="after")
    end_block = find_block_by_timestamp(cfg.end_ts, closest="before")

    if start_block > end_block:
        raise RuntimeError(
            f"Invalid block range from period: start_block={start_block}, end_block={end_block}"
        )

    from_block = int_to_hex_block(start_block)
    to_block = int_to_hex_block(end_block)

    log.info(
        "Block range from Config period: %s (%s) → %s (%s)",
        start_block, from_block, end_block, to_block,
    )

    contract_addresses = [cfg.token_main_address, cfg.token_quote_address]
    stats_list: list[WalletStats] = []

    for wallet in wallets:
        log.info("Fetching wallet=%s label=%s", wallet.address, wallet.label)

        raw_rows = fetch_wallet_transfers(
            wallet.address,
            contract_addresses,
            from_block=from_block,
            to_block=to_block,
        )

        log.info("  raw transfers fetched: %s", len(raw_rows))

        events = filter_and_normalize_rows(raw_rows, wallet, cfg)
        stats = classify_wallet_events(wallet, events, cfg)
        stats_list.append(stats)

        log.info(
            "%s: in_period_events=%s transfers=%s swaps=%s pnl=%s",
            wallet.label,
            len(events),
            len(stats.transfer_events),
            len(stats.swap_events),
            dec_to_str(stats.realized_pnl_quote),
        )

    write_outputs(ss, stats_list, cfg)

    log.info("Done")


if __name__ == "__main__":
    main()
