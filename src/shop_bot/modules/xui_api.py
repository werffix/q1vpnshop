import asyncio
import hmac
import json
import logging
import os
import re
import secrets
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from hmac import compare_digest

from shop_bot.data_manager.database import (
    get_all_hosts,
    get_host,
    get_key_by_email,
    get_or_create_user_subscription_token,
    get_setting,
    get_sub_host,
    get_user_device_limit,
    get_user_id_by_subscription_token,
    get_user_keys,
)

logger = logging.getLogger(__name__)

REMNA_TIMEOUT_SECONDS = 8


def _parse_user_id_from_key_email(email: str | None) -> int | None:
    value = (email or "").strip()
    if not value:
        return None
    for pattern in (r"^u(\d+)\.", r"^user(\d+)[\.-]"):
        m = re.match(pattern, value)
        if not m:
            continue
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def _resolve_effective_device_limit(email: str | None, explicit_limit: int | None = None) -> int:
    if explicit_limit is not None:
        try:
            return max(1, int(explicit_limit))
        except Exception:
            pass
    try:
        default_limit = int((get_setting("default_device_limit") or "3").strip() or "3")
    except Exception:
        default_limit = 3
    if default_limit < 1:
        default_limit = 3
    user_id = _parse_user_id_from_key_email(email)
    if user_id is None:
        return default_limit
    try:
        return get_user_device_limit(user_id, default_limit=default_limit)
    except Exception:
        return default_limit


def _traffic_limit_bytes(traffic_cap_gb: float | int | str | None) -> int | None:
    try:
        if traffic_cap_gb in (None, "", "null"):
            return None
        gb = float(traffic_cap_gb)
        if gb <= 0:
            return None
        return int(gb * 1024 * 1024 * 1024)
    except Exception:
        return None


def _is_whitelist_host(host_name: str | None) -> bool:
    name = (host_name or "").lower()
    return ("белые списки" in name) or ("white list" in name) or ("whitelist" in name)


def _resolve_host_client_traffic_limit_gb(host_data: dict | None) -> float | int | str | None:
    if not host_data:
        return None
    explicit = host_data.get("client_monthly_traffic_gb")
    try:
        if explicit not in (None, "", "null") and float(explicit) > 0:
            return explicit
    except Exception:
        pass
    if _is_whitelist_host(host_data.get("host_name")):
        return 200
    return explicit


def resolve_host_client_traffic_limit_gb(host_data: dict | None) -> float | int | str | None:
    return _resolve_host_client_traffic_limit_gb(host_data)


def _subscription_secret() -> bytes:
    raw = (
        os.getenv("SHOPBOT_SUB_SECRET")
        or os.getenv("SHOPBOT_SECRET_KEY")
        or get_setting("telegram_bot_token")
        or "shopbot-sub-secret"
    )
    return str(raw).encode("utf-8")


def build_unified_subscription_token(user_id: int) -> str:
    payload = str(int(user_id))
    signature = hmac.new(_subscription_secret(), payload.encode("utf-8"), "sha256").hexdigest()[:20]
    return f"{payload}.{signature}"


def parse_unified_subscription_token(token: str) -> int | None:
    token = (token or "").strip()
    if "." not in token:
        return None
    payload, signature = token.split(".", 1)
    if not payload.isdigit() or not signature:
        return None
    expected = hmac.new(_subscription_secret(), payload.encode("utf-8"), "sha256").hexdigest()[:20]
    if not compare_digest(signature, expected):
        return None
    return int(payload)


def resolve_user_id_by_persistent_subscription_token(token: str) -> int | None:
    try:
        return get_user_id_by_subscription_token(token)
    except Exception:
        return None


def normalize_xui_host_url(host_url: str) -> str:
    raw = (host_url or "").strip()
    if not raw:
        return raw
    candidate = raw if "://" in raw else f"https://{raw}"
    parsed = urllib.parse.urlparse(candidate)
    if parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return raw.rstrip("/")


def build_xui_host_candidates(host_url: str) -> list[str]:
    raw = (host_url or "").strip().rstrip("/")
    if not raw:
        return []

    candidate = raw if "://" in raw else f"https://{raw}"
    parsed = urllib.parse.urlparse(candidate)
    candidates: list[str] = []

    if parsed.scheme and parsed.netloc:
        full = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        if full:
            candidates.append(full)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base not in candidates:
            candidates.append(base)

    unique: list[str] = []
    for value in candidates:
        if value and value not in unique:
            unique.append(value)
    return unique


def _host_is_remna(host_data: dict | None) -> bool:
    if not host_data:
        return False
    return bool((host_data.get("remna_api_token") or "").strip())


def _safe_json_loads(raw: bytes) -> dict | list | None:
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _remna_headers(host_data: dict) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "shop-bot/remnawave",
        "x-forwarded-for": "127.0.0.1",
        "x-forwarded-proto": "https",
    }
    token = (host_data.get("remna_api_token") or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    caddy_key = (host_data.get("remna_caddy_api_key") or "").strip()
    if caddy_key:
        headers["X-Api-Key"] = caddy_key
    cookie = (host_data.get("remna_cookie") or "").strip()
    if cookie:
        headers["Cookie"] = cookie
    cf_client_id = (host_data.get("remna_cf_client_id") or "").strip()
    if cf_client_id:
        headers["CF-Access-Client-Id"] = cf_client_id
    cf_client_secret = (host_data.get("remna_cf_client_secret") or "").strip()
    if cf_client_secret:
        headers["CF-Access-Client-Secret"] = cf_client_secret
    return headers


def _remna_endpoint_candidates(host_url: str, path: str) -> list[str]:
    norm_path = "/" + str(path or "").lstrip("/")
    urls: list[str] = []
    for base in build_xui_host_candidates(host_url):
        cleaned = str(base or "").rstrip("/")
        if not cleaned:
            continue
        parsed = urllib.parse.urlparse(cleaned)
        prefix = (parsed.path or "").rstrip("/")
        variants = []
        if prefix.endswith("/api"):
            variants.append(f"{parsed.scheme}://{parsed.netloc}{prefix}{norm_path}")
        else:
            if prefix:
                variants.append(f"{parsed.scheme}://{parsed.netloc}{prefix}/api{norm_path}")
                variants.append(f"{parsed.scheme}://{parsed.netloc}{prefix}{norm_path}")
            variants.append(f"{parsed.scheme}://{parsed.netloc}/api{norm_path}")
            variants.append(f"{parsed.scheme}://{parsed.netloc}{norm_path}")
        for item in variants:
            if item not in urls:
                urls.append(item)
    return urls


def _remna_request_json_sync(
    host_data: dict,
    method: str,
    path: str,
    payload: dict | None = None,
    ok_statuses: tuple[int, ...] = (200,),
    allow_statuses: tuple[int, ...] = (),
) -> dict | list | None:
    if not _host_is_remna(host_data):
        return None

    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = _remna_headers(host_data)
    last_error = None

    for url in _remna_endpoint_candidates(host_data.get("host_url") or "", path):
        request = urllib.request.Request(url=url, data=body, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(request, timeout=REMNA_TIMEOUT_SECONDS) as response:
                status = int(getattr(response, "status", 200) or 200)
                data = _safe_json_loads(response.read())
                if status in ok_statuses or status in allow_statuses:
                    return data if data is not None else {}
                last_error = f"HTTP {status}"
        except urllib.error.HTTPError as exc:
            status = int(exc.code or 0)
            if status in allow_statuses:
                return _safe_json_loads(exc.read())
            raw = exc.read()
            error_payload = _safe_json_loads(raw)
            if isinstance(error_payload, dict):
                last_error = f"HTTP {status}: {error_payload}"
            else:
                last_error = f"HTTP {status}"
        except Exception as exc:
            last_error = exc

    logger.warning(
        "Не удалось выполнить запрос к Remnawave '%s %s' для '%s': %s",
        method,
        path,
        host_data.get("host_name"),
        last_error,
    )
    return None


def _extract_response_payload(data):
    if isinstance(data, dict) and "response" in data:
        return data.get("response")
    return data


def _extract_users_list(data) -> list[dict]:
    payload = _extract_response_payload(data)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        users = payload.get("users")
        if isinstance(users, list):
            return [item for item in users if isinstance(item, dict)]
    return []


def _extract_user(data) -> dict | None:
    payload = _extract_response_payload(data)
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
    return None


def _parse_iso_to_ms(value) -> int:
    if value in (None, "", 0):
        return 0
    try:
        if isinstance(value, (int, float)):
            if float(value) > 10_000_000_000:
                return int(value)
            return int(float(value) * 1000)
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _ms_to_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return dt.isoformat()


def _build_fallback_subscription_url(user_id: int, base_domain: str | None = None) -> str | None:
    token = get_or_create_user_subscription_token(user_id)
    domain = (base_domain or "").strip()
    if not domain:
        try:
            sub_host = get_sub_host()
        except Exception:
            sub_host = None
        if sub_host:
            domain = (
                str(sub_host.get("subscription_url") or "").strip()
                or str(sub_host.get("host_url") or "").strip()
            )
    if not domain:
        domain = (get_setting("domain") or "").strip()
    if not domain:
        return None

    candidate = domain if "://" in domain else f"https://{domain}"
    if "{token}" in candidate:
        return candidate.replace("{token}", token)

    parsed = urllib.parse.urlparse(candidate)
    if not parsed.netloc:
        return None
    base = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    path = (parsed.path or "").rstrip("/")
    if path:
        return f"{base}{path}/{token}"
    return f"{base}/sub/{token}"


def _resolve_primary_host() -> dict | None:
    try:
        host = get_sub_host()
        if host and _host_is_remna(host):
            return host
    except Exception:
        pass
    try:
        for item in (get_all_hosts() or []):
            if _host_is_remna(item):
                return item
    except Exception:
        pass
    return None


def _build_remna_username(user_id: int | None, email: str | None) -> str:
    if user_id is not None:
        return f"tg_{int(user_id)}"
    raw = re.sub(r"[^a-zA-Z0-9_-]+", "_", (email or "user").split("@", 1)[0]).strip("_")
    if len(raw) < 3:
        raw = f"user_{uuid.uuid4().hex[:8]}"
    return raw[:36]


def _lookup_user_id_from_context(email: str | None) -> int | None:
    user_id = _parse_user_id_from_key_email(email)
    if user_id is not None:
        return user_id
    key = get_key_by_email((email or "").strip())
    if key:
        try:
            return int(key.get("user_id"))
        except Exception:
            return None
    return None


def _get_remna_user_by_telegram_id(host_data: dict, user_id: int) -> dict | None:
    data = _remna_request_json_sync(
        host_data,
        "GET",
        f"/users/by-telegram-id/{int(user_id)}",
        ok_statuses=(200,),
        allow_statuses=(404,),
    )
    users = _extract_users_list(data)
    return users[0] if users else None


def _get_remna_user_by_username(host_data: dict, username: str) -> dict | None:
    data = _remna_request_json_sync(
        host_data,
        "GET",
        f"/users/by-username/{urllib.parse.quote(username, safe='')}",
        ok_statuses=(200,),
        allow_statuses=(404,),
    )
    return _extract_user(data)


def _get_remna_user_sync(host_data: dict, user_id: int | None, username: str) -> dict | None:
    if user_id is not None:
        user = _get_remna_user_by_telegram_id(host_data, user_id)
        if user:
            return user
    return _get_remna_user_by_username(host_data, username)


def _override_subscription_url(host_data: dict, user: dict | None, default_url: str | None) -> str | None:
    default_value = (default_url or "").strip() or None
    base = str((host_data or {}).get("subscription_url") or "").strip()
    if not base:
        return default_value

    short_uuid = str((user or {}).get("shortUuid") or "").strip()
    user_uuid = str((user or {}).get("uuid") or "").strip()
    replacements = {
        "{token}": short_uuid,
        "{short_uuid}": short_uuid,
        "{shortUuid}": short_uuid,
        "{uuid}": user_uuid,
        "{user_uuid}": user_uuid,
    }
    result = base
    for key, value in replacements.items():
        if key in result and value:
            result = result.replace(key, value)
    return result or default_value


def build_unified_subscription_url(user_id: int, base_domain: str | None = None) -> str | None:
    host = _resolve_primary_host()
    if host:
        username = _build_remna_username(int(user_id), None)
        user = _get_remna_user_sync(host, int(user_id), username)
        if user:
            return _override_subscription_url(host, user, user.get("subscriptionUrl"))
    return _build_fallback_subscription_url(user_id, base_domain=base_domain)


def resolve_user_id_by_legacy_sub_token(token: str, all_keys: list[dict]) -> int | None:
    token = (token or "").strip()
    if not token:
        return None
    for key in (all_keys or []):
        uid = _lookup_user_id_from_context(key.get("key_email"))
        if uid is None:
            continue
        url = build_unified_subscription_url(uid)
        if not url:
            continue
        if token == url.rstrip("/").split("/")[-1]:
            return uid
    return None


def login_to_host(host_url: str, username: str, password: str, inbound_id: int):
    logger.info("login_to_host больше не используется для Remnawave: %s", host_url)
    return None, None


def get_connection_string(inbound, user_uuid: str, host_url: str, remark: str) -> str | None:
    return None


def get_subscription_link(user_uuid: str, host_url: str, host_name: str | None = None, sub_token: str | None = None) -> str:
    if host_name:
        host = get_host(host_name)
        if host:
            override = _override_subscription_url(host, {"shortUuid": sub_token, "uuid": user_uuid}, None)
            if override:
                return override
    return host_url


def _determine_target_expiry_ms(existing_user: dict | None, days_to_add: int | None, target_expiry_ms: int | None) -> int:
    try:
        safe_target_expiry_ms = int(target_expiry_ms or 0)
    except Exception:
        safe_target_expiry_ms = 0
    if safe_target_expiry_ms > 0:
        return safe_target_expiry_ms

    existing_expiry_ms = _parse_iso_to_ms((existing_user or {}).get("expireAt"))
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    if days_to_add is None:
        return existing_expiry_ms if existing_expiry_ms > 0 else now_ms

    base_ms = existing_expiry_ms if existing_expiry_ms > now_ms else now_ms
    return base_ms + int(days_to_add) * 24 * 3600 * 1000


def _create_or_update_key_on_host_sync(
    host_name: str,
    email: str,
    days_to_add: int | None = None,
    expiry_timestamp_ms: int | None = None,
    preferred_uuid: str | None = None,
    device_limit: int | None = None,
    rotate_sub_token: bool = False,
) -> dict | None:
    host_data = get_host(host_name)
    if not host_data:
        logger.error("Сбой рабочего процесса: запись Remna '%s' не найдена.", host_name)
        return None
    if not _host_is_remna(host_data):
        logger.error("Для записи '%s' не настроен API token Remna.", host_name)
        return None

    user_id = _lookup_user_id_from_context(email)
    username = _build_remna_username(user_id, email)
    existing_user = _get_remna_user_sync(host_data, user_id, username)
    existing_expiry_ms = _parse_iso_to_ms((existing_user or {}).get("expireAt"))
    traffic_bytes = _traffic_limit_bytes(_resolve_host_client_traffic_limit_gb(host_data))
    target_expiry_ms = _determine_target_expiry_ms(existing_user, days_to_add, expiry_timestamp_ms)

    body = {
        "username": username,
        "expireAt": _ms_to_iso(target_expiry_ms),
        "trafficLimitStrategy": "MONTH" if traffic_bytes is not None else "NO_RESET",
        "telegramId": user_id,
        "email": email if "@" in str(email or "") else None,
        "description": f"ShopBot subscription for {host_name}",
    }
    if traffic_bytes is not None:
        body["trafficLimitBytes"] = int(traffic_bytes)
    if device_limit is not None:
        try:
            body["hwidDeviceLimit"] = max(0, int(device_limit))
        except Exception:
            pass

    if existing_user:
        body["uuid"] = existing_user.get("uuid")
        result = _remna_request_json_sync(host_data, "PATCH", "/users", payload=body, ok_statuses=(200,))
    else:
        body["status"] = "ACTIVE"
        if preferred_uuid:
            try:
                uuid.UUID(str(preferred_uuid))
                body["vlessUuid"] = str(preferred_uuid)
            except Exception:
                pass
        result = _remna_request_json_sync(host_data, "POST", "/users", payload=body, ok_statuses=(200, 201))

    user = _extract_user(result)
    if not user:
        logger.error("Не удалось создать/обновить пользователя Remna '%s'.", email)
        return None

    user_uuid = str(user.get("uuid") or "").strip()
    should_reset_traffic = bool(
        existing_user
        and traffic_bytes is not None
        and target_expiry_ms > max(existing_expiry_ms, 0) + 1000
    )
    if rotate_sub_token and user_uuid:
        revoke_body = {"shortUuid": secrets.token_urlsafe(12).replace("-", "A").replace("_", "B")[:20]}
        rotated = _remna_request_json_sync(
            host_data,
            "POST",
            f"/users/{urllib.parse.quote(user_uuid, safe='')}/actions/revoke",
            payload=revoke_body,
            ok_statuses=(200,),
        )
        rotated_user = _extract_user(rotated)
        if rotated_user:
            user = rotated_user
    elif user_uuid:
        enabled = _remna_request_json_sync(
            host_data,
            "POST",
            f"/users/{urllib.parse.quote(user_uuid, safe='')}/actions/enable",
            ok_statuses=(200,),
            allow_statuses=(400, 409),
        )
        enabled_user = _extract_user(enabled)
        if enabled_user:
            user = enabled_user

    if user_uuid and should_reset_traffic:
        reset_result = _remna_request_json_sync(
            host_data,
            "POST",
            f"/users/{urllib.parse.quote(user_uuid, safe='')}/actions/reset-traffic",
            ok_statuses=(200,),
            allow_statuses=(400, 409),
        )
        reset_user = _extract_user(reset_result)
        if reset_user:
            user = reset_user

    connection_string = _override_subscription_url(host_data, user, user.get("subscriptionUrl"))
    return {
        "client_uuid": str(user.get("vlessUuid") or user.get("uuid") or preferred_uuid or ""),
        "email": email,
        "expiry_timestamp_ms": _parse_iso_to_ms(user.get("expireAt")) or target_expiry_ms,
        "connection_string": connection_string,
        "host_name": host_name,
    }


async def create_or_update_key_on_host(
    host_name: str,
    email: str,
    days_to_add: int | None = None,
    expiry_timestamp_ms: int | None = None,
    preferred_uuid: str | None = None,
    device_limit: int | None = None,
    rotate_sub_token: bool = False,
):
    return await asyncio.to_thread(
        _create_or_update_key_on_host_sync,
        host_name,
        email,
        days_to_add,
        expiry_timestamp_ms,
        preferred_uuid,
        device_limit,
        rotate_sub_token,
    )


def _get_key_owner_user_id(key_data: dict) -> int | None:
    try:
        if key_data.get("user_id") is not None:
            return int(key_data.get("user_id"))
    except Exception:
        pass
    return _lookup_user_id_from_context(key_data.get("key_email"))


def _get_remna_user_for_key_sync(key_data: dict) -> tuple[dict | None, dict | None]:
    host_name = key_data.get("host_name")
    host_data = get_host(host_name) if host_name else None
    if not host_data or not _host_is_remna(host_data):
        return None, host_data
    user_id = _get_key_owner_user_id(key_data)
    username = _build_remna_username(user_id, key_data.get("key_email"))
    user = _get_remna_user_sync(host_data, user_id, username)
    return user, host_data


async def get_key_details_from_host(key_data: dict) -> dict | None:
    user, host_data = await asyncio.to_thread(_get_remna_user_for_key_sync, key_data)
    if not user or not host_data:
        return None
    connection_string = _override_subscription_url(host_data, user, user.get("subscriptionUrl"))
    return {
        "connection_string": connection_string,
        "native_connection_string": connection_string,
    }


async def get_key_usage_stats_from_host(key_data: dict) -> dict:
    user, _host_data = await asyncio.to_thread(_get_remna_user_for_key_sync, key_data)
    if not user:
        return {}
    traffic = user.get("userTraffic") or {}
    try:
        used = max(int(traffic.get("usedTrafficBytes") or 0), 0)
    except Exception:
        used = 0
    try:
        total = max(int(user.get("trafficLimitBytes") or 0), 0)
    except Exception:
        total = 0
    return {
        "upload_bytes": 0,
        "download_bytes": used,
        "total_bytes": total,
        "expiry_timestamp_ms": _parse_iso_to_ms(user.get("expireAt")),
    }


async def get_client(server_id: str) -> dict | None:
    sid = str(server_id or "").strip()
    if not sid:
        return _resolve_primary_host()
    host = get_host(sid)
    if host:
        return host
    for item in (get_all_hosts() or []):
        if str(item.get("host_name") or "").strip() == sid:
            return item
        if str(item.get("host_url") or "").strip() == sid:
            return item
    return _resolve_primary_host()


def _get_client_stats_sync(client_host: dict, panel_email: str) -> dict | None:
    if not client_host:
        return None
    user_id = _lookup_user_id_from_context(panel_email)
    username = _build_remna_username(user_id, panel_email)
    user = _get_remna_user_sync(client_host, user_id, username)
    if not user:
        return None
    traffic = user.get("userTraffic") or {}
    try:
        used = max(int(traffic.get("usedTrafficBytes") or 0), 0)
    except Exception:
        used = 0
    try:
        total = max(int(user.get("trafficLimitBytes") or 0), 0)
    except Exception:
        total = 0
    return {"up": 0, "down": used, "total": total}


async def get_client_stats(client_host: dict, panel_email: str) -> dict | None:
    return await asyncio.to_thread(_get_client_stats_sync, client_host, panel_email)


async def build_vless_uri_for_key(key_data: dict) -> str | None:
    details = await get_key_details_from_host(key_data)
    if not details:
        return None
    return details.get("connection_string")


def _has_other_active_keys(user_id: int, skip_email: str | None = None) -> bool:
    now = datetime.now()
    for key in (get_user_keys(int(user_id)) or []):
        if skip_email and str(key.get("key_email") or "").strip() == str(skip_email).strip():
            continue
        expiry_raw = key.get("expiry_date")
        if not expiry_raw:
            continue
        try:
            if datetime.fromisoformat(str(expiry_raw)) > now:
                return True
        except Exception:
            continue
    return False


def _delete_client_on_host_sync(host_name: str, client_email: str) -> bool:
    host_data = get_host(host_name)
    if not host_data or not _host_is_remna(host_data):
        return False

    user_id = _lookup_user_id_from_context(client_email)
    username = _build_remna_username(user_id, client_email)
    user = _get_remna_user_sync(host_data, user_id, username)
    if not user:
        return True

    user_uuid = str(user.get("uuid") or "").strip()
    if not user_uuid:
        return True

    if user_id is not None and _has_other_active_keys(user_id, skip_email=client_email):
        logger.info(
            "Пропускаю disable/delete Remna для '%s': у пользователя %s есть другие активные ключи.",
            client_email,
            user_id,
        )
        return True

    delete_result = _remna_request_json_sync(
        host_data,
        "DELETE",
        f"/users/{urllib.parse.quote(user_uuid, safe='')}",
        ok_statuses=(200, 204),
        allow_statuses=(404,),
    )
    if delete_result is not None:
        return True

    disable_result = _remna_request_json_sync(
        host_data,
        "POST",
        f"/users/{urllib.parse.quote(user_uuid, safe='')}/actions/disable",
        ok_statuses=(200,),
        allow_statuses=(400, 409),
    )
    return disable_result is not None


async def delete_client_on_host(host_name: str, client_email: str) -> bool:
    return await asyncio.to_thread(_delete_client_on_host_sync, host_name, client_email)


def _set_client_enabled_sync(host_name: str, client_email: str, enabled: bool) -> bool:
    host_data = get_host(host_name)
    if not host_data or not _host_is_remna(host_data):
        return False
    user_id = _lookup_user_id_from_context(client_email)
    username = _build_remna_username(user_id, client_email)
    user = _get_remna_user_sync(host_data, user_id, username)
    if not user:
        return False
    user_uuid = str(user.get("uuid") or "").strip()
    if not user_uuid:
        return False
    action = "enable" if enabled else "disable"
    result = _remna_request_json_sync(
        host_data,
        "POST",
        f"/users/{urllib.parse.quote(user_uuid, safe='')}/actions/{action}",
        ok_statuses=(200,),
        allow_statuses=(400, 409),
    )
    return result is not None


async def set_client_enabled_on_host(host_name: str, client_email: str, enabled: bool) -> bool:
    return await asyncio.to_thread(_set_client_enabled_sync, host_name, client_email, enabled)


def _update_user_limits_sync(
    host_name: str,
    client_email: str,
    traffic_gb: float | int | str | None = None,
    device_limit: int | None = None,
    reset_traffic: bool = False,
) -> bool:
    host_data = get_host(host_name)
    if not host_data or not _host_is_remna(host_data):
        return False
    user_id = _lookup_user_id_from_context(client_email)
    username = _build_remna_username(user_id, client_email)
    user = _get_remna_user_sync(host_data, user_id, username)
    if not user:
        return False

    update_body = {"uuid": user.get("uuid")}
    if traffic_gb is not None:
        traffic_bytes = _traffic_limit_bytes(traffic_gb)
        update_body["trafficLimitBytes"] = int(traffic_bytes or 0)
        update_body["trafficLimitStrategy"] = "MONTH" if traffic_bytes else "NO_RESET"
    if device_limit is not None:
        update_body["hwidDeviceLimit"] = max(0, int(device_limit))

    result = _remna_request_json_sync(host_data, "PATCH", "/users", payload=update_body, ok_statuses=(200,))
    if result is None:
        return False

    if reset_traffic:
        user_uuid = str(user.get("uuid") or "").strip()
        if user_uuid:
            _remna_request_json_sync(
                host_data,
                "POST",
                f"/users/{urllib.parse.quote(user_uuid, safe='')}/actions/reset-traffic",
                ok_statuses=(200,),
                allow_statuses=(400, 409),
            )
    return True


async def set_client_monthly_reset_on_host(host_name: str, client_email: str, reset_days: int = 30) -> bool:
    _ = reset_days
    host_data = get_host(host_name)
    return bool(host_data and _host_is_remna(host_data))


async def set_client_device_limit_on_host(host_name: str, client_email: str, device_limit: int) -> bool:
    return await asyncio.to_thread(
        _update_user_limits_sync,
        host_name,
        client_email,
        None,
        device_limit,
        False,
    )


async def increase_client_traffic_limit_on_host(host_name: str, client_email: str, add_gb: float) -> bool:
    host_data = get_host(host_name)
    if not host_data or not _host_is_remna(host_data):
        return False
    user_id = _lookup_user_id_from_context(client_email)
    username = _build_remna_username(user_id, client_email)
    user = _get_remna_user_sync(host_data, user_id, username)
    if not user:
        return False
    current_gb = 0.0
    try:
        current_gb = float(int(user.get("trafficLimitBytes") or 0) / (1024 ** 3))
    except Exception:
        current_gb = 0.0
    return await set_client_traffic_limit_on_host(host_name, client_email, current_gb + float(add_gb or 0))


async def set_client_traffic_limit_on_host(host_name: str, client_email: str, traffic_gb: float | int | str | None) -> bool:
    return await asyncio.to_thread(
        _update_user_limits_sync,
        host_name,
        client_email,
        traffic_gb,
        None,
        False,
    )


def _reset_all_clients_traffic_sync(host_name: str) -> tuple[int, int]:
    host_data = get_host(host_name)
    if not host_data or not _host_is_remna(host_data):
        return (0, 0)

    data = _remna_request_json_sync(host_data, "GET", "/users", ok_statuses=(200,))
    users = _extract_users_list(data)
    total = len(users)
    reset = 0
    for user in users:
        user_uuid = str(user.get("uuid") or "").strip()
        if not user_uuid:
            continue
        result = _remna_request_json_sync(
            host_data,
            "POST",
            f"/users/{urllib.parse.quote(user_uuid, safe='')}/actions/reset-traffic",
            ok_statuses=(200,),
            allow_statuses=(400, 409),
        )
        if result is not None:
            reset += 1
    return total, reset


async def reset_all_clients_traffic_on_host(host_name: str) -> tuple[int, int]:
    return await asyncio.to_thread(_reset_all_clients_traffic_sync, host_name)
