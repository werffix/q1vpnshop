import uuid
import os
import hmac
from datetime import datetime, timedelta
import logging
from urllib.parse import urlparse, quote
from typing import List, Dict
from hmac import compare_digest

from py3xui import Api, Client, Inbound

from shop_bot.data_manager.database import (
    get_host,
    get_key_by_email,
    get_setting,
    get_or_create_user_subscription_token,
    get_user_id_by_subscription_token,
)

logger = logging.getLogger(__name__)

def _obj_get(obj, key, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

def _build_vless_uri_from_inbound_and_client(host_url: str, host_name: str, inbound: Inbound, client_obj) -> str | None:
    client_uuid = _obj_get(client_obj, "id")
    if not client_uuid:
        return None

    parsed = urlparse(host_url if "://" in host_url else f"https://{host_url}")
    server = parsed.hostname or parsed.netloc or ""
    if not server:
        return None
    port = _obj_get(inbound, "port")
    if not port:
        return None

    stream = _obj_get(inbound, "stream_settings")
    network = _obj_get(stream, "network", "tcp") or "tcp"
    security = (_obj_get(stream, "security", "none") or "none").lower()

    params: dict[str, str] = {"type": str(network), "security": str(security)}

    # Try to include transport path if present.
    ws_settings = _obj_get(stream, "ws_settings") or _obj_get(stream, "wsSettings")
    ws_path = _obj_get(ws_settings, "path")
    if ws_path:
        params["path"] = str(ws_path)

    # Reality extras (for modern VLESS Reality setups).
    if security == "reality":
        reality = _obj_get(stream, "reality_settings") or _obj_get(stream, "realitySettings") or {}
        settings = _obj_get(reality, "settings") or {}
        pbk = _obj_get(settings, "publicKey")
        if pbk:
            params["pbk"] = str(pbk)
        fp = _obj_get(settings, "fingerprint")
        if fp:
            params["fp"] = str(fp)
        server_names = _obj_get(reality, "serverNames") or []
        if server_names:
            params["sni"] = str(server_names[0])
        short_ids = _obj_get(reality, "shortIds") or []
        if short_ids:
            params["sid"] = str(short_ids[0])
        params["spx"] = "/"
        flow = _obj_get(client_obj, "flow")
        if flow:
            params["flow"] = str(flow)

    query = "&".join(f"{quote(str(k), safe='')}={quote(str(v), safe='')}" for k, v in params.items())
    remark = quote(str(host_name or "VPN"), safe="")
    return f"vless://{client_uuid}@{server}:{port}?{query}#{remark}"

def resolve_user_id_by_legacy_sub_token(token: str, all_keys: list[dict]) -> int | None:
    """Resolve user by old x-ui sub token (without dot signature)."""
    token = (token or "").strip()
    if not token:
        return None

    keys_by_host: dict[str, list[dict]] = {}
    for key in (all_keys or []):
        host_name = (key.get("host_name") or "").strip()
        if not host_name:
            continue
        keys_by_host.setdefault(host_name, []).append(key)

    for host_name, host_keys in keys_by_host.items():
        host = get_host(host_name)
        if not host:
            continue
        api, inbound = login_to_host(
            host_url=host["host_url"],
            username=host["host_username"],
            password=host["host_pass"],
            inbound_id=host["host_inbound_id"]
        )
        if not api or not inbound:
            continue
        try:
            full_inbound = api.inbound.get_by_id(inbound.id)
            clients = (_obj_get(_obj_get(full_inbound, "settings"), "clients") or [])
            by_uuid = {_obj_get(c, "id"): c for c in clients if _obj_get(c, "id")}
            by_email = {_obj_get(c, "email"): c for c in clients if _obj_get(c, "email")}
            token_fields = ("subId", "subscription", "sub_id", "subscriptionId", "subscription_token")

            for key in host_keys:
                client = by_uuid.get(key.get("xui_client_uuid")) or by_email.get(key.get("key_email"))
                if not client:
                    continue
                for field in token_fields:
                    val = _obj_get(client, field)
                    if val and str(val).strip() == token:
                        try:
                            return int(key.get("user_id"))
                        except Exception:
                            return None
        except Exception:
            continue
    return None

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

def build_unified_subscription_url(user_id: int, base_domain: str | None = None) -> str | None:
    domain = (base_domain or get_setting("domain") or "").strip()
    if not domain:
        return None
    token = get_or_create_user_subscription_token(user_id)
    candidate = domain if "://" in domain else f"https://{domain}"

    # Support explicit templates like https://sub.example.com/sub/{token}
    if "{token}" in candidate:
        return candidate.replace("{token}", token)

    parsed = urlparse(candidate)
    if not parsed.netloc:
        return None
    base = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    path = (parsed.path or "").rstrip("/")
    if path:
        return f"{base}{path}/{token}"
    return f"{base}/sub/{token}"

def normalize_xui_host_url(host_url: str) -> str:
    """Normalize panel URL to scheme://host[:port] for py3xui login."""
    raw = (host_url or "").strip()
    if not raw:
        return raw

    candidate = raw if "://" in raw else f"https://{raw}"
    parsed = urlparse(candidate)
    if parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return raw.rstrip("/")

def build_xui_host_candidates(host_url: str) -> list[str]:
    """Build host candidates for different x-ui web base path setups."""
    raw = (host_url or "").strip().rstrip("/")
    if not raw:
        return []

    candidate = raw if "://" in raw else f"https://{raw}"
    parsed = urlparse(candidate)

    candidates: list[str] = []
    if parsed.scheme and parsed.netloc:
        full = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        if full:
            candidates.append(full)

        # If user pasted direct login URL, try parent path as well.
        if parsed.path.endswith("/login"):
            parent = parsed.path[: -len("/login")].rstrip("/")
            parent_url = f"{parsed.scheme}://{parsed.netloc}{parent}".rstrip("/")
            if parent_url:
                candidates.append(parent_url)

        base = f"{parsed.scheme}://{parsed.netloc}"
        candidates.append(base)

    # Deduplicate preserving order.
    unique: list[str] = []
    for c in candidates:
        if c and c not in unique:
            unique.append(c)
    return unique

def login_to_host(host_url: str, username: str, password: str, inbound_id: int) -> tuple[Api | None, Inbound | None]:
    last_error = None
    candidates = build_xui_host_candidates(host_url)
    if not candidates:
        candidates = [normalize_xui_host_url(host_url)]

    for candidate_host in candidates:
        try:
            api = Api(host=candidate_host, username=username, password=password)
            api.login()
            inbounds: List[Inbound] = api.inbound.get_list()
            target_inbound = next((inbound for inbound in inbounds if int(inbound.id) == int(inbound_id)), None)

            if target_inbound is None:
                available_ids = [getattr(i, "id", None) for i in inbounds]
                logger.error(
                    f"Входящий трафик с ID '{inbound_id}' не найден на хосте '{candidate_host}'. "
                    f"Доступные inbound ID: {available_ids}"
                )
                return None, None
            return api, target_inbound
        except Exception as e:
            last_error = e
            logger.warning(f"Не удалось войти в x-ui по адресу '{candidate_host}': {e}")

    logger.error(
        f"Не удалось выполнить вход в x-ui для хоста '{host_url}'. "
        f"Пробованы адреса: {candidates}. Последняя ошибка: {last_error}",
        exc_info=True
    )
    return None, None

def get_connection_string(inbound: Inbound, user_uuid: str, host_url: str, remark: str) -> str | None:
    if not inbound: return None
    settings = inbound.stream_settings.reality_settings.get("settings")
    if not settings: return None
    
    public_key = settings.get("publicKey")
    fp = settings.get("fingerprint")
    server_names = inbound.stream_settings.reality_settings.get("serverNames")
    short_ids = inbound.stream_settings.reality_settings.get("shortIds")
    port = inbound.port
    
    if not all([public_key, server_names, short_ids]): return None
    
    parsed_url = urlparse(host_url)
    short_id = short_ids[0]
    
    connection_string = (
        f"vless://{user_uuid}@{parsed_url.hostname}:{port}"
        f"?type=tcp&security=reality&pbk={public_key}&fp={fp}&sni={server_names[0]}"
        f"&sid={short_id}&spx=%2F&flow=xtls-rprx-vision#{remark}"
    )
    return connection_string

def get_subscription_link(user_uuid: str, host_url: str, host_name: str | None = None, sub_token: str | None = None) -> str:
    """Build subscription URL with the following priority:
    1) Host-specific subscription_url (xui_hosts.subscription_url)
    2) Fallback: domain/host_url + default path
    Supports optional token replacement if base contains "{token}".
    """
    host_base = None
    try:
        if host_name:
            host = get_host(host_name)
            if host:
                host_base = (host.get("subscription_url") or "").strip()
    except Exception:
        host_base = None

    base = (host_base or "").strip()

    if sub_token:
        if base:
            return base.replace("{token}", sub_token) if "{token}" in base else f"{base.rstrip('/')}/{sub_token}"
        parsed = urlparse(host_url)
        host_part = parsed.netloc or parsed.hostname or ""
        scheme = parsed.scheme if parsed.scheme in ("http", "https") else "https"
        if host_part:
            return f"{scheme}://{host_part}/sub/{sub_token}"
        domain = (get_setting("domain") or "").strip()
        domain_candidate = domain if "://" in domain else f"https://{domain}"
        parsed_domain = urlparse(domain_candidate)
        domain_host = parsed_domain.netloc or parsed_domain.hostname or ""
        domain_scheme = parsed_domain.scheme if parsed_domain.scheme in ("http", "https") else "https"
        return f"{domain_scheme}://{domain_host}/sub/{sub_token}"

    if base:
        return base

    domain = (get_setting("domain") or "").strip()
    parsed = urlparse(host_url)
    hostname = domain if domain else (parsed.hostname or "")
    scheme = parsed.scheme if parsed.scheme in ("http", "https") else "https"
    return f"{scheme}://{hostname}/sub/{user_uuid}?format=v2ray"

def update_or_create_client_on_panel(
    api: Api,
    inbound_id: int,
    email: str,
    days_to_add: int | None = None,
    target_expiry_ms: int | None = None,
    traffic_cap_gb: float | int | str | None = None,
    preferred_uuid: str | None = None
) -> tuple[str | None, int | None, str | None]:
    try:
        inbound_to_modify = api.inbound.get_by_id(inbound_id)
        if not inbound_to_modify:
            raise ValueError(f"Could not find inbound with ID {inbound_id}")

        if inbound_to_modify.settings.clients is None:
            inbound_to_modify.settings.clients = []
            
        client_index = -1
        for i, client in enumerate(inbound_to_modify.settings.clients):
            if client.email == email:
                client_index = i
                break
        
        # Determine new expiry time.
        # Guard against invalid zero/negative explicit expiry values.
        safe_target_expiry_ms = 0
        try:
            safe_target_expiry_ms = int(target_expiry_ms or 0)
        except Exception:
            safe_target_expiry_ms = 0

        if safe_target_expiry_ms > 0:
            new_expiry_ms = safe_target_expiry_ms
        else:
            if days_to_add is None:
                # Fallback: keep finite validity instead of creating immortal clients.
                days_to_add = 30
            if client_index != -1:
                existing_client = inbound_to_modify.settings.clients[client_index]
                if existing_client.expiry_time > int(datetime.now().timestamp() * 1000):
                    current_expiry_dt = datetime.fromtimestamp(existing_client.expiry_time / 1000)
                    new_expiry_dt = current_expiry_dt + timedelta(days=days_to_add)
                else:
                    new_expiry_dt = datetime.now() + timedelta(days=days_to_add)
            else:
                new_expiry_dt = datetime.now() + timedelta(days=days_to_add)

            new_expiry_ms = int(new_expiry_dt.timestamp() * 1000)

        client_sub_token: str | None = None

        traffic_bytes = _traffic_limit_bytes(traffic_cap_gb)

        if client_index != -1:
            # Disable auto-reset/auto-renew on extension
            try:
                inbound_to_modify.settings.clients[client_index].reset = 0
            except Exception:
                pass
            inbound_to_modify.settings.clients[client_index].enable = True
            inbound_to_modify.settings.clients[client_index].expiry_time = new_expiry_ms
            if traffic_bytes is not None:
                for attr in ("total_gb", "totalGB"):
                    try:
                        setattr(inbound_to_modify.settings.clients[client_index], attr, traffic_bytes)
                    except Exception:
                        pass
                try:
                    # Reset traffic usage monthly.
                    inbound_to_modify.settings.clients[client_index].reset = 30
                except Exception:
                    pass

            existing_client = inbound_to_modify.settings.clients[client_index]
            if preferred_uuid:
                try:
                    existing_uuid = getattr(existing_client, "id", None)
                    has_conflict = any(
                        (getattr(c, "id", None) == preferred_uuid) and (getattr(c, "email", None) != email)
                        for c in inbound_to_modify.settings.clients
                    )
                    if not has_conflict and existing_uuid != preferred_uuid:
                        existing_client.id = preferred_uuid
                except Exception:
                    pass
            client_uuid = existing_client.id
            try:
                sub_token_existing = None
                for attr in ("subId", "subscription", "sub_id"):
                    if hasattr(existing_client, attr):
                        val = getattr(existing_client, attr)
                        if val:
                            sub_token_existing = val
                            break
                if sub_token_existing:
                    client_sub_token = sub_token_existing
                else:
                    import secrets
                    client_sub_token = secrets.token_hex(12)
                    for attr in ("subId", "subscription", "sub_id"):
                        try:
                            setattr(existing_client, attr, client_sub_token)
                        except Exception:
                            pass
            except Exception:
                pass
        else:
            client_uuid = str(preferred_uuid).strip() if preferred_uuid else str(uuid.uuid4())
            new_client = Client(
                id=client_uuid,
                email=email,
                enable=True,
                flow="xtls-rprx-vision",
                expiry_time=new_expiry_ms
            )
            # Ensure no auto-reset/auto-renew for new clients
            try:
                setattr(new_client, "reset", 0)
            except Exception:
                pass
            if traffic_bytes is not None:
                for attr in ("total_gb", "totalGB"):
                    try:
                        setattr(new_client, attr, traffic_bytes)
                    except Exception:
                        pass
                try:
                    # Reset traffic usage monthly.
                    setattr(new_client, "reset", 30)
                except Exception:
                    pass

            try:
                import secrets
                client_sub_token = secrets.token_hex(12)
                for attr in ("subId", "subscription", "sub_id"):
                    try:
                        setattr(new_client, attr, client_sub_token)
                    except Exception:
                        pass
            except Exception:
                pass
            inbound_to_modify.settings.clients.append(new_client)

        api.inbound.update(inbound_id, inbound_to_modify)

        return client_uuid, new_expiry_ms, client_sub_token

    except Exception as e:
        logger.error(f"Ошибка в update_or_create_client_on_panel: {e}", exc_info=True)
        return None, None, None

async def create_or_update_key_on_host(
    host_name: str,
    email: str,
    days_to_add: int | None = None,
    expiry_timestamp_ms: int | None = None,
    preferred_uuid: str | None = None
) -> Dict | None:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Сбой рабочего процесса: Хост '{host_name}' не найден в базе данных.")
        return None

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )
    if not api or not inbound:
        logger.error(f"Сбой рабочего процесса: Не удалось войти или найти inbound на хосте '{host_name}'.")
        return None
        
    # Prefer exact expiry when provided (e.g., switching hosts), otherwise add days (purchase/extend/trial)
    client_uuid, new_expiry_ms, client_sub_token = update_or_create_client_on_panel(
        api,
        inbound.id,
        email,
        days_to_add=days_to_add,
        target_expiry_ms=expiry_timestamp_ms,
        traffic_cap_gb=host_data.get("client_monthly_traffic_gb"),
        preferred_uuid=preferred_uuid
    )

    if not client_uuid:
        logger.error(f"Сбой рабочего процесса: Не удалось создать/обновить клиента '{email}' на хосте '{host_name}'.")
        return None
    
    connection_string = get_subscription_link(client_uuid, host_data['host_url'], host_name, sub_token=client_sub_token)
    
    logger.info(f"Успешно обработан ключ для '{email}' на хосте '{host_name}'.")
    
    
    return {
        "client_uuid": client_uuid,
        "email": email,
        "expiry_timestamp_ms": new_expiry_ms,
        "connection_string": connection_string,
        "host_name": host_name
    }

async def get_key_details_from_host(key_data: dict) -> dict | None:
    host_name = key_data.get('host_name')
    if not host_name:
        logger.error(f"Не удалось получить данные ключа: отсутствует host_name для key_id {key_data.get('key_id')}")
        return None

    host_db_data = get_host(host_name)
    if not host_db_data:
        logger.error(f"Не удалось получить данные ключа: хост '{host_name}' не найден в базе данных.")
        return None

    api, inbound = login_to_host(
        host_url=host_db_data['host_url'],
        username=host_db_data['host_username'],
        password=host_db_data['host_pass'],
        inbound_id=host_db_data['host_inbound_id']
    )
    if not api or not inbound: return None

    client_sub_token = None
    try:
        if inbound.settings and inbound.settings.clients:
            for client in inbound.settings.clients:
                if getattr(client, "id", None) == key_data['xui_client_uuid'] or getattr(client, "email", None) == key_data.get('key_email'):
                    candidate_fields = ("subId", "subscription", "sub_id", "subscriptionId", "subscription_token")
                    for attr in candidate_fields:
                        val = None
                        if hasattr(client, attr):
                            val = getattr(client, attr)
                        else:
                            try:
                                val = client.get(attr)
                            except Exception:
                                pass
                        if val:
                            client_sub_token = val
                            break
                    break
    except Exception:
        pass
    connection_string = get_subscription_link(
        key_data['xui_client_uuid'],
        host_db_data['host_url'],
        host_name,
        sub_token=client_sub_token
    )
    native_connection_string = get_subscription_link(
        key_data['xui_client_uuid'],
        host_db_data['host_url'],
        None,
        sub_token=client_sub_token
    )
    return {
        "connection_string": connection_string,
        "native_connection_string": native_connection_string,
    }

async def get_key_usage_stats_from_host(key_data: dict) -> dict:
    host_name = key_data.get("host_name")
    if not host_name:
        return {}

    host_db_data = get_host(host_name)
    if not host_db_data:
        return {}

    api, inbound = login_to_host(
        host_url=host_db_data['host_url'],
        username=host_db_data['host_username'],
        password=host_db_data['host_pass'],
        inbound_id=host_db_data['host_inbound_id']
    )
    if not api or not inbound:
        return {}

    try:
        full_inbound = api.inbound.get_by_id(inbound.id)
    except Exception:
        full_inbound = inbound

    clients = []
    try:
        clients = (_obj_get(_obj_get(full_inbound, "settings"), "clients") or [])
    except Exception:
        clients = []

    match = None
    target_uuid = key_data.get("xui_client_uuid")
    target_email = key_data.get("key_email") or key_data.get("email")
    for client in clients:
        if target_uuid and _obj_get(client, "id") == target_uuid:
            match = client
            break
        if target_email and _obj_get(client, "email") == target_email:
            match = client
            break
    if not match:
        return {}

    def _as_int(value) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0

    def _as_float(value) -> float:
        try:
            if value in (None, ""):
                return 0.0
            if isinstance(value, str):
                cleaned = value.strip().lower().replace("gb", "").strip()
                return float(cleaned or 0)
            return float(value)
        except Exception:
            return 0.0

    total_bytes = _as_int(_obj_get(match, "total"))
    if total_bytes <= 0:
        total_gb = _as_float(_obj_get(match, "totalGB"))
        if total_gb > 0:
            # 3x-ui may report totalGB as GB units; normalize to bytes.
            total_bytes = int(total_gb * (1024 ** 3))
    if total_bytes <= 0:
        total_gb = _as_float(_obj_get(match, "total_gb"))
        if total_gb > 0:
            total_bytes = int(total_gb * (1024 ** 3))

    return {
        "upload_bytes": max(_as_int(_obj_get(match, "up")), 0),
        "download_bytes": max(_as_int(_obj_get(match, "down")), 0),
        "total_bytes": max(total_bytes, 0),
        "expiry_timestamp_ms": max(_as_int(_obj_get(match, "expiry_time")), 0),
    }

async def build_vless_uri_for_key(key_data: dict) -> str | None:
    host_name = key_data.get("host_name")
    if not host_name:
        return None

    host_db_data = get_host(host_name)
    if not host_db_data:
        return None

    api, inbound = login_to_host(
        host_url=host_db_data['host_url'],
        username=host_db_data['host_username'],
        password=host_db_data['host_pass'],
        inbound_id=host_db_data['host_inbound_id']
    )
    if not api or not inbound:
        return None

    try:
        full_inbound = api.inbound.get_by_id(inbound.id)
    except Exception:
        full_inbound = inbound

    clients = []
    try:
        clients = (_obj_get(_obj_get(full_inbound, "settings"), "clients") or [])
    except Exception:
        clients = []

    match = None
    target_uuid = key_data.get("xui_client_uuid")
    target_email = key_data.get("key_email")
    for client in clients:
        if target_uuid and _obj_get(client, "id") == target_uuid:
            match = client
            break
        if target_email and _obj_get(client, "email") == target_email:
            match = client
            break

    if not match:
        return None

    return _build_vless_uri_from_inbound_and_client(
        host_url=host_db_data["host_url"],
        host_name=host_name,
        inbound=full_inbound,
        client_obj=match
    )

async def delete_client_on_host(host_name: str, client_email: str) -> bool:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Не удалось удалить клиента: хост '{host_name}' не найден.")
        return False

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )

    if not api or not inbound:
        logger.error(f"Не удалось удалить клиента: ошибка входа или поиска inbound для хоста '{host_name}'.")
        return False
        
    try:
        client_to_delete = get_key_by_email(client_email)
        if client_to_delete:
            api.client.delete(inbound.id, client_to_delete['xui_client_uuid'])
            logger.info(f"Клиент '{client_email}' успешно удалён с хоста '{host_name}'.")
            return True
        else:
            logger.warning(f"Клиент с email '{client_email}' не найден на хосте '{host_name}' для удаления (возможно, уже удалён).")
            return True
            
    except Exception as e:
        logger.error(f"Не удалось удалить клиента '{client_email}' с хоста '{host_name}': {e}", exc_info=True)
        return False


async def set_client_enabled_on_host(host_name: str, client_email: str, enabled: bool) -> bool:
    """Enable/disable existing client on host inbound without deleting it."""
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Не удалось изменить статус клиента: хост '{host_name}' не найден.")
        return False

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )
    if not api or not inbound:
        logger.error(f"Не удалось изменить статус клиента: ошибка входа/поиска inbound для '{host_name}'.")
        return False

    try:
        inbound_to_modify = api.inbound.get_by_id(inbound.id)
        clients = (inbound_to_modify.settings.clients or [])
        target = None
        for client in clients:
            if getattr(client, "email", None) == client_email:
                target = client
                break
        if not target:
            logger.warning(f"Клиент '{client_email}' не найден на хосте '{host_name}' для смены enable.")
            return False

        if bool(getattr(target, "enable", True)) == bool(enabled):
            return True
        target.enable = bool(enabled)
        api.inbound.update(inbound.id, inbound_to_modify)
        logger.info(f"Клиент '{client_email}' на '{host_name}' переключен enable={bool(enabled)}.")
        return True
    except Exception as e:
        logger.error(f"Не удалось переключить enable клиента '{client_email}' на '{host_name}': {e}", exc_info=True)
        return False


async def set_client_monthly_reset_on_host(host_name: str, client_email: str, reset_days: int = 30) -> bool:
    """Set client traffic reset period (days). Use 30 for monthly reset."""
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Не удалось изменить reset клиента: хост '{host_name}' не найден.")
        return False

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )
    if not api or not inbound:
        logger.error(f"Не удалось изменить reset клиента: ошибка входа/поиска inbound для '{host_name}'.")
        return False

    try:
        inbound_to_modify = api.inbound.get_by_id(inbound.id)
        clients = (inbound_to_modify.settings.clients or [])
        target = None
        for client in clients:
            if getattr(client, "email", None) == client_email:
                target = client
                break
        if not target:
            logger.warning(f"Клиент '{client_email}' не найден на '{host_name}' для reset.")
            return False

        current = int(getattr(target, "reset", 0) or 0)
        if current == int(reset_days):
            return True
        target.reset = int(reset_days)
        api.inbound.update(inbound.id, inbound_to_modify)
        logger.info(f"Клиент '{client_email}' на '{host_name}' reset={int(reset_days)}.")
        return True
    except Exception as e:
        logger.error(f"Не удалось изменить reset клиента '{client_email}' на '{host_name}': {e}", exc_info=True)
        return False
