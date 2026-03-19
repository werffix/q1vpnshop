import asyncio
import logging
import json
import hashlib
import re

from datetime import datetime, timedelta

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import Bot

from shop_bot.bot_controller import BotController
from shop_bot.data_manager import database
from shop_bot.data_manager import speedtest_runner
from shop_bot.data_manager import backup_manager
from shop_bot.data_manager import resource_monitor

from shop_bot.modules import xui_api
from shop_bot.bot import keyboards

CHECK_INTERVAL_SECONDS = 300
NOTIFY_BEFORE_HOURS = {48}
notified_users = {}
notified_user_marks: dict[int, set[int]] = {}
expired_notified_users: set[int] = set()

logger = logging.getLogger(__name__)

# Запуск обоих видов измерений 3 раза в сутки (каждые 8 часов)
SPEEDTEST_INTERVAL_SECONDS = 8 * 3600
_last_speedtests_run_at: datetime | None = None
_last_backup_run_at: datetime | None = None

# Сбор метрик ресурсов (каждые 5 минут)
METRICS_INTERVAL_SECONDS = 5 * 60
_last_metrics_run_at: datetime | None = None

def format_time_left(hours: int) -> str:
    if hours >= 24:
        days = hours // 24
        if days % 10 == 1 and days % 100 != 11:
            return f"{days} день"
        elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
            return f"{days} дня"
        else:
            return f"{days} дней"
    else:
        if hours % 10 == 1 and hours % 100 != 11:
            return f"{hours} час"
        elif 2 <= hours % 10 <= 4 and (hours % 100 < 10 or hours % 100 >= 20):
            return f"{hours} часа"
        else:
            return f"{hours} часов"

def _subscription_email_for_user_host(user_id: int, host_name: str) -> str:
    host_part = re.sub(r"[^a-z0-9]+", "", (host_name or "").lower())[:8] or "host"
    digest = hashlib.sha1(f"{user_id}:{host_name}".encode("utf-8")).hexdigest()[:10]
    return f"u{user_id}.{host_part}.{digest}@bot.local"

async def send_subscription_notification(bot: Bot, user_id: int, key_id: int, time_left_hours: int, expiry_date: datetime):
    try:
        bot_username = (database.get_setting("telegram_bot_username") or "").strip()
        if not bot_username:
            try:
                bot_username = (await bot.get_me()).username or ""
            except Exception:
                bot_username = ""
        referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}" if bot_username else "https://t.me/"
        message = (
            "📢 Срок действия вашей подписки на q1 vpn истекает через 2 дня!\n"
            "Пожалуйста, продлите подписку заранее, чтобы сохранить доступ к сервису без перерывов.\n\n"
            "Вы также можете пригласить друзей в бота и получить 7 дней бесплатной подписки "
            "за каждого приглашённого по вашей ссылке:\n\n"
            f"{referral_link}"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Продлить подписку", callback_data="buy_traffic_start")
        builder.button(text="🤝 Реферальная программа", callback_data="show_referral_program")
        builder.adjust(1)
        
        await bot.send_message(chat_id=user_id, text=message, reply_markup=builder.as_markup())
        logger.debug(f"Scheduler: Отправлено уведомление пользователю {user_id} по ключу {key_id} (осталось {time_left_hours} ч).")
        
    except Exception as e:
        logger.error(f"Scheduler: Ошибка отправки уведомления пользователю {user_id}: {e}")

async def send_subscription_expired_notification(bot: Bot, user_id: int):
    try:
        message = (
            "❗ Срок действия подписки истек\n\n"
            "Срок действия вашей VPN-подписки завершён.\n"
            "Доступ к сервису приостановлен до момента продления.\n\n"
            "Пожалуйста, продлите подписку, чтобы восстановить доступ."
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Продлить подписку", callback_data="buy_traffic_start")
        builder.adjust(1)
        await bot.send_message(chat_id=user_id, text=message, reply_markup=builder.as_markup())
        logger.info(f"Scheduler: Отправлено уведомление об окончании подписки пользователю {user_id}.")
    except Exception as e:
        logger.error(f"Scheduler: Ошибка отправки уведомления об окончании пользователю {user_id}: {e}")

def _cleanup_notified_users(all_db_keys: list[dict]):
    if not notified_users:
        # keep user-level cache in sync too
        active_users = {int(k.get('user_id')) for k in all_db_keys if k.get('user_id') is not None}
        for uid in list(notified_user_marks.keys()):
            if uid not in active_users:
                notified_user_marks.pop(uid, None)
        return

    logger.debug("Scheduler: Очищаю кэш уведомлений...")
    
    active_key_ids = {key['key_id'] for key in all_db_keys}
    active_users = {int(key.get('user_id')) for key in all_db_keys if key.get('user_id') is not None}
    
    users_to_check = list(notified_users.keys())
    
    cleaned_users = 0
    cleaned_keys = 0

    for user_id in users_to_check:
        keys_to_check = list(notified_users[user_id].keys())
        for key_id in keys_to_check:
            if key_id not in active_key_ids:
                del notified_users[user_id][key_id]
                cleaned_keys += 1
        
        if not notified_users[user_id]:
            del notified_users[user_id]
            cleaned_users += 1
    
    if cleaned_users > 0 or cleaned_keys > 0:
        logger.debug(f"Scheduler: Очистка завершена. Удалено записей пользователей: {cleaned_users}, ключей: {cleaned_keys}.")
    for uid in list(notified_user_marks.keys()):
        if uid not in active_users:
            notified_user_marks.pop(uid, None)
    for uid in list(expired_notified_users):
        if uid not in active_users:
            expired_notified_users.discard(uid)

async def check_expiring_subscriptions(bot: Bot):
    logger.debug("Scheduler: Проверяю истекающие подписки...")
    current_time = datetime.now()
    all_keys = database.get_all_keys()
    
    _cleanup_notified_users(all_keys)
    
    user_state: dict[int, dict[str, bool]] = {}

    for key in all_keys:
        try:
            expiry_date = datetime.fromisoformat(key['expiry_date'])
            time_left = expiry_date - current_time

            total_hours_left = int(time_left.total_seconds() / 3600)
            user_id = key['user_id']
            key_id = key['key_id']
            if user_id is not None:
                st = user_state.setdefault(int(user_id), {"has_any": True, "has_active": False})
                st["has_any"] = True
                if time_left.total_seconds() > 0:
                    st["has_active"] = True

            if time_left.total_seconds() < 0:
                continue

            for hours_mark in NOTIFY_BEFORE_HOURS:
                if hours_mark - 1 < total_hours_left <= hours_mark:
                    notified_users.setdefault(user_id, {}).setdefault(key_id, set())
                    notified_user_marks.setdefault(user_id, set())
                    
                    if (
                        hours_mark not in notified_users[user_id][key_id]
                        and hours_mark not in notified_user_marks[user_id]
                    ):
                        await send_subscription_notification(bot, user_id, key_id, hours_mark, expiry_date)
                        notified_users[user_id][key_id].add(hours_mark)
                        notified_user_marks[user_id].add(hours_mark)
                    break 
                    
        except Exception as e:
            logger.error(f"Scheduler: Ошибка обработки истечения для ключа {key.get('key_id')}: {e}")

    for uid, st in user_state.items():
        if st.get("has_active"):
            expired_notified_users.discard(uid)
            continue
        if st.get("has_any") and uid not in expired_notified_users:
            await send_subscription_expired_notification(bot, uid)
            expired_notified_users.add(uid)

async def sync_keys_with_panels():
    logger.debug("Scheduler: Запускаю синхронизацию с XUI-панелями...")
    total_affected_records = 0
    
    all_hosts = database.get_all_hosts()
    if not all_hosts:
        logger.debug("Scheduler: Хосты в базе не настроены. Синхронизация пропущена.")
        return

    for host in all_hosts:
        host_name = host['host_name']
        is_expired_host = int(host.get("is_expired_host") or 0) == 1
        logger.debug(f"Scheduler: Обрабатываю хост: '{host_name}'")

        if is_expired_host:
            try:
                all_users = database.get_all_users() or []
                for user in all_users:
                    user_id = user.get("telegram_id")
                    if not user_id:
                        continue

                    email = _subscription_email_for_user_host(int(user_id), host_name)
                    try:
                        await xui_api.delete_client_on_host(host_name, email)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Scheduler: Ошибка обработки expired-host '{host_name}': {e}", exc_info=True)
            continue
        
        try:
            api, inbound = xui_api.login_to_host(
                host_url=host['host_url'],
                username=host['host_username'],
                password=host['host_pass'],
                inbound_id=host['host_inbound_id']
            )

            if not api or not inbound:
                logger.error(f"Scheduler: Не удалось авторизоваться на хосте '{host_name}'. Пропускаю его.")
                continue
            
            full_inbound_details = api.inbound.get_by_id(inbound.id)
            clients_on_server = {client.email: client for client in (full_inbound_details.settings.clients or [])}
            logger.debug(f"Scheduler: Найдено клиентов на панели '{host_name}': {len(clients_on_server)}")

            keys_in_db = database.get_keys_for_host(host_name)
            
            for db_key in keys_in_db:
                key_email = db_key['key_email']
                expiry_date = datetime.fromisoformat(db_key['expiry_date'])
                now = datetime.now()
                is_expired = expiry_date <= now

                server_client = clients_on_server.pop(key_email, None)

                # Hard expiry policy: remove from 3x-ui and local DB.
                if is_expired:
                    try:
                        await xui_api.delete_client_on_host(host_name, key_email)
                    except Exception as e:
                        logger.warning(f"Scheduler: Не удалось удалить истекший ключ '{key_email}' на '{host_name}': {e}")
                    try:
                        key_id = db_key.get("key_id")
                        if key_id:
                            database.delete_key_by_id(int(key_id))
                            total_affected_records += 1
                    except Exception as e:
                        logger.warning(f"Scheduler: Не удалось удалить запись истекшего ключа key_id={db_key.get('key_id')}: {e}")
                    continue

                if server_client:
                    server_expiry_ms = int(getattr(server_client, "expiry_time", 0) or 0)
                    local_expiry_dt = expiry_date
                    local_expiry_ms = int(local_expiry_dt.timestamp() * 1000)

                    # Source of truth is local DB expiry (tariff-selected date).
                    # Some 3x-ui builds may auto-shift expiry/reset and this must not prolong subscription.
                    if abs(server_expiry_ms - local_expiry_ms) > 1000:
                        try:
                            repaired = await xui_api.create_or_update_key_on_host(
                                host_name=host_name,
                                email=key_email,
                                days_to_add=None,
                                expiry_timestamp_ms=local_expiry_ms,
                            )
                            if repaired and repaired.get("expiry_timestamp_ms"):
                                database.update_key_info(
                                    int(db_key.get("key_id")),
                                    repaired.get("client_uuid") or db_key.get("xui_client_uuid") or "",
                                    int(repaired.get("expiry_timestamp_ms") or local_expiry_ms),
                                )
                                total_affected_records += 1
                                logger.debug(
                                    f"Scheduler: Принудительно восстановлен срок ключа '{key_email}' на '{host_name}' "
                                    f"до локальной даты."
                                )
                        except Exception as e:
                            logger.warning(
                                f"Scheduler: Не удалось синхронизировать срок ключа '{key_email}' с локальной БД "
                                f"на '{host_name}': {e}"
                            )
                    try:
                        await xui_api.set_client_enabled_on_host(host_name, key_email, enabled=True)
                    except Exception as e:
                        logger.warning(f"Scheduler: Не удалось включить активный ключ '{key_email}' на '{host_name}': {e}")

                    # Do not force panel reset period here: on some 3x-ui builds it can auto-extend expiry.
                else:
                    logger.warning(
                        f"Scheduler: Ключ '{key_email}' для хоста '{host_name}' не найден на сервере."
                    )

            if clients_on_server:
                # Try to attach orphan clients from panel to local DB so old keys get subscriptions
                for orphan_email, orphan_client in clients_on_server.items():
                    try:
                        # Extract user_id from email like: user12345-key1-...@telegram.bot
                        import re
                        m = re.search(r"user(\d+)", orphan_email)
                        user_id = int(m.group(1)) if m else None
                        if not user_id:
                            logger.warning(
                                f"Scheduler: Найден осиротевший клиент '{orphan_email}' на '{host_name}', но не удалось определить user_id — пропускаю."
                            )
                            continue

                        # Check that user exists
                        usr = database.get_user(user_id)
                        if not usr:
                            logger.warning(
                                f"Scheduler: Осиротевший клиент '{orphan_email}' указывает на user_id={user_id}, но пользователь не найден — пропускаю."
                            )
                            continue

                        # If key already present (race/duplicate), skip insert
                        existing = database.get_key_by_email(orphan_email)
                        if existing:
                            continue

                        reset_days = getattr(orphan_client, 'reset', 0) or 0
                        expiry_ms = int(getattr(orphan_client, 'expiry_time', 0)) + int(reset_days) * 24 * 3600 * 1000
                        client_uuid = getattr(orphan_client, 'id', None) or getattr(orphan_client, 'email', None) or ''

                        if not client_uuid:
                            logger.warning(
                                f"Scheduler: У осиротевшего клиента '{orphan_email}' нет UUID/id — не могу привязать."
                            )
                            continue

                        new_id = database.add_new_key(
                            user_id=user_id,
                            host_name=host_name,
                            xui_client_uuid=str(client_uuid),
                            key_email=orphan_email,
                            expiry_timestamp_ms=expiry_ms,
                        )
                        if new_id:
                            logger.info(
                                f"Scheduler: Осиротевший клиент '{orphan_email}' на '{host_name}' привязан к пользователю {user_id} как key_id={new_id}."
                            )
                            total_affected_records += 1
                        else:
                            logger.warning(
                                f"Scheduler: Не удалось привязать осиротевшего клиента '{orphan_email}' на '{host_name}'."
                            )
                    except Exception as e:
                        logger.error(
                            f"Scheduler: Ошибка при попытке привязать осиротевшего клиента '{orphan_email}' на '{host_name}': {e}",
                            exc_info=True,
                        )

        except Exception as e:
            logger.error(f"Scheduler: Непредвиденная ошибка при обработке хоста '{host_name}': {e}", exc_info=True)
            
    logger.debug(f"Scheduler: Синхронизация с XUI-панелями завершена. Затронуто записей: {total_affected_records}.")

async def periodic_subscription_check(bot_controller: BotController):
    logger.info("Scheduler: Планировщик фоновых задач запущен.")
    await asyncio.sleep(10)

    while True:
        try:
            await sync_keys_with_panels()

            # Периодические измерения скорости по всем хостам (оба варианта: SSH и сетевой)
            await _maybe_run_periodic_speedtests()
            await _maybe_collect_host_metrics()

            # Ежедневный автобэкап БД с отправкой админам
            bot = bot_controller.get_bot_instance() if bot_controller.get_status().get("is_running") else None
            if bot:
                await _maybe_run_daily_backup(bot)

            if bot_controller.get_status().get("is_running"):
                bot = bot_controller.get_bot_instance()
                if bot:
                    await check_expiring_subscriptions(bot)
                else:
                    logger.warning("Scheduler: Бот помечен как запущенный, но экземпляр недоступен.")
            else:
                logger.debug("Scheduler: Бот остановлен, уведомления пользователям пропущены.")

        except Exception as e:
            logger.error(f"Scheduler: Необработанная ошибка в основном цикле: {e}", exc_info=True)
            
        logger.info(f"Scheduler: Цикл завершён. Следующая проверка через {CHECK_INTERVAL_SECONDS} сек.")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

async def _maybe_run_periodic_speedtests():
    global _last_speedtests_run_at
    now = datetime.now()
    if _last_speedtests_run_at and (now - _last_speedtests_run_at).total_seconds() < SPEEDTEST_INTERVAL_SECONDS:
        return
    try:
        await _run_speedtests_for_all_hosts()
        _last_speedtests_run_at = now
    except Exception as e:
        logger.error(f"Scheduler: Ошибка запуска speedtests: {e}", exc_info=True)

async def _run_speedtests_for_all_hosts():
    hosts = database.get_all_hosts()
    if not hosts:
        logger.debug("Scheduler: Нет хостов для измерений скорости.")
        return
    logger.info(f"Scheduler: Запускаю speedtest для {len(hosts)} хост(ов)...")
    for h in hosts:
        host_name = h.get('host_name')
        if not host_name:
            continue
        try:
            logger.info(f"Scheduler: Speedtest для '{host_name}' запущен...")
            # Ограничим каждый хост таймаутом, чтобы не зависнуть надолго
            try:
                async with asyncio.timeout(180):
                    res = await speedtest_runner.run_both_for_host(host_name)
            except AttributeError:
                # Для Python <3.11: fallback через wait_for
                res = await asyncio.wait_for(speedtest_runner.run_both_for_host(host_name), timeout=180)
            ok = res.get('ok')
            err = res.get('error')
            if ok:
                logger.info(f"Scheduler: Speedtest для '{host_name}' завершён успешно")
            else:
                logger.warning(f"Scheduler: Speedtest для '{host_name}' завершён с ошибками: {err}")
        except asyncio.TimeoutError:
            logger.warning(f"Scheduler: Таймаут speedtest для хоста '{host_name}'")
        except Exception as e:
            logger.error(f"Scheduler: Ошибка выполнения speedtest для '{host_name}': {e}", exc_info=True)

async def _maybe_run_daily_backup(bot: Bot):
    global _last_backup_run_at
    now = datetime.now()
    # Считаем интервал из настроек (в днях). 0 или пусто — автобэкап выключен.
    try:
        s = database.get_setting("backup_interval_days") or "1"
        days = int(str(s).strip() or "1")
    except Exception:
        days = 1
    if days <= 0:
        return
    interval_seconds = max(1, days) * 24 * 3600
    if _last_backup_run_at and (now - _last_backup_run_at).total_seconds() < interval_seconds:
        return
    try:
        zip_path = backup_manager.create_backup_file()
        if zip_path and zip_path.exists():
            try:
                sent = await backup_manager.send_backup_to_admins(bot, zip_path)
                logger.info(f"Scheduler: Создан бэкап {zip_path.name}, отправлен {sent} адм.")
            except Exception as e:
                logger.error(f"Scheduler: Не удалось отправить бэкап: {e}")
            try:
                backup_manager.cleanup_old_backups(keep=7)
            except Exception:
                pass
        _last_backup_run_at = now
    except Exception as e:
        logger.error(f"Scheduler: Критическая ошибка при создании и отправке бэкапа: {e}", exc_info=True)
async def _maybe_collect_host_metrics():
    global _last_metrics_run_at
    now = datetime.now()
    if _last_metrics_run_at and (now - _last_metrics_run_at).total_seconds() < METRICS_INTERVAL_SECONDS:
        return
    
    # Собираем локальные метрики
    try:
        local_metrics = await asyncio.wait_for(asyncio.to_thread(resource_monitor.get_local_metrics), timeout=10)
        if local_metrics and local_metrics.get('ok'):
            database.insert_resource_metric(
                'local', 'panel',
                cpu_percent=local_metrics.get('cpu_percent'),
                mem_percent=local_metrics.get('mem_percent'),
                disk_percent=local_metrics.get('disk_percent'),
                load1=local_metrics.get('loadavg', {}).get('1m') if local_metrics.get('loadavg') else None,
                net_bytes_sent=local_metrics.get('network_sent'),
                net_bytes_recv=local_metrics.get('network_recv'),
                raw_json=json.dumps(local_metrics, ensure_ascii=False)
            )
    except Exception as e:
        logger.error(f"Scheduler: Ошибка сбора локальных метрик: {e}")
    
    # Собираем метрики хостов
    hosts = database.get_all_hosts()
    if not hosts:
        _last_metrics_run_at = now
        return
    for h in hosts:
        host_name = h.get('host_name')
        if not host_name:
            continue
        if not (h.get('ssh_host') and h.get('ssh_user')):
            continue
        try:
            try:
                m = await asyncio.wait_for(asyncio.to_thread(resource_monitor.get_host_metrics_via_ssh, h), timeout=30)
            except AttributeError:
                m = await asyncio.wait_for(asyncio.to_thread(resource_monitor.get_host_metrics_via_ssh, h), timeout=30)
            try:
                database.insert_host_metrics(host_name, m)
                # Также сохраняем в resource_metrics для графиков
                if m and m.get('ok'):
                    database.insert_resource_metric(
                        'host', host_name,
                        cpu_percent=m.get('cpu_percent'),
                        mem_percent=m.get('mem_percent'),
                        disk_percent=m.get('disk_percent'),
                        load1=m.get('loadavg', {}).get('1m') if m.get('loadavg') else None,
                        raw_json=json.dumps(m, ensure_ascii=False)
                    )
            except Exception as e:
                logger.warning(f"Scheduler: insert_host_metrics failed for {host_name}: {e}")
        except asyncio.TimeoutError:
            logger.warning(f"Scheduler: Таймаут сбора метрик для хоста '{host_name}'")
        except Exception as e:
            logger.error(f"Scheduler: Ошибка сбора метрик для '{host_name}': {e}")
    _last_metrics_run_at = now
