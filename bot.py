import asyncio
import csv
import json
import os
import time
import traceback
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import BotCommand, FSInputFile, Message
from dotenv import load_dotenv

env_path = Path(__file__).with_name(".env")
print("ENV PATH:", env_path)
print("ENV EXISTS:", env_path.exists())
print("ENV RAW CONTENT:")
print(env_path.read_text(encoding="utf-8", errors="replace"))

load_dotenv(dotenv_path=env_path, override=True)

print("TELEGRAM_TOKEN loaded:", bool(os.getenv("TELEGRAM_TOKEN")))
print("GOOGLE_API_KEY loaded:", bool(os.getenv("GOOGLE_API_KEY")))

telegram_token = (os.getenv("TELEGRAM_TOKEN") or "").strip()
google_api_key = (os.getenv("GOOGLE_API_KEY") or "").strip()

if not telegram_token:
    raise RuntimeError("TELEGRAM_TOKEN")
if not google_api_key:
    raise RuntimeError("GOOGLE_API_KEY")

import scraper

bot = Bot(token=telegram_token, timeout=30)
dp = Dispatcher()

CACHE_PATH = Path(__file__).with_name("cache.json")
CACHE_TTL_SECONDS = 24 * 60 * 60


def _load_cache() -> dict[str, Any]:
    try:
        if not CACHE_PATH.exists():
            return {}
        raw = CACHE_PATH.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        traceback.print_exc()
        return {}


def _save_cache(cache: dict[str, Any]) -> None:
    try:
        CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        traceback.print_exc()


def _make_cache_key(city: str, category: str, *, filters: dict[str, Any], limit: int) -> str:
    key_obj = {
        "city": (city or "").strip().casefold(),
        "category": (category or "").strip().casefold(),
        "filters": {k: filters[k] for k in sorted(filters.keys())},
        "limit": int(limit),
    }
    return json.dumps(key_obj, ensure_ascii=False, sort_keys=True)

last_results_by_chat_id: dict[int, list[dict[str, Any]]] = {}

HELP_TEXT = (
    "Команды:\n"
    "/start — начать работу\n"
    "/help — показать все команды\n"
    "/search <city> <category>\n"
    "/search <city> <category> nosite\n"
    "/search <city> <category> withsite\n"
    "/search <city> <category> outdated\n"
    "/search <city> <category> broken\n"
    "/search <city> <category> limit=10\n"
    "/export — экспорт последних результатов в CSV\n\n"
    "Примеры:\n"
    "/search Helsinki restaurant\n"
    "/search Helsinki restaurant nosite\n"
    "/search Helsinki dentist outdated limit=10\n"
    "/export"
)


def _normalize_website_state(lead: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(lead, dict):
        return lead

    if "website_checked" not in lead:
        website = (lead.get("website") or "").strip()
        if website:
            lead["website_checked"] = True
            lead["has_website"] = True
        else:
            lead["website_checked"] = False
            lead["has_website"] = None

    if "has_website" not in lead:
        lead["has_website"] = True if (lead.get("website") or "").strip() else None

    return lead

def _format_lead(lead: dict[str, Any]) -> str:
    name = (lead.get("name") or "").strip() or "—"
    phone = (lead.get("phone") or "").strip() or "—"
    address = (lead.get("address") or "").strip() or "—"
    website = (lead.get("website") or "").strip()
    website_checked = bool(lead.get("website_checked"))
    has_website = lead.get("has_website")

    if has_website is True:
        website_status = (lead.get("website_status") or "").strip().lower()
        year_est = lead.get("website_year_estimate")
        platform = (lead.get("website_platform") or "unknown")
        outdated = bool(lead.get("website_outdated"))
        notes = (lead.get("website_notes") or "").strip() or "—"

        year_text = "—"
        if isinstance(year_est, int):
            year_text = str(year_est)
        elif isinstance(year_est, str) and year_est.strip().isdigit():
            year_text = year_est.strip()

        header = "🌐 Has website"
        if outdated or website_status == "broken":
            header = "⚠️ WARM LEAD — outdated or broken website"

        lines = [
            header,
            f"Название: {name}",
            f"Телефон: {phone}",
            f"Адрес: {address}",
        ]
        if website:
            lines.append(f"Сайт: {website}")
        lines.append(f"Примерный год сайта: {year_text}")
        lines.append(f"Платформа: {platform}")
        lines.append(f"Устаревший сайт: {'yes' if outdated else 'no'}")
        lines.append(f"Заметка: {notes}")
        return "\n".join(lines)

    if website_checked and has_website is False:
        return "\n".join(
            [
                "🔥 HOT LEAD — no website",
                f"Название: {name}",
                f"Телефон: {phone}",
                f"Адрес: {address}",
            ]
        )

    return "\n".join(
        [
            "❓ Website not verified",
            f"Название: {name}",
            f"Телефон: {phone}",
            f"Адрес: {address}",
        ]
    )

@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    try:
        await message.answer(
            "Привет! Я бот для поиска бизнесов.\n"
            "Используй /search <город> <категория>.\n"
            "Напиши /help для примеров."
        )
    except Exception:
        traceback.print_exc()

@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    try:
        await message.answer(HELP_TEXT)
    except Exception:
        traceback.print_exc()


@dp.message(Command("export"))
async def cmd_export(message: Message) -> None:
    try:
        chat_id = int(message.chat.id)
        results = last_results_by_chat_id.get(chat_id) or []
        if not results:
            await message.answer("Сначала выполните поиск через /search")
            return

        file_name = f"leads_{chat_id}.csv"
        with open(file_name, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "name",
                    "phone",
                    "email",
                    "address",
                    "website",
                    "has_website",
                    "website_status",
                    "website_year_estimate",
                    "website_platform",
                    "website_outdated",
                    "website_notes",
                ],
                extrasaction="ignore",
            )
            writer.writeheader()
            for r in results:
                writer.writerow(
                    {
                        "name": (r.get("name") or ""),
                        "phone": (r.get("phone") or ""),
                        "email": (r.get("email") or ""),
                        "address": (r.get("address") or ""),
                        "website": (r.get("website") or ""),
                        "has_website": r.get("has_website"),
                        "website_status": (r.get("website_status") or ""),
                        "website_year_estimate": (r.get("website_year_estimate") or ""),
                        "website_platform": (r.get("website_platform") or ""),
                        "website_outdated": bool(r.get("website_outdated")),
                        "website_notes": (r.get("website_notes") or ""),
                    }
                )

        await message.answer_document(FSInputFile(file_name))
    except Exception:
        traceback.print_exc()
        try:
            await message.answer("Произошла ошибка. Попробуйте позже.")
        except Exception:
            traceback.print_exc()

@dp.message(Command("search"))
async def cmd_search(message: Message) -> None:
    try:
        text = (message.text or "").strip()
        parts = text.split()
        if len(parts) < 3:
            await message.answer(
                "Использование:\n"
                "/search <город> <категория>\n"
                "/search <город> <категория> nosite\n"
                "/search <город> <категория> limit=10\n"
                "/search <город> <категория> nosite limit=10"
            )
            return

        city = parts[1].strip()
        category = parts[2].strip()

        nosite = False
        withsite = False
        only_outdated = False
        only_broken = False
        limit = 15
        for t in parts[3:]:
            tl = t.strip().lower()
            if not tl:
                continue
            if tl == "nosite":
                nosite = True
                continue
            if tl == "withsite":
                withsite = True
                continue
            if tl == "outdated":
                only_outdated = True
                continue
            if tl == "broken":
                only_broken = True
                continue
            if tl.startswith("limit="):
                raw = tl.split("=", 1)[1].strip()
                try:
                    limit = int(raw)
                except Exception:
                    limit = 15

        if limit < 1:
            limit = 1
        if limit > 30:
            limit = 30

        await message.answer("Ищу, подождите...")

        filters = {
            "nosite": bool(nosite),
            "withsite": bool(withsite),
            "outdated": bool(only_outdated),
            "broken": bool(only_broken),
        }
        cache_key = _make_cache_key(city, category, filters=filters, limit=limit)
        cache = _load_cache()
        now = int(time.time())

        cached_entry = cache.get(cache_key) if isinstance(cache, dict) else None
        if isinstance(cached_entry, dict) and (now - int(cached_entry.get("ts") or 0) <= CACHE_TTL_SECONDS):
            print("Using cached results")
            results = cached_entry.get("results") or []
            if not isinstance(results, list):
                results = []
        else:
            print("Fetching fresh results")
            results = await asyncio.to_thread(scraper.search_businesses, city, category, google_api_key, nosite)

            results = [_normalize_website_state(r) for r in (results or []) if isinstance(r, dict)]

            if withsite:
                results = [r for r in results if r.get("has_website") is True]
            if nosite:
                results = [r for r in results if bool(r.get("website_checked")) and r.get("has_website") is False]
            if only_outdated:
                results = [r for r in results if r.get("has_website") is True and bool(r.get("website_outdated"))]
            if only_broken:
                results = [r for r in results if (r.get("website_status") == "broken")]

            results = (results or [])[:limit]

            if not isinstance(cache, dict):
                cache = {}
            cache[cache_key] = {"ts": now, "results": results}

            try:
                expired_keys: list[str] = []
                for k, v in list(cache.items()):
                    if not isinstance(v, dict):
                        expired_keys.append(k)
                        continue
                    ts = int(v.get("ts") or 0)
                    if now - ts > CACHE_TTL_SECONDS:
                        expired_keys.append(k)
                for k in expired_keys:
                    cache.pop(k, None)
            except Exception:
                traceback.print_exc()

            _save_cache(cache)

        results = [_normalize_website_state(r) for r in (results or []) if isinstance(r, dict)]

        last_results_by_chat_id[int(message.chat.id)] = list(results)

        if not results:
            await message.answer("Ничего не найдено.")
            return

        blocks = [_format_lead(r) for r in results]

        chunk: list[str] = []
        size = 0
        for b in blocks:
            add = len(b) + (2 if chunk else 0)
            if chunk and size + add > 3500:
                await message.answer("\n\n".join(chunk))
                chunk = []
                size = 0
            chunk.append(b)
            size += add

        if chunk:
            await message.answer("\n\n".join(chunk))
    except Exception:
        traceback.print_exc()
        try:
            await message.answer("Произошла ошибка. Попробуйте позже.")
        except Exception:
            traceback.print_exc()

async def main() -> None:
    print("✅ Bot started successfully!")
    try:
        await bot.set_my_commands(
            [
                BotCommand(command="start", description="Запустить бота"),
                BotCommand(command="help", description="Показать все команды"),
                BotCommand(command="search", description="Поиск бизнесов"),
                BotCommand(command="export", description="Экспорт результатов в CSV"),
            ]
        )
        await dp.start_polling(bot)
    except Exception:
        traceback.print_exc()
    finally:
        try:
            await bot.session.close()
        except Exception:
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
