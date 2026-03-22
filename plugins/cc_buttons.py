"""
plugins/cc_buttons.py
Handles the 🔥 Hardsub and 🔄 Convert buttons added to URL keyboards.

Callbacks:
  cchs|<token>  → start hardsub flow (asks for subtitle)
  ccnv|<token>  → show resolution picker → submit convert to CloudConvert
  ccres|<h>|<token> → resolution picked → submit convert job
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import urllib.parse as _urlparse

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
)

from services.utils import safe_edit

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 🔥 Hardsub button  →  cchs|<token>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^cchs\|"))
async def cc_hardsub_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 2:
        return await cb.answer("Invalid data.", show_alert=True)

    token = parts[1]
    uid = cb.from_user.id
    await cb.answer()

    # Get URL from url_handler's cache
    from plugins.url_handler import _get
    url = _get(token)
    if not url:
        return await safe_edit(cb.message, "❌ Session expired. Resend the link.")

    raw_name = url.split("/")[-1].split("?")[0]
    fname = _urlparse.unquote_plus(raw_name)[:50] or "video.mkv"

    st = cb.message

    # Start hardsub flow — sets state to waiting_subtitle
    from plugins.hardsub import start_hardsub_for_url
    await start_hardsub_for_url(client, st, uid, url, fname)


# ─────────────────────────────────────────────────────────────
# 🔄 Convert button  →  ccnv|<token>  →  show resolution picker
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^ccnv\|"))
async def cc_convert_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 2:
        return await cb.answer("Invalid data.", show_alert=True)

    token = parts[1]
    uid = cb.from_user.id
    await cb.answer()

    from plugins.url_handler import _get
    url = _get(token)
    if not url:
        return await safe_edit(cb.message, "❌ Session expired. Resend the link.")

    raw_name = url.split("/")[-1].split("?")[0]
    fname = _urlparse.unquote_plus(raw_name)[:50] or "video.mkv"

    await safe_edit(cb.message,
        "🔄 <b>CloudConvert Convert</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{fname[:45]}</code>\n\n"
        "📐 <b>Choose output resolution:</b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎬 Original",  callback_data=f"ccres|0|{token}"),
             InlineKeyboardButton("🔵 1080p",     callback_data=f"ccres|1080|{token}")],
            [InlineKeyboardButton("🟢 720p",      callback_data=f"ccres|720|{token}"),
             InlineKeyboardButton("🟡 480p",      callback_data=f"ccres|480|{token}")],
            [InlineKeyboardButton("🟠 360p",      callback_data=f"ccres|360|{token}"),
             InlineKeyboardButton("❌ Cancel",     callback_data=f"dl|cancel|{token}")],
        ]),
    )


# ─────────────────────────────────────────────────────────────
# Resolution picked  →  ccres|<height>|<token>  →  submit convert
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^ccres\|"))
async def cc_resolution_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, height_str, token = parts[:3]
    uid = cb.from_user.id
    await cb.answer()

    from plugins.url_handler import _get
    url = _get(token)
    if not url:
        return await safe_edit(cb.message, "❌ Session expired. Resend the link.")

    scale_height = int(height_str) if height_str.isdigit() else 0
    res_label = f"{scale_height}p" if scale_height else "Original"

    raw_name = url.split("/")[-1].split("?")[0]
    video_fname = _urlparse.unquote_plus(raw_name)[:50] or "video.mkv"
    name_base = os.path.splitext(video_fname)[0]
    output_name = re.sub(r'[^\w\s\-\[\]()]', '_', name_base).strip() + f" [{res_label}].mp4"

    await safe_edit(cb.message,
        "🔄 <b>Submitting to CloudConvert…</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{video_fname[:42]}</code>\n"
        f"📐 <b>{res_label}</b>\n"
        f"📤 → <code>{output_name[:42]}</code>\n\n"
        "<i>CloudConvert will convert and the webhook\n"
        "will auto-upload the result.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        api_key = os.environ.get("CC_API_KEY", "").strip()
        from services.cloudconvert_api import submit_convert, parse_api_keys, pick_best_key

        keys = parse_api_keys(api_key)
        if len(keys) > 1:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits)"
        else:
            key_info = "🔑 1 API key"

        job_id = await submit_convert(
            api_key,
            video_url=url,
            output_name=output_name,
            scale_height=scale_height,
        )

        await safe_edit(cb.message,
            "✅ <b>Convert Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"🎬 <code>{video_fname[:38]}</code>\n"
            f"📐 <b>{res_label}</b>\n"
            f"📦 → <code>{output_name[:38]}</code>\n"
            f"☁️ URL import (no upload needed)\n"
            f"{key_info}\n\n"
            "⏳ <i>CloudConvert is processing…\n"
            "The webhook will auto-upload the result.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        log.info("[Convert] Job %s submitted for uid=%d: %s → %s",
                 job_id, uid, video_fname, output_name)

    except Exception as exc:
        log.error("[Convert] Submit failed: %s", exc, exc_info=True)
        await safe_edit(cb.message,
            f"❌ <b>Convert failed</b>\n\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
