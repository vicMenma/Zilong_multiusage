"""
plugins/caption_templates.py
Per-channel caption templates with auto-filled variables.

Variables available in templates:
  {filename}    — clean filename without extension
  {ext}         — file extension (.mp4, .mkv …)
  {filesize}    — human-readable size (e.g. 401.8 MB)
  {resolution}  — video resolution (e.g. 1080p) or blank
  {duration}    — HH:MM:SS or MM:SS
  {audio_lang}  — first audio track language flag + name
  {audio_langs} — all audio languages (flags)
  {sub_langs}   — all subtitle languages (flags)
  {codec}       — video codec (e.g. H.264, HEVC)
  {date}        — upload date YYYY-MM-DD
  {botname}     — bot name

Default template (used when no custom one is set):
  <code>{filename}{ext}</code>

Example custom template:
  🎬 <b>{filename}</b>
  📐 {resolution}  🎵 {audio_langs}  💬 {sub_langs}
  💾 {filesize}  ⏱ {duration}
  ✨ via @MyChannel
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import settings
from services.utils import human_size, fmt_hms, lang_flag

log = logging.getLogger(__name__)

# ── Storage file ──────────────────────────────────────────────
_DATA_DIR   = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)
_STORE_PATH = os.path.join(_DATA_DIR, "caption_templates.json")

# channel_id (str) → template string
_templates: dict[str, str] = {}

DEFAULT_TEMPLATE = "<code>{filename}{ext}</code>"

EXAMPLE_TEMPLATE = (
    "🎬 <b>{filename}</b>\n"
    "📐 {resolution}  🎵 {audio_langs}  💬 {sub_langs}\n"
    "💾 {filesize}  ⏱ {duration}\n"
    "🔖 {codec}"
)


def _load() -> None:
    try:
        with open(_STORE_PATH, encoding="utf-8") as f:
            _templates.update(json.load(f))
        log.info("[CaptionTpl] Loaded %d templates", len(_templates))
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("[CaptionTpl] Load error: %s", e)


def _save() -> None:
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
        with open(_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump(_templates, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning("[CaptionTpl] Save error: %s", e)


_load()


# ─────────────────────────────────────────────────────────────
# Template get / set / delete
# ─────────────────────────────────────────────────────────────

def get_template(channel_id: int) -> str:
    return _templates.get(str(channel_id), DEFAULT_TEMPLATE)


def set_template(channel_id: int, template: str) -> None:
    _templates[str(channel_id)] = template
    _save()


def delete_template(channel_id: int) -> None:
    _templates.pop(str(channel_id), None)
    _save()


def has_custom_template(channel_id: int) -> bool:
    return str(channel_id) in _templates


# ─────────────────────────────────────────────────────────────
# Variable resolution
# ─────────────────────────────────────────────────────────────

async def _probe_vars(path: str) -> dict:
    """Extract all template variables from a file via ffprobe."""
    from services import ffmpeg as FF

    base = os.path.basename(path)
    name, ext = os.path.splitext(base)
    fsize = os.path.getsize(path) if os.path.isfile(path) else 0

    vars_: dict = {
        "filename":   name,
        "ext":        ext,
        "filesize":   human_size(fsize),
        "resolution": "",
        "duration":   "",
        "audio_lang": "",
        "audio_langs":"",
        "sub_langs":  "",
        "codec":      "",
        "date":       datetime.now().strftime("%Y-%m-%d"),
    }

    try:
        from core.bot_name import get_bot_name
        vars_["botname"] = get_bot_name()
    except Exception:
        vars_["botname"] = "Bot"

    try:
        sd  = await FF.probe_streams(path)
        dur = await FF.probe_duration(path)

        if dur:
            vars_["duration"] = fmt_hms(dur)

        # Video stream
        for v in sd.get("video", []):
            h     = v.get("height", 0)
            codec = (v.get("codec_name") or "").lower()
            if h:
                vars_["resolution"] = f"{h}p"
            codec_map = {
                "h264": "H.264", "hevc": "H.265/HEVC",
                "av1":  "AV1",   "vp9": "VP9",
                "mpeg4":"MPEG-4","xvid":"XviD",
            }
            vars_["codec"] = codec_map.get(codec, codec.upper())
            break

        # Audio streams
        audio_flags = []
        for a in sd.get("audio", []):
            tags = a.get("tags") or {}
            lang = (tags.get("language") or "und").lower()
            flag = lang_flag(lang)
            if flag not in audio_flags:
                audio_flags.append(flag)
        if audio_flags:
            vars_["audio_langs"] = " ".join(audio_flags)
            vars_["audio_lang"]  = audio_flags[0]

        # Subtitle streams
        sub_flags = []
        for s in sd.get("subtitle", []):
            tags = s.get("tags") or {}
            lang = (tags.get("language") or "und").lower()
            flag = lang_flag(lang)
            if flag not in sub_flags:
                sub_flags.append(flag)
        if sub_flags:
            vars_["sub_langs"] = " ".join(sub_flags)

    except Exception as exc:
        log.debug("[CaptionTpl] probe_vars failed: %s", exc)

    return vars_


def _render(template: str, vars_: dict) -> str:
    """Substitute variables into a template string."""
    try:
        return template.format_map({k: (v or "") for k, v in vars_.items()})
    except (KeyError, ValueError) as exc:
        log.warning("[CaptionTpl] render error: %s", exc)
        return template


async def build_caption(path: str, channel_id: int) -> str:
    """
    Build the final caption for a file being forwarded to channel_id.
    Returns rendered template with all variables substituted.
    """
    template = get_template(channel_id)
    vars_    = await _probe_vars(path)
    return _render(template, vars_)


# ─────────────────────────────────────────────────────────────
# State for /captiontemplate command
# ─────────────────────────────────────────────────────────────

_WAITING: dict[int, int] = {}   # uid → channel_id being edited


async def _channels_with_templates(uid: int) -> list[dict]:
    """Get all channels the user has configured (from settings)."""
    try:
        s = await settings.get(uid)
        return s.get("forward_channels", [])
    except Exception:
        return []


def _manage_kb(channels: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for ch in channels:
        name    = ch.get("name", str(ch["id"]))[:25]
        ch_id   = ch["id"]
        has_tpl = has_custom_template(ch_id)
        icon    = "✏️" if has_tpl else "➕"
        rows.append([
            InlineKeyboardButton(
                f"{icon} {name}",
                callback_data=f"ctpl|edit|{ch_id}",
            ),
            InlineKeyboardButton(
                "🗑 Reset",
                callback_data=f"ctpl|reset|{ch_id}",
            ),
        ])
    rows.append([InlineKeyboardButton("❌ Close", callback_data="ctpl|close|0")])
    return InlineKeyboardMarkup(rows)


def _manage_text(channels: list[dict]) -> str:
    lines = [
        "✏️ <b>Caption Templates</b>",
        "──────────────────────",
        "",
        "Set a custom caption for each forward channel.",
        "",
        "<b>Variables you can use:</b>",
        "  <code>{filename}</code>  — file name",
        "  <code>{ext}</code>       — extension (.mp4 …)",
        "  <code>{filesize}</code>  — e.g. 401.8 MB",
        "  <code>{resolution}</code>— e.g. 1080p",
        "  <code>{duration}</code>  — e.g. 24:10",
        "  <code>{audio_langs}</code>— 🇯🇵 🇬🇧",
        "  <code>{sub_langs}</code> — 🇫🇷 🇬🇧",
        "  <code>{codec}</code>     — e.g. H.265/HEVC",
        "  <code>{date}</code>      — 2025-04-01",
        "  <code>{botname}</code>   — bot name",
        "",
    ]
    if not channels:
        lines.append("<i>No forward channels configured.\nAdd channels via /settings → ⚙️ Channels</i>")
    else:
        lines.append("<b>Your channels:</b>")
        for ch in channels:
            name  = ch.get("name", str(ch["id"]))
            ch_id = ch["id"]
            tpl   = get_template(ch_id)
            tpl_s = (tpl[:40] + "…") if len(tpl) > 40 else tpl
            icon  = "✏️" if has_custom_template(ch_id) else "📄 default"
            lines.append(f"  {icon} <b>{name}</b>")
            lines.append(f"  <code>{tpl_s}</code>")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# /captiontemplate command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("captiontemplate"))
async def cmd_captiontemplate(client: Client, msg: Message):
    uid      = msg.from_user.id
    if uid != cfg.owner_id:
        return

    channels = await _channels_with_templates(uid)
    await msg.reply(
        _manage_text(channels),
        reply_markup=_manage_kb(channels),
        parse_mode=enums.ParseMode.HTML,
        disable_web_page_preview=True,
    )


@Client.on_callback_query(filters.regex(r"^ctpl\|"))
async def ctpl_cb(client: Client, cb: CallbackQuery):
    parts  = cb.data.split("|")
    action = parts[1]
    ch_id  = int(parts[2]) if parts[2].lstrip("-").isdigit() else 0
    uid    = cb.from_user.id
    await cb.answer()

    if action == "close":
        _WAITING.pop(uid, None)
        await cb.message.delete()
        return

    if action == "reset":
        delete_template(ch_id)
        channels = await _channels_with_templates(uid)
        await cb.message.edit(
            _manage_text(channels),
            reply_markup=_manage_kb(channels),
            parse_mode=enums.ParseMode.HTML,
        )
        return

    if action == "edit":
        _WAITING[uid] = ch_id
        cur = get_template(ch_id)
        await cb.message.edit(
            f"✏️ <b>Edit Template</b>\n\n"
            f"Channel ID: <code>{ch_id}</code>\n\n"
            f"Current:\n<code>{cur}</code>\n\n"
            f"Send your new template, or send <code>default</code> to reset.\n\n"
            f"<b>Example:</b>\n<code>{EXAMPLE_TEMPLATE}</code>\n\n"
            f"<i>Send /cancel to abort.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return


@Client.on_message(
    filters.private & filters.text
    & ~filters.command([
        "start","help","settings","info","status","log","restart",
        "broadcast","admin","ban_user","unban_user","banned_list",
        "cancel","show_thumb","del_thumb","json_formatter","bulk_url",
        "hardsub","botname","ccstatus","convert","resize","compress",
        "usage","captiontemplate",
        "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
        "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",
    ]),
    group=12,
)
async def ctpl_input_receiver(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _WAITING:
        return

    ch_id = _WAITING.pop(uid)
    text  = msg.text.strip()

    if text.lower() in ("/cancel", "cancel"):
        await msg.reply("❌ Cancelled.")
        msg.stop_propagation()
        return

    if text.lower() == "default":
        delete_template(ch_id)
        await msg.reply(
            f"✅ Template reset to default for channel <code>{ch_id}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        set_template(ch_id, text)
        await msg.reply(
            f"✅ <b>Template saved!</b>\n\n"
            f"Channel: <code>{ch_id}</code>\n\n"
            f"Template:\n<code>{text[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    msg.stop_propagation()
