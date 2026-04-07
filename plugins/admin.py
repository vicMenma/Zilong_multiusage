"""
plugins/admin.py
Owner-only gate + whitelist + admin commands.

WHITELIST SYSTEM:
  The gate now checks a persistent whitelist (data/whitelist.json).
  Owner is always allowed. Whitelisted UIDs are allowed. Everyone else is blocked.

  /allow <user_id>   — grant access (owner only)
  /deny  <user_id>   — revoke access (owner only)
  /allowed           — list all whitelisted users (owner only)

  ADMINS env var users are auto-whitelisted on startup.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

from pyrogram import Client, filters, enums
from pyrogram.types import CallbackQuery, Message

from core.config import cfg
from core.session import users

log = logging.getLogger(__name__)

_PRIVATE_MSG = (
    "🔒 <b>Private Bot</b>\n\n"
    "This bot is for private use only.\n"
    "To request access, contact "
    "<a href='https://t.me/kingkum_1'>@kingkum_1</a>"
)

# ─────────────────────────────────────────────────────────────
# Persistent whitelist
# ─────────────────────────────────────────────────────────────

_WL_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "whitelist.json")
)
_whitelist: set[int] = set()


def _load_whitelist() -> None:
    """Load whitelist from disk. Called once at import time."""
    global _whitelist
    # Always include owner + ADMINS env var
    _whitelist = set(cfg.admins)
    try:
        with open(_WL_PATH, encoding="utf-8") as f:
            saved = json.load(f)
        _whitelist.update(int(uid) for uid in saved)
        log.info("[Whitelist] Loaded %d entries", len(_whitelist))
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("[Whitelist] Load error: %s", e)


def _save_whitelist() -> None:
    try:
        os.makedirs(os.path.dirname(_WL_PATH), exist_ok=True)
        with open(_WL_PATH, "w", encoding="utf-8") as f:
            # Save only non-owner / non-env entries so the file stays clean
            extra = _whitelist - set(cfg.admins)
            json.dump(sorted(extra), f, indent=2)
    except Exception as e:
        log.warning("[Whitelist] Save error: %s", e)


def is_allowed(uid: int) -> bool:
    return uid in _whitelist


def whitelist_add(uid: int) -> None:
    _whitelist.add(uid)
    _save_whitelist()


def whitelist_remove(uid: int) -> None:
    _whitelist.discard(uid)
    _save_whitelist()


# Load on import
_load_whitelist()


# ─────────────────────────────────────────────────────────────
# Owner-only gate — fires before every other handler (group=-1)
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private, group=-1)
async def owner_only_gate(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    if is_allowed(msg.from_user.id):
        return   # whitelisted — let through
    await msg.reply(
        _PRIVATE_MSG,
        parse_mode=enums.ParseMode.HTML,
        disable_web_page_preview=True,
    )
    msg.stop_propagation()


@Client.on_callback_query(group=-1)
async def owner_only_cb_gate(_: Client, cb: CallbackQuery) -> None:
    if is_allowed(cb.from_user.id):
        return
    await cb.answer("🔒 Private bot. Contact @kingkum_1", show_alert=True)
    cb.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Admin filter
# ─────────────────────────────────────────────────────────────

def _is_admin(_, __, msg: Message) -> bool:
    return (msg.from_user.id if msg.from_user else 0) in cfg.admins

ADMIN = filters.create(_is_admin)


# ─────────────────────────────────────────────────────────────
# Ban gate (group=2)
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private, group=2)
async def ban_gate(_: Client, msg: Message) -> None:
    if not msg.from_user:
        return
    uid = msg.from_user.id
    if uid in cfg.admins:
        return
    if users.is_banned(uid):
        await msg.reply("🚫 You are banned from using this bot.")
        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# /allow — grant access to a user
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("allow") & filters.user(cfg.owner_id))
async def cmd_allow(client: Client, msg: Message) -> None:
    """
    Usage:
      /allow 123456789
      /allow (as reply to a forwarded message — reads the sender's ID)
    """
    uid: int | None = None

    # Try to get UID from command arg
    args = msg.command[1:]
    if args and args[0].lstrip("-").isdigit():
        uid = int(args[0])

    # Or from the replied-to message
    elif msg.reply_to_message and msg.reply_to_message.forward_from:
        uid = msg.reply_to_message.forward_from.id

    elif msg.reply_to_message and msg.reply_to_message.from_user:
        uid = msg.reply_to_message.from_user.id

    if not uid:
        return await msg.reply(
            "Usage: <code>/allow &lt;user_id&gt;</code>\n"
            "or reply to a forwarded message with /allow",
            parse_mode=enums.ParseMode.HTML,
        )

    whitelist_add(uid)
    await msg.reply(
        f"✅ <b>Access granted</b>\n\n"
        f"User <code>{uid}</code> can now use the bot.\n"
        f"They just need to send /start again.",
        parse_mode=enums.ParseMode.HTML,
    )
    log.info("[Whitelist] Added uid=%d by owner", uid)

    # Optionally notify the user
    try:
        await client.send_message(
            uid,
            "✅ <b>Access granted!</b>\n\n"
            "You can now use this bot. Send /start to begin.",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception:
        pass   # user may have never started the bot


# ─────────────────────────────────────────────────────────────
# /deny — revoke access
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("deny") & filters.user(cfg.owner_id))
async def cmd_deny(client: Client, msg: Message) -> None:
    args = msg.command[1:]
    if not args or not args[0].lstrip("-").isdigit():
        return await msg.reply("Usage: <code>/deny &lt;user_id&gt;</code>",
                               parse_mode=enums.ParseMode.HTML)
    uid = int(args[0])
    if uid in cfg.admins:
        return await msg.reply("❌ Cannot deny an admin.")
    whitelist_remove(uid)
    await msg.reply(
        f"✅ <b>Access revoked</b>\n\nUser <code>{uid}</code> is blocked.",
        parse_mode=enums.ParseMode.HTML,
    )
    log.info("[Whitelist] Removed uid=%d by owner", uid)


# ─────────────────────────────────────────────────────────────
# /allowed — list whitelisted users
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("allowed") & filters.user(cfg.owner_id))
async def cmd_allowed(_: Client, msg: Message) -> None:
    extra = sorted(_whitelist - set(cfg.admins))
    if not extra:
        return await msg.reply(
            "📋 <b>Whitelist</b>\n\nNo extra users — only you and ADMINS env var.",
            parse_mode=enums.ParseMode.HTML,
        )
    lines = ["📋 <b>Whitelisted Users</b>\n"]
    for uid in extra:
        u = users.get(uid)
        name = f" ({u.name})" if u and u.name else ""
        lines.append(f"• <code>{uid}</code>{name}")
    await msg.reply("\n".join(lines)[:4000], parse_mode=enums.ParseMode.HTML)


# ─────────────────────────────────────────────────────────────
# /admin help
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("admin") & ADMIN)
async def cmd_admin(_: Client, msg: Message) -> None:
    await msg.reply(
        "<b>Admin Commands</b>\n\n"
        "/allow &lt;id&gt; — grant bot access\n"
        "/deny &lt;id&gt;  — revoke bot access\n"
        "/allowed        — list whitelisted users\n\n"
        "/ban_user &lt;id&gt;\n"
        "/unban_user &lt;id&gt;\n"
        "/banned_list\n"
        "/stats\n"
        "/log\n"
        "/restart\n"
        "/broadcast (reply to a message)",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# Standard admin commands
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ban_user") & ADMIN)
async def cmd_ban(_: Client, msg: Message) -> None:
    args = msg.command[1:]
    if not args:
        return await msg.reply("Usage: /ban_user &lt;id&gt;",
                               parse_mode=enums.ParseMode.HTML)
    try:
        uid = int(args[0])
    except ValueError:
        return await msg.reply("❌ Invalid ID")
    await users.ban(uid)
    await msg.reply(f"✅ <code>{uid}</code> banned.", parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("unban_user") & ADMIN)
async def cmd_unban(_: Client, msg: Message) -> None:
    args = msg.command[1:]
    if not args:
        return await msg.reply("Usage: /unban_user &lt;id&gt;",
                               parse_mode=enums.ParseMode.HTML)
    try:
        uid = int(args[0])
    except ValueError:
        return await msg.reply("❌ Invalid ID")
    await users.unban(uid)
    await msg.reply(f"✅ <code>{uid}</code> unbanned.", parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("banned_list") & ADMIN)
async def cmd_banned(_: Client, msg: Message) -> None:
    banned = [u for u in users.all_users() if u.banned]
    if not banned:
        return await msg.reply("No banned users.")
    lines = ["<b>Banned Users</b>\n"] + [
        f"• <code>{u.uid}</code> ({u.name})" for u in banned
    ]
    await msg.reply("\n".join(lines)[:4000], parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("stats") & ADMIN)
async def cmd_stats(client: Client, msg: Message) -> None:
    from services.task_runner import render_panel, render_panel_kb
    total  = users.count()
    banned = sum(1 for u in users.all_users() if u.banned)
    text   = await render_panel(target_uid=None)
    text  += (
        f"\n\n👥 <b>Users:</b> <code>{total}</code>  "
        f"🚫 <b>Banned:</b> <code>{banned}</code>"
    )
    await msg.reply(text, parse_mode=enums.ParseMode.HTML,
                    reply_markup=render_panel_kb(msg.from_user.id))


@Client.on_message(filters.command("status") & filters.private)
async def cmd_status(client: Client, msg: Message) -> None:
    from services.task_runner import render_panel, render_panel_kb
    uid  = msg.from_user.id
    text = await render_panel(target_uid=uid)
    await msg.reply(text, parse_mode=enums.ParseMode.HTML,
                    reply_markup=render_panel_kb(uid))


@Client.on_message(filters.command("log") & ADMIN)
async def cmd_log(_: Client, msg: Message) -> None:
    for fname in ("zilong.log", "bot.log"):
        if os.path.exists(fname):
            with open(fname, encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-50:]
            return await msg.reply(
                f"<pre>{''.join(lines)[-3900:]}</pre>",
                parse_mode=enums.ParseMode.HTML,
            )
    await msg.reply("No log file found.")


@Client.on_message(filters.command("restart") & ADMIN)
async def cmd_restart(client: Client, msg: Message) -> None:
    await msg.reply("♻️ Restarting…")
    try:
        await client.stop()
    except Exception:
        pass
    os.execv(sys.executable, [sys.executable] + sys.argv)


@Client.on_message(filters.command("broadcast") & ADMIN)
async def cmd_broadcast(_: Client, msg: Message) -> None:
    if not msg.reply_to_message:
        return await msg.reply("Reply to a message with /broadcast.")
    bcast = msg.reply_to_message
    st    = await msg.reply("📡 Broadcasting…")
    sent  = failed = 0
    for user in users.all_users():
        if user.banned:
            continue
        try:
            await bcast.copy(user.uid)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await st.edit(
        f"✅ Sent: <code>{sent}</code>  ❌ Failed: <code>{failed}</code>",
        parse_mode=enums.ParseMode.HTML,
    )
