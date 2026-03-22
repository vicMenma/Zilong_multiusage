"""
plugins/admin.py
Admin-only commands: ban/unban, stats, log, restart, broadcast.

CHANGES:
- Added owner_only_gate (group=-1) — any non-owner gets redirected to @kingkum_1
- Added owner_only_cb_gate for callback queries
- Removed redundant ban check (non-owners can't reach bot at all now)
"""
import asyncio
import os
import sys

from pyrogram import Client, filters, enums
from pyrogram.types import CallbackQuery, Message

from core.config import cfg
from core.session import users


# ── Owner-only gate ───────────────────────────────────────────
# Runs before every other handler. Non-owners get one reply and nothing else.

@Client.on_message(filters.private, group=-1)
async def owner_only_gate(client: Client, msg: Message):
    if not msg.from_user:
        return
    if msg.from_user.id == cfg.owner_id:
        return  # owner passes through
    await msg.reply(
        "🔒 <b>Private Bot</b>\n\n"
        "This bot is for private use only.\n"
        "To request access, contact "
        "<a href='https://t.me/kingkum_1'>@kingkum_1</a>",
        parse_mode=enums.ParseMode.HTML,
        disable_web_page_preview=True,
    )
    msg.stop_propagation()


@Client.on_callback_query(group=-1)
async def owner_only_cb_gate(client: Client, cb: CallbackQuery):
    if cb.from_user.id != cfg.owner_id:
        await cb.answer(
            "🔒 Private bot. Contact @kingkum_1",
            show_alert=True,
        )
        cb.stop_propagation()


# ── Admin filter ──────────────────────────────────────────────
# Kept for forward-compatibility (admin set may grow beyond owner_id).

def _admin_filter(_, __, msg: Message) -> bool:
    return (msg.from_user.id if msg.from_user else 0) in cfg.admins

ADMIN = filters.create(_admin_filter)


# ── Ban gate (group=2) — kept but now only reached by owner ───

@Client.on_message(filters.private, group=2)
async def ban_gate(client: Client, msg: Message):
    if not msg.from_user:
        return
    uid = msg.from_user.id
    if uid in cfg.admins:
        return
    if users.is_banned(uid):
        await msg.reply("🚫 You are banned from using this bot.")
        msg.stop_propagation()


# ── Admin commands ────────────────────────────────────────────

@Client.on_message(filters.command("admin") & ADMIN)
async def cmd_admin(client: Client, msg: Message):
    await msg.reply(
        "<b>Admin Commands</b>\n\n"
        "/ban_user &lt;id&gt;\n"
        "/unban_user &lt;id&gt;\n"
        "/banned_list\n"
        "/stats\n"
        "/log\n"
        "/restart\n"
        "/broadcast (reply to a message)",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("ban_user") & ADMIN)
async def cmd_ban(client: Client, msg: Message):
    args = msg.command[1:]
    if not args:
        return await msg.reply("Usage: /ban_user &lt;id&gt;", parse_mode=enums.ParseMode.HTML)
    try:
        uid = int(args[0])
    except ValueError:
        return await msg.reply("❌ Invalid ID")
    await users.ban(uid)
    await msg.reply(f"✅ <code>{uid}</code> banned.", parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("unban_user") & ADMIN)
async def cmd_unban(client: Client, msg: Message):
    args = msg.command[1:]
    if not args:
        return await msg.reply("Usage: /unban_user &lt;id&gt;", parse_mode=enums.ParseMode.HTML)
    try:
        uid = int(args[0])
    except ValueError:
        return await msg.reply("❌ Invalid ID")
    await users.unban(uid)
    await msg.reply(f"✅ <code>{uid}</code> unbanned.", parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("banned_list") & ADMIN)
async def cmd_banned(client: Client, msg: Message):
    banned = [u for u in users.all_users() if u.banned]
    if not banned:
        return await msg.reply("No banned users.")
    lines = ["<b>Banned Users</b>\n"] + [f"• <code>{u.uid}</code> ({u.name})" for u in banned]
    await msg.reply("\n".join(lines)[:4000], parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("stats") & ADMIN)
async def cmd_stats(client: Client, msg: Message):
    from services.task_runner import runner, render_panel
    total  = users.count()
    banned = sum(1 for u in users.all_users() if u.banned)
    text   = await render_panel(target_uid=None)
    text  += f"\n\n👥 <b>Users:</b> <code>{total}</code>  🚫 <b>Banned:</b> <code>{banned}</code>"
    st     = await msg.reply(text, parse_mode=enums.ParseMode.HTML)
    runner.open_panel(msg.from_user.id, st, target_uid=None)


@Client.on_message(filters.command("status") & filters.private)
async def cmd_status(client: Client, msg: Message):
    from services.task_runner import runner, render_panel
    uid  = msg.from_user.id
    text = await render_panel(target_uid=uid)
    st   = await msg.reply(text, parse_mode=enums.ParseMode.HTML)
    runner.open_panel(uid, st, target_uid=uid)


@Client.on_message(filters.command("log") & ADMIN)
async def cmd_log(client: Client, msg: Message):
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
async def cmd_restart(client: Client, msg: Message):
    await msg.reply("♻️ Restarting…")
    try:
        await client.stop()
    except Exception:
        pass
    os.execv(sys.executable, [sys.executable] + sys.argv)


@Client.on_message(filters.command("broadcast") & ADMIN)
async def cmd_broadcast(client: Client, msg: Message):
    if not msg.reply_to_message:
        return await msg.reply("Reply to a message with /broadcast.")
    bcast  = msg.reply_to_message
    st     = await msg.reply("📡 Broadcasting…")
    sent   = failed = 0
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
