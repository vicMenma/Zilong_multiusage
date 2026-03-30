"""
plugins/panel.py
Handles inline button callbacks for the live progress panel:
  panel|cancel|<tid>       — cancel one task
  panel|cancel_all|<uid>   — cancel all tasks for uid
  panel|refresh|<uid>      — force panel re-render
  panel|close|<uid>        — delete panel message
"""
from __future__ import annotations

from pyrogram import Client, filters
from pyrogram.types import CallbackQuery

from core.config import cfg
from services.task_runner import runner, tracker, render_panel, render_panel_kb


@Client.on_callback_query(filters.regex(r"^panel\|"))
async def panel_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, action, target = parts
    uid = cb.from_user.id

    # ── Cancel single task ────────────────────────────────────
    if action == "cancel":
        tid = target
        t   = tracker._tasks.get(tid)
        if not t:
            await cb.answer("Task not found or already finished.", show_alert=True)
            return
        fname = (t.fname or t.label)[:30]
        ok    = await runner.cancel_task(tid)
        await cb.answer(
            f"❌ Cancelled: {fname}" if ok else "Task already finished.",
            show_alert=False,
        )
        # Refresh panel
        try:
            from pyrogram import enums
            text = await render_panel(uid)
            kb   = render_panel_kb(uid)
            await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass
        return

    # ── Cancel all ────────────────────────────────────────────
    if action == "cancel_all":
        count = await runner.cancel_all(uid)
        await cb.answer(
            f"❌ Cancelled {count} task(s)." if count else "Nothing to cancel.",
            show_alert=False,
        )
        try:
            from pyrogram import enums
            text = await render_panel(uid)
            kb   = render_panel_kb(uid)
            await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass
        return

    # ── Refresh ───────────────────────────────────────────────
    if action == "refresh":
        try:
            from pyrogram import enums
            text = await render_panel(uid)
            kb   = render_panel_kb(uid)
            await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            await cb.answer("🔄 Refreshed")
        except Exception as exc:
            if "MESSAGE_NOT_MODIFIED" in str(exc):
                await cb.answer("Already up to date.")
            else:
                await cb.answer("Refresh failed.", show_alert=True)
        return

    # ── Close ─────────────────────────────────────────────────
    if action == "close":
        runner.close_panel(uid)
        try:
            await cb.message.delete()
        except Exception:
            pass
        await cb.answer("Panel closed.")
        return

    await cb.answer("Unknown action.", show_alert=True)
