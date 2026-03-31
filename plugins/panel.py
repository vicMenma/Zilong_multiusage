"""
plugins/panel.py
Handles inline button callbacks for the /status panel.
  panel|cancel|<tid>
  panel|cancel_all|<uid>
  panel|refresh|<uid>
  panel|close|<uid>
"""
from __future__ import annotations

from pyrogram import Client, filters
from pyrogram.types import CallbackQuery

from services.task_runner import render_panel, render_panel_kb, runner, tracker


@Client.on_callback_query(filters.regex(r"^panel\|"))
async def panel_cb(client: Client, cb: CallbackQuery) -> None:
    parts = cb.data.split("|", 2)
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, action, target = parts
    uid = cb.from_user.id

    if action == "cancel":
        t = tracker._tasks.get(target)
        if not t:
            return await cb.answer("Task not found or already finished.", show_alert=True)
        fname = (t.fname or t.label)[:30]
        ok    = await runner.cancel_task(target)
        await cb.answer(
            f"❌ Cancelled: {fname}" if ok else "Task already finished.",
        )

    elif action == "cancel_all":
        count = await runner.cancel_all(uid)
        await cb.answer(
            f"❌ Cancelled {count} task(s)." if count else "Nothing to cancel.",
        )

    elif action == "refresh":
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

    elif action == "close":
        try:
            await cb.message.delete()
        except Exception:
            pass
        await cb.answer("Panel closed.")
        return

    else:
        return await cb.answer("Unknown action.", show_alert=True)

    # Refresh after cancel actions
    try:
        from pyrogram import enums
        text = await render_panel(uid)
        kb   = render_panel_kb(uid)
        await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
    except Exception:
        pass
