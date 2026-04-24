"""
plugins/panel.py
Handles inline button callbacks for the /status panel.
  panel|cancel|<tid>
  panel|cancel_all|<uid>
  panel|refresh|<uid>
  panel|close|<uid>
"""
from __future__ import annotations

from pyrogram import Client, filters, enums
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
            await cb.answer("⚠️ Task not found or already finished.", show_alert=True)
            # Still refresh panel to show updated state
        else:
            fname = (t.fname or t.label)[:30]
            if t.is_terminal:
                await cb.answer(f"Task #{t.seq} already finished.", show_alert=True)
            else:
                ok = await runner.cancel_task(target)
                if ok:
                    await cb.answer(f"❌ Cancelled #{t.seq}: {fname}", show_alert=False)
                else:
                    await cb.answer("⚠️ Could not cancel — task may have just finished.")

    elif action == "cancel_all":
        count = await runner.cancel_all(uid)
        if count:
            await cb.answer(f"❌ Cancelled {count} task(s).")
        else:
            await cb.answer("Nothing active to cancel.")

    elif action == "refresh":
        try:
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
        await cb.answer()
        return

    else:
        return await cb.answer("Unknown action.", show_alert=True)

    # Refresh panel after any cancel action
    try:
        text = await render_panel(uid)
        kb   = render_panel_kb(uid)
        await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
    except Exception:
        pass
