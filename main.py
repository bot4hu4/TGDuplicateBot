from pyrogram import Client, filters  # type: ignore
from pyrogram.errors import FloodWait
from pyrogram.types import Message
import asyncio
from collections import defaultdict

API_ID = 13691707
API_HASH = '2a31b117896c5c7da27c74025aa602b8'
BOT_TOKEN = '8614262779:AAG8XK-JL8aGrWSPNSdaAQ9u4km00Noury4'

app = Client("DEX-DUP", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_media_info(msg: Message) -> tuple[str, int] | None:
    """Return (media_type, file_size) for grouping, or None if not media."""
    if getattr(msg, 'empty', False) or not msg.media:
        return None
    if msg.photo:      return ('photo',     msg.photo.file_size)
    if msg.video:      return ('video',     msg.video.file_size)
    if msg.document:   return ('document',  msg.document.file_size)
    if msg.audio:      return ('audio',     msg.audio.file_size)
    if msg.voice:      return ('voice',     msg.voice.file_size)
    if msg.animation:  return ('animation', msg.animation.file_size)
    if msg.sticker:    return ('sticker',   msg.sticker.file_size)
    return None


async def _anext_or_none(agen):
    """
    Safely advance an async generator, returning None when exhausted.
    No asyncio.wait_for — cancelling stream_media mid-flight corrupts
    its internal DC connection and breaks the generator state.
    """
    try:
        return await agen.__anext__()
    except StopAsyncIteration:
        return None


async def compare_streaming(client: Client, msg1: Message, msg2: Message) -> bool:
    """
    Stream both files chunk by chunk in parallel.
    Returns True only if every chunk matches — bails on first mismatch,
    so in the best case only one chunk is downloaded from each file.
    """
    try:
        g1 = client.stream_media(msg1)
        g2 = client.stream_media(msg2)

        chunk_num = 0
        while True:
            chunk_num += 1
            # Fetch sequentially — concurrent stream_media calls fight over
            # Pyrogram's internal session lock and deadlock each other.
            c1 = await _anext_or_none(g1)
            c2 = await _anext_or_none(g2)

            if c1 is None and c2 is None:
                print(f"  [msg {msg1.id} vs {msg2.id}] All {chunk_num - 1} chunk(s) matched → DUPLICATE")
                return True

            if c1 is None or c2 is None:
                print(f"  [msg {msg1.id} vs {msg2.id}] Stream length mismatch at chunk {chunk_num} → NOT duplicate")
                return False

            if c1 != c2:
                print(f"  [msg {msg1.id} vs {msg2.id}] Chunk {chunk_num} differs ({len(c1)} bytes) → NOT duplicate")
                return False

            print(f"  [msg {msg1.id} vs {msg2.id}] Chunk {chunk_num} OK ({len(c1):,} bytes)")

    except Exception as e:
        print(f"  [msg {msg1.id} vs {msg2.id}] Error: {type(e).__name__}: {e}")
        return False


# ── Bot command ───────────────────────────────────────────────────────────────

@app.on_message(filters.command("clear"))
async def clear_duplicate_handler(client: Client, m: Message):
    try:
        spl = m.text.split()
        if len(spl) != 4:
            return await m.reply("Usage: `/clear chat_id st_msg en_msg`")
        cid = int(spl[1])
        st  = int(spl[2])
        en  = int(spl[3])
    except ValueError:
        return await m.reply("Invalid arguments. Usage: `/clear chat_id st_msg en_msg`")

    status_msg = await m.reply(f"Fetching messages {st}–{en}...")

    # ── 1. Fetch all messages in range ────────────────────────────────────────
    mrange = list(range(st, en + 1))
    mlist: list[Message] = []

    for i in range(0, len(mrange), 200):
        chunk = mrange[i:i + 200]
        try:
            msgs = await client.get_messages(cid, chunk)
            if not isinstance(msgs, list):
                msgs = [msgs]
            mlist.extend(msg for msg in msgs if msg)
        except FloodWait as e:
            await asyncio.sleep(float(e.value))  # type: ignore
            msgs = await client.get_messages(cid, chunk)
            if not isinstance(msgs, list):
                msgs = [msgs]
            mlist.extend(msg for msg in msgs if msg)
        except Exception as e:
            print(f"Error fetching chunk {chunk[0]}-{chunk[-1]}: {e}")

    await status_msg.edit(f"Fetched {len(mlist)} messages. Grouping by type + size...")

    # ── 2. Group by (media_type, file_size) ───────────────────────────────────
    groups: dict[tuple[str, int], list[Message]] = defaultdict(list)
    for msg in mlist:
        info = get_media_info(msg)
        if info and info[1]:
            groups[info].append(msg)

    candidates = {k: v for k, v in groups.items() if len(v) > 1}

    if not candidates:
        return await status_msg.edit(
            "No files with matching type and size — nothing to compare."
        )

    total = sum(len(v) for v in candidates.values())
    await status_msg.edit(
        f"Found {total} file(s) across {len(candidates)} group(s) to stream-compare..."
    )

    # ── 3. Stream-compare chunk by chunk within each group ────────────────────
    duplicates_to_delete: list[int] = []

    for (mtype, fsize), group in candidates.items():
        print(f"\n── Group: {mtype}, {fsize:,} bytes ({len(group)} messages) ──")
        unique_msgs: list[Message] = [group[0]]

        for msg in group[1:]:
            is_dup = False
            for u_msg in unique_msgs:
                print(f" Comparing msg {msg.id} vs msg {u_msg.id} ...")
                if await compare_streaming(client, u_msg, msg):
                    duplicates_to_delete.append(msg.id)
                    is_dup = True
                    break
            if not is_dup:
                unique_msgs.append(msg)

    # ── 4. Delete duplicates ──────────────────────────────────────────────────
    if not duplicates_to_delete:
        return await status_msg.edit("No byte-for-byte exact duplicates found.")

    await status_msg.edit(
        f"Found {len(duplicates_to_delete)} duplicate(s). Deleting..."
    )

    try:
        for i in range(0, len(duplicates_to_delete), 100):
            chunk = duplicates_to_delete[i:i + 100]
            await client.delete_messages(cid, chunk)
            await asyncio.sleep(1)

        await status_msg.edit(
            f"🎉 Done! Deleted {len(duplicates_to_delete)} duplicate message(s)."
        )
    except Exception as e:
        await status_msg.edit(f"Error during deletion: {e}")


if __name__ == '__main__':
    print("Starting duplicate bot...")
    app.run()