import os
import re
import secrets
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse
import uvicorn
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
LOG_CHANNEL = os.getenv("LOG_CHANNEL")
HOST = os.getenv("HOST", "http://localhost:8080")
PORT = int(os.getenv("PORT", 8080))
HASH_LENGTH = int(os.getenv("HASH_LENGTH", 6))
ALLOWED_USERS = [int(x.strip()) for x in os.getenv("ALLOWED_USERS", "").split(",") if x.strip()]

file_registry: dict[str, dict] = {}
user_files: dict[int, list[str]] = {}
http_bot = Bot(token=BOT_TOKEN)
tg_client: TelegramClient | None = None
tg_ready: bool = False

def generate_hash() -> str:
    return secrets.token_urlsafe(HASH_LENGTH)[:HASH_LENGTH]

def format_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"

def extract_file_info(msg) -> dict | None:
    mapping = [
        ("document", ["file_id", "file_name", "mime_type", "file_size"]),
        ("video", ["file_id", "file_name", "mime_type", "file_size"]),
        ("audio", ["file_id", "file_name", "mime_type", "file_size"]),
        ("voice", "voice"),
        ("video_note", "video_note"),
        ("animation", ["file_id", "file_name", "mime_type", "file_size"]),
    ]
    for attr, fields in mapping:
        obj = getattr(msg, attr, None)
        if obj:
            if fields == "voice":
                return {
                    "file_id": obj.file_id,
                    "file_name": f"voice_{obj.file_id[:8]}.ogg",
                    "mime_type": "audio/ogg",
                    "file_size": obj.file_size,
                }
            if fields == "video_note":
                return {
                    "file_id": obj.file_id,
                    "file_name": f"video_note_{obj.file_id[:8]}.mp4",
                    "mime_type": "video/mp4",
                    "file_size": obj.file_size,
                }
            return {
                "file_id": getattr(obj, fields[0]),
                "file_name": getattr(obj, fields[1]) or f"{attr}_{obj.file_id[:8]}",
                "mime_type": getattr(obj, fields[2]) or "application/octet-stream",
                "file_size": getattr(obj, fields[3]),
            }
    if msg.photo:
        photo = msg.photo[-1]
        return {
            "file_id": photo.file_id,
            "file_name": f"photo_{photo.file_id[:8]}.jpg",
            "mime_type": "image/jpeg",
            "file_size": photo.file_size,
        }
    return None

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if ALLOWED_USERS and user.id not in ALLOWED_USERS:
        return

    msg = update.effective_message
    info = extract_file_info(msg)
    if not info:
        await msg.reply_text("Send a file, photo, video, or audio.")
        return

    file_hash = generate_hash()
    while file_hash in file_registry:
        file_hash = generate_hash()

    info["user_id"] = user.id
    file_registry[file_hash] = info

    if user.id not in user_files:
        user_files[user.id] = []
    user_files[user.id].append(file_hash)

    link = f"{HOST}/file/{file_hash}"
    watch_link = f"{HOST}/watch/{file_hash}"
    if info["mime_type"] and info["mime_type"].startswith("video/"):
        link_text = f"🎬 <a href='{watch_link}'>Watch</a> | <code>{link}</code>"
    else:
        link_text = f"🔗 <code>{link}</code>"
    await msg.reply_text(
        f"<b>File received!</b>\n\n"
        f"📄 {info['file_name']}\n"
        f"📦 {format_size(info['file_size'])}\n\n"
        f"{link_text}",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

    if LOG_CHANNEL:
        try:
            await context.bot.send_message(
                chat_id=LOG_CHANNEL,
                text=f"New file: {info['file_name']} ({format_size(info['file_size'])}) by {user.full_name}\n{link}",
            )
        except Exception as e:
            logger.warning(f"Failed to log: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Send me any file and I'll give you a direct streaming link.")
    if tg_ready:
        await update.message.reply_text("MTProto streaming: ready")
    else:
        await update.message.reply_text("MTProto streaming: not connected (some large files may fail)")

async def my_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    hashes = user_files.get(user.id, [])
    if not hashes:
        await update.message.reply_text("You haven't uploaded any files yet.")
        return
    lines = ["<b>Your files:</b>\n"]
    for h in hashes:
        info = file_registry.get(h)
        if info:
            lines.append(f"• {info['file_name']} — {HOST}/file/{h}")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def stream_via_botapi(file_id: str, range_header: str):
    tg_file = await http_bot.get_file(file_id)
    url = tg_file.file_path
    headers = {}
    if range_header:
        headers["Range"] = range_header
    client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    resp = await client.get(url, headers=headers)
    if resp.status_code >= 400:
        await resp.aclose()
        await client.aclose()
        raise HTTPException(resp.status_code, "Upstream error")
    return resp, client

async def stream_via_mtproto(file_id: str, offset: int, limit: int):
    async for chunk in tg_client.iter_download(file_id, offset=offset, limit=limit or 0):
        yield chunk

# FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    global tg_client, tg_ready

    polling_app = Application.builder().token(BOT_TOKEN).build()
    polling_app.add_handler(CommandHandler("start", start))
    polling_app.add_handler(CommandHandler("myfiles", my_files))
    polling_app.add_handler(MessageHandler(filters.ATTACHMENT, file_handler))
    await polling_app.initialize()
    await polling_app.start()
    await polling_app.updater.start_polling()
    logger.info("Bot started polling")

    tg_client = TelegramClient("fsb_session", API_ID, API_HASH)
    try:
        await tg_client.start(bot_token=BOT_TOKEN)
        me = await tg_client.get_me()
        tg_ready = True
        logger.info(f"Telethon ready as @{me.username}")
    except Exception as e:
        logger.warning(f"Telethon not available (flood wait?): {e}")
        logger.warning("Falling back to Bot API for streaming (20MB limit)")

    yield

    if tg_client:
        await tg_client.disconnect()
    await polling_app.updater.stop()
    await polling_app.stop()
    await polling_app.shutdown()

app = FastAPI(title="File Stream Bot", lifespan=lifespan)

RANGE_RE = re.compile(r"bytes=(\d+)-(\d*)")

WATCH_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} - File Stream</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;display:flex;justify-content:center;padding:2rem 1rem}}
  .container{{width:100%;max-width:960px}}
  .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;overflow:hidden}}
  .player-wrap{{background:#000;position:relative}}
  video{{display:block;width:100%;max-height:80vh;outline:none}}
  .info{{padding:1.25rem 1.5rem}}
  .name{{font-size:1.1rem;font-weight:600;margin-bottom:.35rem;word-break:break-word}}
  .meta{{font-size:.85rem;color:#8b949e;display:flex;gap:1rem;flex-wrap:wrap}}
  .meta span{{display:inline-flex;align-items:center;gap:.35rem}}
  .actions{{margin-top:1rem;display:flex;gap:.5rem}}
  .btn{{display:inline-flex;align-items:center;gap:.4rem;padding:.5rem 1rem;border-radius:6px;font-size:.85rem;font-weight:500;text-decoration:none;border:1px solid #30363d;background:#21262d;color:#e6edf3;transition:background .15s}}
  .btn:hover{{background:#30363d}}
  .btn-dl{{background:#1f6feb;border-color:#1f6feb;color:#fff}}
  .btn-dl:hover{{background:#388bfd}}
</style>
</head>
<body>
<div class="container">
  <div class="card">
    <div class="player-wrap">
      <video controls preload="metadata" playsinline webkit-playsinline>
        <source src="{stream_url}" type="{mime}">
      </video>
    </div>
    <div class="info">
      <div class="name">{name}</div>
      <div class="meta">
        <span><svg width="16" height="16" fill="currentColor" viewBox="0 0 16 16"><path d="M1 3.5A1.5 1.5 0 0 1 2.5 2h2.764c.958 0 1.76.56 2.311 1.147C7.99 3.693 8.779 4 10 4h4.5A1.5 1.5 0 0 1 16 5.5v7a1.5 1.5 0 0 1-1.5 1.5h-11A1.5 1.5 0 0 1 2 12.5v-9Z"/></svg> {type_label}</span>
        <span><svg width="16" height="16" fill="currentColor" viewBox="0 0 16 16"><path d="M9 4a.5.5 0 0 0-.5-.5H2.5A1.5 1.5 0 0 0 1 5v9.5A1.5 1.5 0 0 0 2.5 16h10a1.5 1.5 0 0 0 1.5-1.5V9.5a.5.5 0 0 0-1 0v6a.5.5 0 0 1-.5.5h-10a.5.5 0 0 1-.5-.5V5a.5.5 0 0 1 .5-.5h6A.5.5 0 0 0 9 4Z"/><path d="M14.854.146a.5.5 0 0 0-.707 0L10.5 3.793 8.854 2.146a.5.5 0 0 0-.708.708l2 2a.5.5 0 0 0 .708 0l4.5-4.5a.5.5 0 0 0 .002-.708Z"/></svg> {size}</span>
      </div>
      <div class="actions">
        <a href="{direct_url}" class="btn btn-dl" download>Download</a>
        <a href="{direct_url}" class="btn" target="_blank">Direct Link</a>
      </div>
    </div>
  </div>
</div>
</body>
</html>'''

def mime_type_label(mime: str) -> str:
    if not mime: return "File"
    if mime.startswith("video/"): return "Video"
    if mime.startswith("audio/"): return "Audio"
    if mime.startswith("image/"): return "Image"
    if mime == "application/pdf": return "PDF"
    parts = mime.split("/")
    return parts[-1].upper() if len(parts) > 1 else "File"

@app.get("/watch/{file_hash}")
async def watch_page(file_hash: str):
    info = file_registry.get(file_hash)
    if not info:
        raise HTTPException(404, "File not found")
    stream_url = f"{HOST}/file/{file_hash}"
    page = WATCH_HTML.format(
        name=info["file_name"],
        size=format_size(info["file_size"]),
        mime=info["mime_type"],
        type_label=mime_type_label(info["mime_type"]),
        stream_url=stream_url,
        direct_url=stream_url,
    )
    return HTMLResponse(page)

@app.get("/file/{file_hash}")
async def stream_file(request: Request, file_hash: str):
    info = file_registry.get(file_hash)
    if not info:
        raise HTTPException(404, "File not found or expired")

    file_size = info["file_size"]
    file_id = info["file_id"]
    range_header = request.headers.get("range", "")

    match = RANGE_RE.match(range_header)
    status = 200
    start = 0
    end = file_size - 1
    content_range = None

    if match:
        status = 206
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else (file_size - 1)
        if start >= file_size:
            raise HTTPException(416, "Range not satisfiable")
        content_range = f"bytes {start}-{end}/{file_size}"

    response_headers = {
        "Content-Disposition": f'inline; filename="{info["file_name"]}"',
        "Accept-Ranges": "bytes",
        "Content-Type": info["mime_type"],
    }
    if content_range:
        response_headers["Content-Range"] = content_range
        response_headers["Content-Length"] = str(end - start + 1)
    elif file_size:
        response_headers["Content-Length"] = str(file_size)

    # Try MTProto first if ready (no file size limit)
    if tg_ready:
        async def mtproto_stream():
            try:
                async for chunk in tg_client.iter_download(
                    file_id, offset=start, limit=(end - start + 1) if match else 0
                ):
                    yield chunk
            except Exception as e:
                logger.error(f"MTProto stream error: {e}")
                raise

        return StreamingResponse(
            mtproto_stream(),
            status_code=status,
            headers=response_headers,
        )

    # Fallback: Bot API (httpx) — works for files up to 20MB
    try:
        tg_file = await http_bot.get_file(file_id)
    except Exception as e:
        raise HTTPException(502, f"Cannot get file from Telegram: {e}")

    url = tg_file.file_path
    headers = {}
    if match:
        headers["Range"] = range_header

    client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    resp = await client.get(url, headers=headers)

    if resp.status_code >= 400:
        await resp.aclose()
        await client.aclose()
        raise HTTPException(resp.status_code, "Upstream error")

    # Override with actual headers from Telegram CDN
    cl = resp.headers.get("content-length")
    cr = resp.headers.get("content-range")
    ct = resp.headers.get("content-type")
    if cl:
        response_headers["Content-Length"] = cl
    if cr:
        response_headers["Content-Range"] = cr
    if ct:
        response_headers["Content-Type"] = ct

    async def http_stream():
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    return StreamingResponse(
        http_stream(),
        status_code=resp.status_code,
        headers=response_headers,
    )

@app.get("/")
async def index():
    return {"status": "ok", "files": len(file_registry), "users": len(user_files),
            "mtproto": tg_ready}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
