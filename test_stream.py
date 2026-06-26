import os, asyncio, logging
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.file_id import FileId, FileType
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation, InputPhotoFileLocation
from pyrogram.raw.types.upload import File as UploadFile

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

async def test():
    pyro = Client("test_fsb", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
    await pyro.start()
    print("Pyrogram started!")

    # Test file_id from a real file sent to the bot
    # Try decoding a known Bot API file_id format
    test_file_id = "BAACAgUAAx0CZtVppAACAQ9nvy1S3A4xVj3cW6grHRIF0pHIZAACbBgAAjVxIVfXnl4lYqaz-wE"
    
    try:
        decoded = FileId.decode(test_file_id)
        print(f"Decoded: type={decoded.file_type}, dc={decoded.dc_id}, media_id={decoded.media_id}")
        
        thumb = decoded.thumbnail_size or ""
        if decoded.file_type == FileType.PHOTO:
            loc = InputPhotoFileLocation(
                id=decoded.media_id,
                access_hash=decoded.access_hash,
                file_reference=decoded.file_reference,
                thumb_size=thumb,
            )
        else:
            loc = InputDocumentFileLocation(
                id=decoded.media_id,
                access_hash=decoded.access_hash,
                file_reference=decoded.file_reference,
                thumb_size=thumb,
            )
        
        print("Trying to get file chunk...")
        result = await pyro.invoke(GetFile(location=loc, offset=0, limit=1024*1024))
        if isinstance(result, UploadFile):
            print(f"Got chunk: {len(result.bytes)} bytes")
        else:
            print(f"Unexpected result type: {type(result)}")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
    
    await pyro.stop()

asyncio.run(test())
