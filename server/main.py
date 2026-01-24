import hashlib
import hmac
import json
import logging
from pathlib import Path
from typing import Any, Optional
from bot_client import BotClient
from config import conf_logger, settings
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
import asyncio
from pyrus_api_service import get_token_manager
from utils import download_files, find_value, open_chat, send_message_to_telegram_chat, upload_file_to_pyrus, add_comment_with_attachment
from contextlib import asynccontextmanager
from word_processor import process_word_template, get_director_data, extract_field_value

logger = logging.getLogger(__name__)

count_triggered = 1

webhook_queue = asyncio.Queue(maxsize=500)  # type: ignore

def require(condition: str | int | bytes, msg: str, status_code: int = 500):
    if not condition:
        logger.error(msg)
        raise HTTPException(status_code=status_code, detail=msg)

def create_file_payload(data: dict[str, Any]):
    """Creates a data folder and a json file from the request body"""
    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / "payload.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def verify_signature(header_sig: Optional[str], body: bytes):
    """Verifies webhook signature using HMAC-SHA1 with the secret key.
    Accepts optional 'sha1=' prefix in header.
    """
    if not header_sig:
        logging.debug("No signature header provided")
        return False

    try:
        expected_sig = hmac.new(
            settings.SECURITY_KEY.encode("utf-8"), body, hashlib.sha1
        ).hexdigest()
        return hmac.compare_digest(header_sig.lower(), expected_sig.lower())
    except Exception:
        return False


async def webhook_worker():
    """Background worker to process webhooks from the queue."""
    while True:

        logger.info("Webhook worker activated.")

        body, sig, retry, user_agent = await webhook_queue.get() # type: ignore

        try:
            await process_webhook(body, sig, retry, user_agent)  # type: ignore
        except Exception as e:
            logger.exception("Error processing webhook: %s", e)
        finally:
            webhook_queue.task_done()


@asynccontextmanager 
async def lifespan(app: FastAPI):
    """Lifespan context manager to start the webhook worker on app startup."""
    worker_task = asyncio.create_task(webhook_worker())
    app.state.worker_task = worker_task
    logger.info("Starting webhook worker.")
    try:
        yield
    finally:
        logger.info("Shutting down webhook worker.")
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            logger.info("Webhook worker cancelled.")


app = FastAPI(title="Pyrus Webhook (FastAPI)", lifespan=lifespan)

@app.post("/webhook")
async def pyrus_webhook(
    request: Request,
    x_pyrus_sig: Optional[str] = Header(None, alias="X-Pyrus-Sig"),
    x_pyrus_retry: Optional[str] = Header(None, alias="X-Pyrus-Retry"),
    user_agent: Optional[str] = Header(None, alias="User-Agent"),
):
    """Receives incoming POST requests from Pyrus and queues them for processing."""
    body = await request.body()

    if not user_agent or not user_agent.startswith("Pyrus-Bot-"):
        logger.error("Unexpected User-Agent: %s", user_agent)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Bad User-Agent"
        )

    if not verify_signature(x_pyrus_sig, body):
        logger.error("Invalid or missing X-Pyrus-Sig header")
        raise HTTPException(status_code=500, detail="Request body is missing")

    logger.info("Webhook received and queued for processing.")

    await webhook_queue.put((body, x_pyrus_sig, x_pyrus_retry, user_agent))  # type: ignore


async def process_webhook(
    body: bytes,
    x_pyrus_sig: Optional[str] = Header(None, alias="X-Pyrus-Sig"),
    x_pyrus_retry: Optional[str] = Header(None, alias="X-Pyrus-Retry"),
    user_agent: Optional[str] = Header(None, alias="User-Agent"),
):
    """
    Processes incoming POST requests from Pyrus and the last event and the last comment.
    """

    print("processing...")
    
    require(body, "No body was found during request processing.")

    try:
        data = json.loads(body)
    except Exception:
        logger.exception("Error when parsing JSON")
        raise HTTPException(status_code=422, detail="Incorrect JSON")

    task = data.get("task", {})

    access_token = data.get("access_token", {})

    fields = task.get("fields", [])

    require(task, "Task not found.")

    require(access_token, "token not found")

    require(isinstance(fields, list) and fields, "fields not found")

    comments = task.get("comments")

    require(comments, "No comments in task")

    require(fields, "Unexpected User-Agent")

    try:
        create_file_payload(data)
    except Exception:
        logger.exception("Error when creating the file with request body")

    event = data.get("event", {})

    require(event, "No event field in payload")

    task_id = data.get("task_id", {})

    require(task_id, "No task_id field in payload")

    global count_triggered

    logger.info(f"Received webhook for task #{task_id} from Pyrus #{count_triggered}")

    count_triggered += 1

    create_date = task.get("create_date", {})

    require(create_date, "No create_date field in payload")

    try:
        token = await get_token_manager().get_token()
        print("Задача успешно получена")
        
        lead_task_id = None
        for field in fields:
            if field.get("name") == "Новый Лид" and field.get("type") == "form_link":
                value = field.get("value")
                if isinstance(value, dict):
                    lead_task_id = value.get("task_id")
                else:
                    lead_task_id = value
                break

        logger.info(f"Lead task id: {lead_task_id}")
        
        director_fio = "ВЫ НЕ УКАЗАЛИ ДИРЕКТОРА"
        is_general_director = False
        if lead_task_id:
            director_fio, is_general_director = await get_director_data(lead_task_id)
        
        template_path = Path("Шаблон.docx")
        output_path = Path("data") / f"output_{task_id}.docx"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Processing Word template: {template_path} -> {output_path}")
        process_word_template(template_path, output_path, fields, director_fio, is_general_director)
        logger.info(f"Word document generated successfully: {output_path}")
        
        # Загружаем файл в Pyrus
        filename = "Спецификация.docx"
        logger.info(f"Uploading file {output_path} to Pyrus as '{filename}'")
        file_guid = await upload_file_to_pyrus(output_path, filename)
        
        if file_guid:
            logger.info(f"File uploaded successfully with GUID: {file_guid}")
            # Прикрепляем файл к задаче и удаляем бота из наблюдателей
            subscribers_removed = [{"id": 1239059}]
            success = await add_comment_with_attachment(task_id, "", file_guid, subscribers_removed)
            if success:
                logger.info(f"File attached successfully to task {task_id} and bot removed from subscribers")
            else:
                logger.error(f"Failed to attach file to task {task_id}")
        else:
            logger.error(f"Failed to upload file to Pyrus")

    except Exception as e:
        logger.exception('Error processing webhook')
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"error": str(e)})

    return JSONResponse(status_code=status.HTTP_200_OK, content={})


if __name__ == "__main__":
    import uvicorn
    conf_logger()
    logger.info("Server started.")
    uvicorn.run("server.main:app", host="127.0.0.1", port=8000)
