import hashlib
import hmac
import html
import json
import logging
from collections import deque
from pathlib import Path
from typing import Any, Optional
from config import conf_logger, settings
from fastapi import FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
import asyncio
from pyrus_api_service import api_request, get_token_manager
from utils import (
    download_files,
    find_value,
    open_chat,
    send_message_to_telegram_chat,
    upload_file_to_pyrus,
    send_comment_in_pyrus,
)
from contextlib import asynccontextmanager
from word_processor import process_word_template, get_director_data, extract_field_value

logger = logging.getLogger(__name__)

PYRUS_DELETE_LINKED_FORM_ID = 1562280

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


async def _collect_tasks_in_linked_graph(root_task_id: int) -> list[int]:
    """Обходит граф связанных задач через linked_task_ids (BFS)."""
    visited: set[int] = set()
    order: list[int] = []
    queue: deque[int] = deque([root_task_id])
    while queue:
        tid = queue.popleft()
        if tid in visited:
            continue
        visited.add(tid)
        order.append(tid)
        try:
            data = await api_request("GET", endpoint=f"/tasks/{tid}")
        except Exception as e:
            logger.warning("GET /tasks/%s failed while collecting linked graph: %s", tid, e)
            continue
        if not isinstance(data, dict):
            continue
        task = data.get("task") or {}
        for lid in task.get("linked_task_ids") or []:
            try:
                lid_int = int(lid)
            except (TypeError, ValueError):
                continue
            if lid_int not in visited:
                queue.append(lid_int)
    return order


@app.get("/delete", response_class=HTMLResponse)
async def delete_task_and_linked(task_id: int = Query(..., ge=1)):
    """
    Удаляет связанные задачи с form_id=1562280, затем основную задачу.
    Вызывается из кнопки на форме Pyrus: GET /delete?task_id=...
    """
    await get_token_manager().get_token()

    try:
        root_data = await api_request("GET", endpoint=f"/tasks/{task_id}")
    except Exception as e:
        logger.exception("Failed to fetch root task %s", task_id)
        return HTMLResponse(
            _delete_result_html(False, [], [], [f"Не удалось загрузить задачу {task_id}: {e}"]),
            status_code=502,
        )

    if not isinstance(root_data, dict) or not root_data.get("task"):
        return HTMLResponse(
            _delete_result_html(False, [], [], [f"Задача {task_id} не найдена"]),
            status_code=404,
        )

    all_in_graph = await _collect_tasks_in_linked_graph(task_id)
    related_ids = [tid for tid in all_in_graph if tid != task_id]

    linked_will_delete: list[int] = []
    errors: list[str] = []

    for rid in related_ids:
        try:
            tdata = await api_request("GET", endpoint=f"/tasks/{rid}")
        except Exception as e:
            errors.append(f"Задача {rid}: не удалось загрузить ({e})")
            continue
        task = (tdata or {}).get("task") or {}
        form_id = task.get("form_id")
        if form_id == PYRUS_DELETE_LINKED_FORM_ID:
            linked_will_delete.append(rid)

    deleted_linked: list[int] = []

    for rid in linked_will_delete:
        try:
            del_resp = await api_request("DELETE", endpoint=f"/tasks/{rid}")
        except Exception as e:
            errors.append(f"Задача {rid}: удаление не выполнено ({e})")
            continue
        if isinstance(del_resp, dict) and del_resp.get("deleted") is True:
            deleted_linked.append(rid)
        else:
            errors.append(f"Задача {rid}: неожиданный ответ API при удалении")

    try:
        root_del = await api_request("DELETE", endpoint=f"/tasks/{task_id}")
    except Exception as e:
        errors.append(f"Основная задача {task_id}: удаление не выполнено ({e})")
        return HTMLResponse(
            _delete_result_html(False, deleted_linked, [], errors),
            status_code=502,
        )

    root_ok = isinstance(root_del, dict) and root_del.get("deleted") is True
    if not root_ok:
        errors.append(f"Основная задача {task_id}: неожиданный ответ API при удалении")

    return HTMLResponse(
        _delete_result_html(root_ok, deleted_linked, [task_id] if root_ok else [], errors),
        status_code=status.HTTP_200_OK if root_ok else status.HTTP_502_BAD_GATEWAY,
    )


def _id_chips(ids: list[int]) -> str:
    if not ids:
        return '<p class="muted">—</p>'
    chips = "".join(
        f'<span class="chip">{html.escape(str(i))}</span>' for i in ids
    )
    return f'<div class="id-list">{chips}</div>'


def _delete_result_html(
    success: bool,
    deleted_linked: list[int],
    deleted_root: list[int],
    errors: list[str],
) -> str:
    has_errors = bool(errors)
    if success and not has_errors:
        title = "Все задачи удалены"
        lead = "Операция завершена успешно."
    elif success and has_errors:
        title = "Удаление завершено частично"
        lead = "Часть действий выполнена; ниже список успешно удалённых id и замечания."
    else:
        title = "Ошибка"
        lead = "Удаление не завершено полностью."

    badge = "#16a34a" if success and not has_errors else ("#ca8a04" if success else "#dc2626")
    parts: list[str] = [f'<p class="lead">{html.escape(lead)}</p>']

    all_deleted = list(deleted_linked) + list(deleted_root)
    if success and all_deleted:
        parts.append(
            f'<p class="summary">Всего удалено: <strong>{len(all_deleted)}</strong></p>'
        )

    parts.append(
        f'<h2>Связанные (форма {PYRUS_DELETE_LINKED_FORM_ID})</h2>'
        f"{_id_chips(deleted_linked)}"
    )
    parts.append("<h2>Основная задача</h2>" + _id_chips(deleted_root))

    if success and not deleted_linked and deleted_root:
        parts.insert(
            2,
            '<p class="muted">Связанных задач с нужной формой не было — удалена только основная.</p>',
        )

    if errors:
        err_html = "<br/>".join(html.escape(e) for e in errors)
        parts.append(f'<p class="err"><strong>Замечания:</strong><br/>{err_html}</p>')
    body = "".join(parts)
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{html.escape(title)}</title>
<style>
body {{ font-family: system-ui, Segoe UI, Roboto, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; }}
.card {{ background: #1e293b; border-radius: 12px; padding: 28px 32px; max-width: 640px; width: 100%; box-shadow: 0 10px 40px rgba(0,0,0,.35); border: 1px solid #334155; }}
h1 {{ margin: 0 0 8px; font-size: 1.35rem; display: flex; align-items: center; gap: 10px; }}
h2 {{ margin: 22px 0 10px; font-size: 0.85rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #94a3b8; }}
.badge {{ display: inline-block; width: 10px; height: 10px; border-radius: 999px; background: {badge}; flex-shrink: 0; }}
.lead {{ margin: 0 0 8px; color: #cbd5e1; font-size: 0.98rem; line-height: 1.5; }}
.summary {{ margin: 12px 0 0; font-size: 0.95rem; }}
.muted {{ margin: 8px 0 0; color: #64748b; font-size: 0.9rem; }}
.id-list {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 4px 0 0; }}
.chip {{ display: inline-flex; align-items: center; padding: 8px 14px; border-radius: 10px; background: #334155; border: 1px solid #475569; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.92rem; color: #f1f5f9; }}
.err {{ color: #fecaca; margin-top: 18px; font-size: 0.92rem; line-height: 1.45; }}
</style>
</head>
<body>
<div class="card">
<h1><span class="badge"></span>{html.escape(title)}</h1>
{body}
</div>
</body>
</html>"""


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
            # Обновляем поле спецификации (id: 5) через field_updates
            subscribers_removed = [{"id": 1239059}]
            json_data = {
                "field_updates": [
                    {
                        "id": 5,
                        "value": [
                            {
                                "guid": file_guid,
                            }
                        ],
                    }
                ],
                "subscribers_removed": subscribers_removed,
            }

            try:
                await send_comment_in_pyrus(task_id, json_data)
                logger.info(
                    f"Specification file GUID set to field id 5 for task {task_id} "
                    "and bot removed from subscribers"
                )
            except Exception as e:
                logger.error(f"Failed to update field 5 with specification GUID for task {task_id}: {e}")
        else:
            logger.error(f"Failed to upload file to Pyrus")
        
        # Удаляем временный файл после загрузки
        try:
            if output_path.exists():
                output_path.unlink()
                logger.info(f"Temporary file {output_path} deleted successfully")
        except Exception as e:
            logger.warning(f"Failed to delete temporary file {output_path}: {e}")

    except Exception as e:
        logger.exception('Error processing webhook')
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"error": str(e)})

    return JSONResponse(status_code=status.HTTP_200_OK, content={})


if __name__ == "__main__":
    import uvicorn
    conf_logger()
    logger.info("Server started.")
    uvicorn.run("server.main:app", host="127.0.0.1", port=8080)
