from typing import Any, Dict, NoReturn
from cache import request_cache
from contextlib import asynccontextmanager

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from fastapi import FastAPI, Depends, BackgroundTasks

from models import ProcessedRequest
from worker_manager import worker_manager
from database import get_db, create_tables
from utils import create_conflict_exception, create_server_error_exception


def _remember_row_and_conflict(
    request_id: str, row: ProcessedRequest, source: str
) -> NoReturn:
    request_cache.set(request_id, row.cache_dict())
    raise create_conflict_exception(
        request_id=request_id,
        worker_id=row.worker_id,
        created_at=row.created_at.isoformat(),
        source=source,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    create_tables()
    print("Database tables created successfully")
    print(
        f"Starting Request Dispatcher API with {worker_manager.num_workers} workers"
    )
    yield
    # Shutdown (if needed)


app = FastAPI(
    title="Request Dispatcher API",
    description=(
        "A round-robin load balancer API that processes requests only once"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


class ProcessRequestBody(BaseModel):
    request_id: str = Field(..., min_length=1)
    payload: Dict[str, Any]

    @field_validator("request_id", mode="before")
    @classmethod
    def strip_request_id(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip()
        return v


class ProcessRequestResponse(BaseModel):
    message: str
    request_id: str
    worker_id: int
    created_at: str


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/process-request", response_model=ProcessRequestResponse)
async def process_request(
    request_body: ProcessRequestBody,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Process a request using round-robin worker assignment.
    Prevents duplicate processing of the same request_id.
    """
    request_id = request_body.request_id
    payload = request_body.payload

    # First-level check: Look in cache for request_id
    cached_request = request_cache.get(request_id)
    if cached_request:
        # Request found in cache, return cached info
        raise create_conflict_exception(
            request_id=request_id,
            worker_id=cached_request["worker_id"],
            created_at=cached_request["created_at"],
            source="cache"
        )

    # Second-level check: Look in database if not found in cache
    stmt = select(ProcessedRequest).where(
        ProcessedRequest.request_id == request_id
    )
    existing_request = db.scalars(stmt).first()

    if existing_request:
        _remember_row_and_conflict(request_id, existing_request, "database")

    # Get next worker using round-robin
    worker_id = worker_manager.get_next_worker()

    # Create new request record
    new_request = ProcessedRequest(
        request_id=request_id, worker_id=worker_id
    )
    new_request.set_payload(payload)

    # Save to database
    try:
        db.add(new_request)
        db.commit()
        db.refresh(new_request)

        request_cache.set(request_id, new_request.cache_dict())

        # Process request in background
        background_tasks.add_task(worker_manager.process_request, request_id)

        return ProcessRequestResponse(
            message="Request queued for processing",
            request_id=request_id,
            worker_id=worker_id,
            created_at=new_request.created_at.isoformat(),
        )

    except IntegrityError:
        db.rollback()
        winner = db.scalars(
            select(ProcessedRequest).where(
                ProcessedRequest.request_id == request_id
            )
        ).first()
        if winner is None:
            raise create_server_error_exception(
                "Failed to queue request: unique constraint violation"
            )
        _remember_row_and_conflict(request_id, winner, "database")

    except Exception as e:
        db.rollback()
        raise create_server_error_exception(f"Failed to queue request: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
