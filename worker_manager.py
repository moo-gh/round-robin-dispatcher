import random
import asyncio
import threading
from typing import Dict, Optional
from datetime import datetime, timezone

from sqlalchemy import select

from models import ProcessedRequest
from cache import request_cache
from database import SessionLocal


class WorkerManager:
    def __init__(self, num_workers: int = 3):
        self.num_workers = num_workers
        self.current_worker = 0
        self.lock = threading.Lock()
        self.worker_status = {i: "free" for i in range(num_workers)}

    def get_next_worker(self) -> int:
        """Get next worker ID using round-robin algorithm"""
        with self.lock:
            worker_id = self.current_worker
            self.current_worker = (self.current_worker + 1) % self.num_workers
            return worker_id

    def set_worker_busy(self, worker_id: int) -> None:
        """Mark worker as busy"""
        with self.lock:
            self.worker_status[worker_id] = "busy"

    def set_worker_free(self, worker_id: int) -> None:
        """Mark worker as free"""
        with self.lock:
            self.worker_status[worker_id] = "free"

    def get_worker_status(self) -> Dict[int, str]:
        """Get current status of all workers"""
        with self.lock:
            return self.worker_status.copy()

    async def process_request(self, request_id: str) -> None:
        """
        Simulate processing a request with a worker.

        Uses a dedicated DB session so work continues after the HTTP request
        closes the dependency-injected session.
        """
        worker_id: Optional[int] = None
        with SessionLocal() as db:
            stmt = select(ProcessedRequest).where(
                ProcessedRequest.request_id == request_id
            )
            request_record = db.scalars(stmt).first()
            if request_record is None:
                print(f"No DB row for request_id={request_id}, skipping background work")
                return

            worker_id = request_record.worker_id
            print(f"Worker {worker_id} started request {request_id}")

            try:
                self.set_worker_busy(worker_id)

                request_cache.set(request_id, request_record.cache_dict())

                processing_time = random.randint(1, 10)
                await asyncio.sleep(processing_time)

                result = {
                    "processed_by": f"worker_{worker_id}",
                    "processing_time": processing_time,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "original_payload": request_record.get_payload(),
                    "result": f"Successfully processed request {request_id}",
                }

                request_record.set_result(result)
                db.commit()

                request_cache.set(request_id, request_record.cache_dict(result=result))

                print(f"Worker {worker_id} completed request {request_id}")

            except Exception as e:
                request_record.set_result({"error": str(e)})
                db.commit()

                request_cache.set(
                    request_id,
                    request_record.cache_dict(error=str(e)),
                )

                print(
                    f"Worker {worker_id} failed to process request {request_id}: {e}"
                )

            finally:
                if worker_id is not None:
                    self.set_worker_free(worker_id)


# Global worker manager instance
worker_manager = WorkerManager(num_workers=3)
