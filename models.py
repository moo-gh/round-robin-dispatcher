import json
from datetime import datetime
from typing import Any, Dict

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class ProcessedRequest(Base):
    __tablename__ = "processed_requests"

    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(String(255), unique=True, index=True, nullable=False)
    payload = Column(Text, nullable=False)
    worker_id = Column(Integer, nullable=False)
    result = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    def set_payload(self, payload_dict):
        """Convert dict to JSON string for storage"""
        self.payload = json.dumps(payload_dict)

    def get_payload(self):
        """Convert JSON string back to dict"""
        return json.loads(self.payload) if self.payload else {}

    def set_result(self, result_dict):
        """Convert dict to JSON string for storage"""
        self.result = json.dumps(result_dict)

    def get_result(self):
        """Convert JSON string back to dict"""
        return json.loads(self.result) if self.result else {}

    def cache_dict(self, **extra: Any) -> Dict[str, Any]:
        """Common shape for in-memory cache entries (optional extra keys merged in)."""
        data: Dict[str, Any] = {
            "worker_id": self.worker_id,
            "created_at": self.created_at.isoformat(),
            "payload": self.get_payload(),
        }
        data.update(extra)
        return data
