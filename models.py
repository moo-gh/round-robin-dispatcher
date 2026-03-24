import json
from datetime import datetime
from typing import Any, Dict, Optional

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

    @staticmethod
    def _encode_dict(data: Dict[str, Any]) -> str:
        return json.dumps(data)

    @staticmethod
    def _decode_json(text: Optional[str]) -> Dict[str, Any]:
        return json.loads(text) if text else {}

    def set_payload(self, payload_dict: Dict[str, Any]) -> None:
        """Convert dict to JSON string for storage"""
        self.payload = self._encode_dict(payload_dict)

    def get_payload(self) -> Dict[str, Any]:
        """Convert JSON string back to dict"""
        return self._decode_json(self.payload)

    def set_result(self, result_dict: Dict[str, Any]) -> None:
        """Convert dict to JSON string for storage"""
        self.result = self._encode_dict(result_dict)

    def get_result(self) -> Dict[str, Any]:
        """Convert JSON string back to dict"""
        return self._decode_json(self.result)

    def cache_dict(self, **extra: Any) -> Dict[str, Any]:
        """Common shape for in-memory cache entries (optional extra keys merged in)."""
        data: Dict[str, Any] = {
            "worker_id": self.worker_id,
            "created_at": self.created_at.isoformat(),
            "payload": self.get_payload(),
        }
        data.update(extra)
        return data
