from fastapi import HTTPException


def create_conflict_exception(
    request_id: str,
    worker_id: int,
    created_at: str,
    source: str = "unknown"
) -> HTTPException:
    """
    Create an HTTPException for request conflicts (409).
    """
    return HTTPException(
        status_code=409,
        detail={
            "error": "Request already processed or in progress",
            "request_id": request_id,
            "worker_id": worker_id,
            "created_at": created_at,
            "source": source,
        },
    )


def create_server_error_exception(message: str) -> HTTPException:
    """
    Create an HTTPException for server errors (500).
    """
    return HTTPException(
        status_code=500,
        detail=message
    )
