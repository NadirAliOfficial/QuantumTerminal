"""
trade_mapper_routes.py — Stub for consumer terminal.
PRO-only routes. Consumer does not use these endpoints.
"""
from fastapi import APIRouter


def create_trade_mapper_router(*args, **kwargs) -> APIRouter:
    """Return empty router — PRO endpoints not available in consumer."""
    return APIRouter(tags=["trade_mapper"])
