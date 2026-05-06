"""Главная и статические страницы."""
from __future__ import annotations

import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from web.jinja_env import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    max_bot_link = (os.getenv("MAX_BOT_LINK") or "").strip()
    return templates.TemplateResponse(
        "about.html",
        {"request": request, "max_bot_link": max_bot_link},
    )
