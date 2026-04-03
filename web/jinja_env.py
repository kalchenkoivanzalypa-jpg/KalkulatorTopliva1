"""Общий Jinja2: глобальные переменные для всех шаблонов."""
from __future__ import annotations

from fastapi.templating import Jinja2Templates

from web import settings
from web.email_util import smtp_status_for_admin

templates = Jinja2Templates(directory="web/templates")
templates.env.globals["marketing_site_url"] = settings.MARKETING_SITE_URL
templates.env.globals["smtp_admin"] = smtp_status_for_admin
