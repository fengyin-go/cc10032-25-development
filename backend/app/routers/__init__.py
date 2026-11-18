"""业务路由汇总：编排专用路由优先注册，通用资源路由兜底动态模块路径。"""
from __future__ import annotations

from app.routers import orchestration, resource

ROUTERS = [orchestration, resource]
