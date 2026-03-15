"""
Flask application factory and API setup.

This module is responsible for two things:
  1. create_app(app)  — configure a Flask app instance (settings, middleware)
  2. create_api(app, ...) — attach a Flask-RESTX Api to that app

It is called by framework/app/runner.py and should remain thin:
all business logic lives in the worker layer, not here.
"""

import os
import time

import urllib3
from flask import request, g
from flask_restx import Api
from ..commons.logger import logger
from werkzeug.serving import WSGIRequestHandler
from ..tracing import get_tracer


class CustomRequestHandler(WSGIRequestHandler):
    """Silence noisy broken-pipe errors from clients that close early.

    Werkzeug's default handler lets ConnectionResetError and BrokenPipeError
    propagate, filling logs with noise on every dropped connection.
    """
    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError, AssertionError):
            pass


def create_api(app, version, title, description):
    """Attach a Flask-RESTX Api to *app*.

    Returns the Api instance so callers can register namespaces and models.
    urllib3 InsecureRequestWarning is suppressed globally here — the PoC
    makes outbound requests to local services that may use self-signed certs.
    """
    api = Api(
        app=app,
        version=version,
        title=title,
        description=description,
    )
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return api


def create_app(app):
    """Configure a Flask app for the QF framework.

    Reads config from the application's config.Config class (loaded via
    app.config.from_object). Attaches request/response logging middleware
    that uses the framework logger (with traceparent injection).

    Note: JWT_ALGORITHM is no longer configured here — the auth module
    has been removed. Security is the responsibility of the deployment
    environment (API gateway, mTLS, etc.).
    """
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # Load all uppercase attributes from the app's config.Config class.
    # This makes Config.SECRET_KEY etc. available as app.config['SECRET_KEY'].
    app.config.from_object('config.Config')

    app.debug = False
    app.testing = False

    # ---- Per-request logging middleware ----
    # Records a DEBUG line for every incoming request and a DEBUG line for
    # every response, including wall-clock duration in milliseconds.
    # We use Flask's 'g' object (per-request context) to carry the start time
    # from before_request to after_request without thread-local gymnastics.

    def log_request_info():
        g.start_time = time.perf_counter()
        logger.debug(
            f"[HTTP] --> {request.method} {request.url}",
            "gray_back",
        )

    def log_response_info(response):
        start_time = getattr(g, "start_time", None)
        if start_time:
            duration = (time.perf_counter() - start_time) * 1000
            logger.debug(
                f"[HTTP] <-- {request.method} {request.url} "
                f"status={response.status_code} duration={duration:.1f}ms",
                "gray_back",
            )
        return response

    # Enable request/response logging when LOG_ENDPOINTS=true.
    # Off by default to reduce noise in high-throughput scenarios.
    if os.environ.get("LOG_ENDPOINTS", "false").lower() in ("1", "true", "yes"):
        app.before_request(log_request_info)
        app.after_request(log_response_info)

    return app
