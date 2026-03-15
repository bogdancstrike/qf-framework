import ast
import os
import time

import urllib3
from flask import request, g
from flask_restx import Api
from ..commons.logger import logger
from werkzeug.serving import WSGIRequestHandler
from ..tracing import get_tracer


class CustomRequestHandler(WSGIRequestHandler):
    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError, AssertionError):
            pass


def create_api(app, version, title, description):
    api = Api(
        app=app,
        # doc=False,
        version=version,
        title=title,
        description=description
    )
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return api


def create_app(app):
    app.config['JWT_ALGORITHM'] = 'RS256'

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    app.config.from_object('config.Config')

    app.debug = False
    app.testing = False
    try:
        if not app.config['SECRET_KEY']:
            raise ValueError("No SECRET_KEY set for Flask application")
        if not app.config['TOKEN_URL']:
            raise ValueError("No TOKEN_URL set for Flask application")
    except KeyError:
        pass

    # Add logging to endpoints
    def log_request_info():
        # Use g to safely store start time
        g.start_time = time.perf_counter()
        logger.info(f"[ENDPOINT REQUEST] {request.method} {request.url}", "gray_back")

    def log_response_info(response):
        # Safely retrieve start time from g
        start_time = getattr(g, "start_time", None)
        if start_time:
            duration = (time.perf_counter() - start_time) * 1000  # Convert to milliseconds
            logger.debug(
                f"[ENDPOINT RESPONSE] {request.method} {request.url} - {response.status_code} [Duration: {duration:.2f} ms]",
                "gray_back"
            )
        else:
            logger.debug(
                f"[ENDPOINT RESPONSE] {request.method} {request.url} - {response.status_code}",
                "gray_back"
            )
        return response

    # Conditionally attach logging handlers

    # if ast.literal_eval(os.environ.get('LOG_ENDPOINTS', False)):
    #     app.before_request(log_request_info)
    #     app.after_request(log_response_info)

    return app
