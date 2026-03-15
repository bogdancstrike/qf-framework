import json
import os
import threading
import time
from functools import wraps
from typing import Optional
from ..commons.logger import logger

import flask
import jwt
import requests
from flask import current_app
from flask import jsonify, abort, session, request, make_response
from flask_jwt_extended.exceptions import JWTDecodeError
from flask_restx._http import HTTPStatus

from ..auth.keys import load_public_key_from_iam
from ..commons.utils import load_json

cache = {}
_cache_lock = threading.RLock()


def get_realms():
    with open('maps/realm.json', 'rt') as fin:
        return json.load(fin)


# Verifies that it is the correct realm
def find_realm_data(realm_name):
    with _cache_lock:
        if 'realms' not in cache:
            cache['realms'] = load_json('maps/realm.json')
        for realm in cache['realms']:
            if realm['realm_name'] == realm_name:
                return realm
    raise Exception('Realm not found')


# Client side decorator for JWT injection (kwarg access_token)
def token_handler(realm_name=None, config=None):
    # Yell if IAM_SERVER_URL is not set
    if config is None or config.IAM_SERVER_URL is None:
        raise Exception("token_handler: IAM_SERVER_URL is not set")

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            realm_data = find_realm_data(realm_name)
            current_app = args[0] if args else kwargs['app']
            if not realm_data:
                return jsonify({"message": "Realm not found"}), 404

            if realm_data['access_token'] and verify_token(realm_data['access_token'], realm_data, current_app):
                kwargs['access_token'] = realm_data['access_token']
                return fn(*args, **kwargs)

            # Refresh the token if necessary
            if realm_data['refresh_token']:
                try:
                    refresh_response = requests.post(f'{config.IAM_SERVER_URL}/oauth2/refresh_token',
                                                     json={'refresh_token': realm_data['refresh_token']},
                                                     verify=False)
                except TimeoutError as e:
                    abort(HTTPStatus.INTERNAL_SERVER_ERROR, "Couldn't reach IAM server")

                if refresh_response.status_code == 200:
                    realm_data.update({
                        'access_token': refresh_response.json().get('access_token'),
                        'refresh_token': refresh_response.json().get('refresh_token')
                    })

                    if verify_token(realm_data['access_token'], realm_data, current_app):
                        kwargs['access_token'] = realm_data['access_token']
                        return fn(*args, **kwargs)
                else:
                    abort(HTTPStatus.INTERNAL_SERVER_ERROR, "Couldn't refresh token")

            # Perform login if refresh failed
            try:
                login_response = requests.post(f'{config.IAM_SERVER_URL}/oauth2/login',
                                               json=realm_data['login_data'], verify=False, timeout=5)
            except requests.exceptions.ConnectionError:
                logger.debug("Bad Gateway - Connection refused or server unreachable")
                abort(HTTPStatus.INTERNAL_SERVER_ERROR, "Couldn't reach IAM server")

            if login_response.status_code == 200:
                realm_data.update({
                    'access_token': login_response.json().get('access_token'),
                    'refresh_token': login_response.json().get('refresh_token')
                })

                if verify_token(realm_data['access_token'], realm_data, current_app):
                    kwargs['access_token'] = realm_data['access_token']
                    return fn(*args, **kwargs)

            return jsonify({"message": "Unauthorized"}), 401

        return wrapper

    return decorator


def role_required(role_names, realm_name=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if request.headers.get('Authorization', None) is not None:
                auth_header = request.headers.get('Authorization', None)
            else:
                if 'access_token' in session and session['access_token'] is not None:
                    auth_header = 'Bearer ' + session['access_token']
                else:
                    auth_header = None

            if auth_header is None:
                return make_response(jsonify({"message": "Authorization header is missing"}), 401)

            parts = auth_header.split()
            if len(parts) != 2 or parts[0].lower() != 'bearer':
                return make_response(
                    jsonify({"message": "Authorization header must be in the format: 'Bearer <token>'"}), 401)

            token = parts[1]

            try:
                decoded_token = jwt.decode(
                    token,
                    current_app.config['JWT_PUBLIC_KEY'],
                    algorithms=[current_app.config['JWT_ALGORITHM']],
                    options={"verify_aud": False, "allow_expired": True}
                )

                if decoded_token['exp'] <= time.time():
                    return make_response(jsonify({"message": "Token expired"}), 401)

                # Check for roles
                user_roles = decoded_token.get('realm_access', {}).get('roles', [])
                if not any(role in role_names for role in user_roles):
                    return make_response(jsonify({"message": "Forbidden"}), 403)

                return fn(*args, **kwargs)
            except JWTDecodeError as e:
                logger.debug(f"Token verification failed: {e}")
                return make_response(jsonify({"message": "Token is invalid or expired"}), 401)
            except Exception as e:
                try:
                    public_key = load_public_key_from_iam(realm_name=realm_name, token=token,
                                                          iam_server_url=os.environ.get('IAM_SERVER_URL'))
                    if public_key:
                        try:
                            decoded_token = jwt.decode(
                                token,
                                current_app.config['JWT_PUBLIC_KEY'],
                                algorithms=[current_app.config['JWT_ALGORITHM']],
                                options={"verify_aud": False, "allow_expired": True}
                            )

                            if decoded_token['exp'] <= time.time():
                                return make_response(jsonify({"message": "Token expired"}), 401)

                            user_roles = decoded_token.get('realm_access', {}).get('roles', [])
                            if not any(role in role_names for role in user_roles):
                                return make_response(jsonify({"message": "Forbidden"}), 403)

                            return fn(*args, **kwargs)
                        except JWTDecodeError as e:
                            logger.debug(f"Token verification failed: {e}")
                            return make_response(jsonify({"message": "Token is invalid or expired"}), 401)
                except Exception as e:
                    return make_response(jsonify({"message": "Unauthorized"}), 401)

        return wrapper

    return decorator


def verify_token(token, realm_data, current_app=None):
    def decode_token(token):
        return jwt.decode(
            token,
            current_app.config['JWT_PUBLIC_KEY'],
            algorithms=[current_app.config['JWT_ALGORITHM']],
            options={"verify_aud": False, "verify_exp": False}
        )

    with current_app.app_context():
        try:
            # Decode the token and allow expired tokens manually
            decoded_token = decode_token(token)

            # Manually check for token expiration
            if 'exp' in decoded_token and decoded_token['exp'] < time.time():
                logger.debug("Token has expired.")
                raise jwt.ExpiredSignatureError("Token has expired.")

            return True
        except jwt.ExpiredSignatureError:
            logger.debug("Token has expired. Reloading public key.")
            # Reload the public key and try again
            realm_data['public_key'] = load_public_key_from_iam(
                realm_name=realm_data['realm_name'],
                token=token,
                iam_server_url=os.environ.get('IAM_SERVER_URL')
            )

            try:
                # Decode token again after reloading the public key
                decoded_token = decode_token(token)

                if 'exp' in decoded_token and decoded_token['exp'] < time.time():
                    logger.debug("Token has expired after reloading public key.")
                    return False

                return True
            except JWTDecodeError:
                logger.debug("Token verification failed after reloading public key.")
                return False
        except Exception as e:
            logger.debug(f"Unexpected error during token verification: {e}")

            # Retry by reloading the public key one more time
            realm_data['public_key'] = load_public_key_from_iam(
                realm_name=realm_data['realm_name'],
                token=token,
                iam_server_url=os.environ.get('IAM_SERVER_URL')
            )

            try:
                decoded_token = decode_token(token)

                if 'exp' in decoded_token and decoded_token['exp'] < time.time():
                    logger.debug("Token has expired after second key reload.")
                    return False

                return True
            except Exception as second_exception:
                logger.debug(f"Second attempt to verify token failed: {second_exception}")
                return False
