import os
from ..commons.logger import logger

import jwt
import requests
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.x509 import load_pem_x509_certificate
from flask import current_app


# Public Key for decrypting the JWT
def load_public_key_from_iam(realm_name, iam_server_url='https://127.0.0.1:4000', token=None):
    try:
        # Form the URL for the public key endpoint
        url = f"{iam_server_url}/oauth2/public_key"

        # Prepare the JSON payload with the realm_name and kid
        header = jwt.get_unverified_header(token)
        kid = None
        if 'kid' in header:
            kid = header['kid']

        data = {
            'realm_name': realm_name,
            'kid': kid
        }

        # Make a POST request to the server to get the public key
        response = requests.post(url, json=data, verify=False)

        # Check if the response was successful
        if response.status_code == 200:
            # Load the public key from the JSON response
            public_key_data = response.json()['public_key']
            public_key = load_pem_public_key(
                public_key_data.encode(),
                backend=default_backend()
            )

            current_app.config['JWT_PUBLIC_KEY'] = public_key
            current_app.config['JWT_ALGORITHM'] = 'RS256'
            return public_key
        else:
            logger.debug("Failed to retrieve the public key: Status code", response.status_code)
            return None
    except requests.exceptions.RequestException as e:
        logger.debug(f"An HTTP error occurred: {e}")
        return None
    except ValueError as e:
        logger.debug(f"An error occurred during the public key loading: {e}")
        return None
    except Exception as e:
        logger.debug(f"Unexpected error: {e}")
        return None


def load_public_key():
    try:
        # Get the certificate file path from the environment variable
        cert_path = os.environ.get('CERTIFICATE_HERA_IAM')

        if not cert_path:
            logger.debug("The CERTIFICATE_HERA_IAM environment variable is not set or empty.")
            return None

        # Read the certificate file
        with open(cert_path, "rb") as cert_file:
            cert_data = cert_file.read()

        # Load the certificate
        certificate = load_pem_x509_certificate(cert_data, default_backend())

        # Extract the public key from the certificate
        public_key = certificate.public_key()

        return public_key

    except FileNotFoundError:
        logger.debug(f"The certificate file {cert_path} was not found.")
        return None
    except ValueError as e:
        logger.debug(f"An error occurred while loading the certificate or public key: {e}")
        return None
    except Exception as e:
        logger.debug(f"Unexpected error: {e}")
        return None


def load_private_key():
    try:
        # Get the private key file path from the environment variable
        key_path = os.environ.get('KEY_HERA_IAM')

        if not key_path:
            logger.debug("The KEY_HERA_IAM environment variable is not set or empty.")
            return None

        # Ensure the private key file exists
        with open(key_path, "rb") as key_file:
            # Load the private key from the file, no password needed
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        return private_key

    except FileNotFoundError:
        logger.debug(f"The private key file {key_path} was not found.")
        return None
    except ValueError as e:
        logger.debug(f"An error occurred during the private key loading: {e}")
        return None
    except UnsupportedAlgorithm as e:
        logger.debug(f"Unsupported algorithm: {e}")
        return None
    except Exception as e:
        logger.debug(f"Unexpected error: {e}")
        return None
