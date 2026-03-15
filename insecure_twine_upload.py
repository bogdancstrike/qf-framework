# insecure_twine_upload.py
import sys
import requests
from twine.commands.upload import main as twine_upload_main

# Backup the real method
_real_request = requests.sessions.Session.request

def insecure_request(self, method, url, *args, **kwargs):
    # Force verify=False
    kwargs["verify"] = False
    return _real_request(self, method, url, *args, **kwargs)

# Patch requests globally
requests.sessions.Session.request = insecure_request

# Forward CLI arguments to Twine
if __name__ == "__main__":
    # Pass sys.argv[1:] to Twine
    sys.exit(twine_upload_main(sys.argv[1:]))

