import json
import os
import time
import webbrowser
from requests_oauthlib import OAuth2Session

from dotenv import load_dotenv

load_dotenv()
# This flags Eve SSO to let us use a standard HTTP:// connection to do our Eve login.
# Eve's SSO wants a https:// connection. But, who has time for setting that up. We're just going to use localhost:8000
# Probably not ideal to transmit credentials across plain HTTP, so use your own judgement.
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

CLIENT_ID = os.getenv('CLIENT_ID')  # stored in you .env file
SECRET_KEY = os.getenv('SECRET_KEY')  # stored in you .env file
REDIRECT_URI = 'http://localhost:8000/callback'  # workaround so we don't have to set up a real server
AUTHORIZATION_URL = 'https://login.eveonline.com/v2/oauth/authorize'
TOKEN_URL = 'https://login.eveonline.com/v2/oauth/token'
token_file = 'token.json'


# ------------------------------------------------
# Functions: Oauth2 Flow
# -----------------------------------------------
# noinspection PyTypeChecker
def save_token(token):
    # Save the OAuth token including refresh token to a file.
    print('saving token...')
    with open(token_file, 'w') as f:
        json.dump(token, f)
        # note some IDEs will flag this as an error.
        # This is because jason.dump expects a str, but got a TextIO instead.
        # TextIO does support string writing, so this is not actually an issue.
    print('token saved')


def load_token():
    # Load the OAuth token from a file, if it exists.
    if os.path.exists(token_file):
        print('loading token...')
        with open(token_file, 'r') as f:
            return json.load(f)
    return None


def get_oauth_session(token=None, requested_scope=None):
    # Get an OAuth session, refreshing the token if necessary.
    extra = {'client_id': CLIENT_ID, 'client_secret': SECRET_KEY}
    if token:
        return OAuth2Session(CLIENT_ID, token=token, auto_refresh_url=TOKEN_URL, auto_refresh_kwargs=extra,
                             token_updater=save_token)
    else:
        return OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=requested_scope)


# Redirect user to the EVE Online login page to get the authorization code.
# noinspection DuplicatedCode
def get_authorization_code():
    oauth = get_oauth_session()
    authorization_url, state = oauth.authorization_url(AUTHORIZATION_URL)
    print(f"Please go to this URL and authorize access: {authorization_url}")
    webbrowser.open(authorization_url)
    redirect_response = input('Paste the full redirect URL here: ')
    token = oauth.fetch_token(TOKEN_URL, authorization_response=redirect_response, client_secret=SECRET_KEY)
    save_token(token)
    return token


def get_token(requested_scope):
    # Retrieve a token, refreshing it using the refresh token if available.
    print('opening ESI session...')
    print('----------------------------------')
    token = load_token()
    if token:
        oauth = get_oauth_session(token, requested_scope)
        expire = oauth.token['expires_at']
        print(f'token expires at {expire}')
        if expire < time.time():
            print("Token expired, refreshing token...")
            token = oauth.refresh_token(TOKEN_URL, client_id=CLIENT_ID, client_secret=SECRET_KEY)
            print('saving new token')
            save_token(token)
        print('returning token')
        return token
    else:
        print('need to get an authorization code, stand by')
        return get_authorization_code()
