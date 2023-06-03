from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine
import requests
from urllib.parse import urlencode
import base64
import pandas as pd
from flask import Flask, request
from secrets import clientID, secret, spotify_username, spotify_password
from threading import Thread
from werkzeug.serving import make_server
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


default_args = {
    "owner": "SirNicholas1st",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    dag_id = "spotify_etl",
    start_date = datetime(2023, 5, 30),
    default_args = default_args,
    schedule = "@daily",
    catchup = False
)
def pipeline():

    app = Flask(__name__)
    # setting global variables
    server = None
    code = None
    browser = None
    token = None

    @app.route('/callback')
    def callback():
        # flask callback app to receive the code.
        global code
        code = request.args.get('code')
        return code

    @task
    def start_server():
        # starting a server to host the flask app. 
        global server
        server = make_server("localhost", 8080, app)
        thread = Thread(target=server.serve_forever)
        thread.start()

    @task
    def launch_browser_to_get_code():
        # Launches a browser window with arguments for the spotify api, authorizes the app by signing in with Spotify username and password defined in Api_codes.
        # This code is required for requesting a token for the application whis is valid for 1 hour. The output code from this is one time use only. So every run requires a new one
        auth_headers = {
        "client_id": clientID,
        "response_type": "code",
        # redirect has to be set to the same as the one in the spotify app settings.
        "redirect_uri": "http://localhost:8080/callback",
        "scope": "user-read-recently-played"
    }
        
        url = "https://accounts.spotify.com/authorize?" + urlencode(auth_headers)
        global browser
        browser = webdriver.Chrome()
        browser.get(url)

        username_input = WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.ID, "login-username"))
        )

        password_input = WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.ID, "login-password"))
        )

        username_input.send_keys(spotify_username)
        password_input.send_keys(spotify_password)
        password_input.send_keys(Keys.RETURN)
        
    
