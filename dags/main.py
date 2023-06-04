from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
import requests
from urllib.parse import urlencode
import base64
import pandas as pd
from api_codes import clientID, secret, spotify_username, spotify_password
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

    @task
    def launch_browser_to_get_code():
        # the spotify api requires an authentication from the user every time the DAG runs. The need for always authenticating the user comes from the fact that
        # this code is only usable once.
        # the purpose of this function is to receive an authorization code which is used to receive a token which is valid for only 1 hour. 
        auth_headers = {
        "client_id": clientID,
        "response_type": "code",
        # redirect has to be set to the same as the one in the spotify app settings.
        "redirect_uri": "http://localhost:7032/callback",
        "scope": "user-read-recently-played"
        }

        url = "https://accounts.spotify.com/authorize?" + urlencode(auth_headers)

        options = webdriver.ChromeOptions()
        # adding the headless argument to options, so there wont be a visible browser window.
        options.add_argument('-headless')
        remote_webdriver = 'remote_chromedriver'

        # launches chrome with the defined options and waits until it can locate the elements "login-username" and "login-password".
        # If this takes more than 10 seconds an exception will be risen.
        with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
            driver.get(url)
            username_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "login-username"))
            )

            password_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "login-password"))
            )

            # Login to spotify with username and password defined in ap_codes.py file.
            username_input.send_keys(spotify_username)
            password_input.send_keys(spotify_password)
            password_input.send_keys(Keys.RETURN)

            # The browser waits until the the url contains the specified string, if this takes more than 10 seconds an exception will be risen.
            # Port needs to be the same as set in the spotify app. 
            WebDriverWait(driver, 10).until(
                EC.url_contains("localhost:7032/callback")
            )
            
            # the needed code is in the url after "code=" substring, so the string is splitted from that point and the 2nd item from the list is returned
            code = driver.current_url.split("code=")[1]
            return code

    @task
    def get_token(code):
            
            # The purpose of this function is to receive the token which is valid for 1 hour. 
            # To receive the token we need the one time authorization code returned by the launch_browser_to_get_code function
            # and client secret defined in the spotify app.
            encoded_credentials = base64.b64encode(clientID.encode() + b':' + secret.encode()).decode("utf-8")

            token_headers = {
                "Authorization": "Basic " + encoded_credentials,
                "Content-Type": "application/x-www-form-urlencoded"
            }

            token_data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": "http://localhost:7032/callback"
            }

            r = requests.post("https://accounts.spotify.com/api/token", data=token_data, headers=token_headers)
            token = r.json()["access_token"]

            return token
    
    @task
    def get_history(token):
         # the purpose of this function is to retrieve the recently listened songs as a json object from the api.
         # the function requires the token which is valid for 1 hour.
        user_headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json"
        }

        # the spotify api uses timestamps as microseconds. This stamp is the current time - 24 hours without decimals.
        # since the dag will run everyday at 00:00 this time stamp will retrieve the latest 50 songs played during the day (50 songs is the api limitation).
        stamp = str((datetime.now() - timedelta(hours=24)).timestamp() * 1000).split(".")[0]
        user_params = {
            "limit": 50,
            "after": stamp
        }

        user_tracks_response = requests.get("https://api.spotify.com/v1/me/player/recently-played", params=user_params, headers=user_headers)

        respo = user_tracks_response.json()

        return respo

    task1 = launch_browser_to_get_code()
    task2 = get_token(code = task1)
    task3 = get_history(token = task2)


pipeline()
        
    
