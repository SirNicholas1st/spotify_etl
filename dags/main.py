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

        auth_headers = {
        "client_id": clientID,
        "response_type": "code",
        # redirect has to be set to the same as the one in the spotify app settings.
        "redirect_uri": "http://localhost:7032/callback",
        "scope": "user-read-recently-played"
        }

        url = "https://accounts.spotify.com/authorize?" + urlencode(auth_headers)

        options = webdriver.ChromeOptions()
        options.add_argument('-headless')
        remote_webdriver = 'remote_chromedriver'
        with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
            driver.get(url)
            username_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "login-username"))
            )

            password_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "login-password"))
            )

            username_input.send_keys(spotify_username)
            password_input.send_keys(spotify_password)
            password_input.send_keys(Keys.RETURN)

            WebDriverWait(driver, 10).until(
                EC.url_contains("localhost:7032/callback")
            )
            
            code = driver.current_url.split("code=")[1]
            return code


    task1 = launch_browser_to_get_code()


pipeline()
        
    
