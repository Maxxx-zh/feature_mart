# Imports
import pandas as pd
import numpy as np

import requests
import json
from datetime import date 

import hopsworks
import modal


LOCAL = False

if LOCAL == False:
    # Create a modal.Stub instance 
    stub = modal.Stub(name="feature_pipeline")

    # Create a custom image
    image = modal.Image.debian_slim().pip_install(["pandas", "requests", "hopsworks"]) 

    @stub.function(
        schedule=modal.Period(days=1),
        image=image,
        secret=modal.Secret.from_name("HOPSWORKS_API_KEY")
        )
    def modal_pipeline():
        feature_pipeline()


def parse_weather_data(city_name, today, coordinates):
    
    latitude, longitude = coordinates[city_name]
    
    url = f'https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={today}&end_date={today}&hourly=temperature_2m,relativehumidity_2m,precipitation,weathercode,windspeed_10m,winddirection_10m'
    response = requests.get(url)

    assert response.status_code == 200, f"⚠️ Response status code is: {response.status_code}"
    
    data_json = json.loads(response.content)['hourly']

    date = data_json['time']
    temperature = data_json['temperature_2m']
    humidity = data_json['relativehumidity_2m']
    precipitation = data_json['precipitation']
    weather_condition = data_json['weathercode']
    wind_speed = data_json['windspeed_10m']
    wind_direction = data_json['winddirection_10m'] 
    
    data_frame = pd.DataFrame(
        {
            'date': date,
            'weather_condition': weather_condition,
            'temperature': temperature,
            'humidity': humidity,
            'precipitation': precipitation,
            'wind_speed': wind_speed,
            'wind_direction': wind_direction
        }
    )
    data_frame.date = data_frame.date.str.replace('T',' ')
    data_frame.date = pd.to_datetime(data_frame.date, format='%Y-%m-%d %H:%M')
    data_frame['city_name'] = city_name
    
    return data_frame[
        [
            'city_name',
            'date',
            'weather_condition',
            'temperature',
            'humidity',
            'precipitation',
            'wind_speed',
            'wind_direction'
        ]
    ]


def connect_and_update_fg(data):
    project = hopsworks.login()

    fs = project.get_feature_store() 

    weather_historical_fg = fs.get_or_create_feature_group(
        name='weather_historical_fg',
        version=1
    )   
    weather_historical_fg.insert(data, write_options={"wait_for_job": False})


def feature_pipeline():
    city_coordinates = {
        'Kyiv': [50.5, 30.5],  # latitude, longitude
        'London': [51.5, -0.099990845],
        'Paris': [48.90001, 2.4000092],
        'Stockholm': [59.300003, 18.100006],
        'New_York': [40.699997, -74],
        'Los_Angeles': [34.1, -118.2],
        'Singapore': [1.4000015, 103.80002],
        'Sidney': [40.300003, 84.2],
        'Hong_Kong': [22.300003, 114.20001],
        'Rome': [41.90001, 12.5]
    }

    #today = date.today()
    today = '2023-01-26'

    data_parsed = pd.DataFrame()

    for city in city_coordinates.keys():
        parsed_df = parse_weather_data(
            city,
            today,
            city_coordinates
        )
        data_parsed = pd.concat([data_parsed,parsed_df])

    return connect_and_update_fg(data_parsed)


if __name__ == "__main__":
    if LOCAL == True :
        feature_pipeline()
    else:
        stub.deploy("feature_pipeline")
        with stub.run():
            modal_pipeline.call()






