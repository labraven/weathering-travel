import requests

# https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&stations=<station_id>&startDate=<start_date>&endDate=<end_date>&dataTypes=HLY-TEMP-NORMAL&format=json
# datasetid: NORMAL_HLY indicates that we want to retrieve hourly data from the US National Centers for Environmental Information's (NCEI) "Daily Normals" dataset.
# datatypeid: HLY-TEMP-NORMAL indicates that we want to retrieve hourly temperature data from the "Daily Normals" dataset.
# stationid: GHCND:USW00013874 specifies the station ID for Atlanta Hartsfield Jackson International Airport.
# startdate: 2020-01-01T00:00:00 specifies the start date and time of the data range we want to retrieve.
# enddate: 2020-12-31T23:59:59 specifies the end date and time of the data range we want to retrieve.
# units: standard specifies that we want to retrieve the data in US customary units (i.e. Fahrenheit for temperature).
# limit: 1000 specifies the maximum number of results to return in a single API call.

NOAA_TOKEN = "KqpPzkMmEetNTfMrBKYkuFMRfjETbKjv"


import requests
import json
from datetime import datetime, timedelta

# NOAA API base URL
base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

# Replace 'YOUR_TOKEN' with your NOAA API token
api_token = NOAA_TOKEN

# Station ID for Atlanta Hartsfield Jackson International Airport
station_id = 'GHCND:USW00013874'

# Specify the date range for the hourly history
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 2)

# Create a list to store the hourly data
hourly_data = []

# Make API requests for each hour and collect the data
current_date = start_date
while current_date < end_date:
    # Format the date in ISO8601 format
    formatted_date = current_date.strftime('%Y-%m-%dT%H:%M:%S')

    # Set the query parameters
    params = {
        'datasetid': 'GHCND',
        'stationid': station_id,
        'startdate': formatted_date,
        'enddate': formatted_date,
        'limit': 64,  # Adjust as needed
    }

    # Set the headers with your API token
    headers = {'token': api_token}

    # Make the API request
    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        hourly_data.extend(response.json().get('results', []))
    else:
        print(f"Failed to retrieve data for {formatted_date}")

    # Move to the next hour
    current_date += timedelta(hours=1)

# Save the hourly data as a JSON file
with open('hourly_weather_data.json', 'w') as outfile:
    json.dump(hourly_data, outfile)

print("Hourly data saved to hourly_weather_data.json")
