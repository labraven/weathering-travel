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
# Set the base URL and query parameters
base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
params = {
    "datasetid": "NORMAL_HLY",
    "stations": "GHCND:USW00013874",
    "startdate": "2020-01-01T00:00:00 ",
    "enddate": "2020-01-02T23:59:59"
}

# Set the headers with your token
headers = {"token": NOAA_TOKEN}

# Make the request
response = requests.get(base_url, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Save the response to a file
    with open("weather_data_available.json", "w") as outfile:
        outfile.write(response.text)
else:
    print(f"Request failed with status code {response.status_code}")
