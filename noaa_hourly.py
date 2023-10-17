import requests

# Set the base URL for the CDO API
base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

# Set the query parameters for the API request
params = {
    "datasetid": "GHCND",
    "stationid": "GHCND:USW00013874",
    "startdate": "2022-01-01T00:00:00",
    "enddate": "2022-01-01T23:59:59",
    "datatypeid": "AWND,WBAN,WT01,WT02,WT03,WT04,WT05,WT06,WT08,WT09,WT11,WT13,WT14,WT15,WT16,WT17,WT18,WT19,WT22,WT23,WT25,WT26,WT27,WT28,WT31,WT32,WT33,WT34,WT35,WT36,WT37,WT38,WT39,WT40,WT41,WT42,WT43,WT44,WT45,WT46,WT47,WT48,WT49,WT50,WT51,WT52,WT53,WT54,WT55,WT56,WT57,WT58,WT59,WT60,WT61,WT62,WT63,WT64,WT65,WT66,WT67,WT68,WT69,WT70,WT71,WT72,WT73,WT74,WT75,WT76,WT77,WT78,WT79,WT80,WT81,WT82,WT83,WT84,WT85,WT86,WT87,WT88,WT89,WT90,WT91,WT92,WT93,WT94,WT95,WT96",
    "units": "standard",
    "limit": 128
}
YOUR_TOKEN = "KqpPzkMmEetNTfMrBKYkuFMRfjETbKjv"
# Set the headers with your token
headers = {"token": YOUR_TOKEN}

# Make the initial request to the API to get the total number of results
response = requests.get(base_url, headers=headers, params=params)
total_results = response.json()["metadata"]["resultset"]["count"]

# Make subsequent requests to the API to retrieve all hourly data for January 1, 2022
results = []
offset = 0
while offset < total_results:
    params["offset"] = offset
    response = requests.get(base_url, headers=headers, params=params)
    hourly_data = response.json()["results"]
    results.extend(hourly_data)
    offset += len(hourly_data)

# Extract the hourly wet bulb temperature data from the results
hourly_wet_bulb_temperatures = []
for hour in results:
    wet_bulb_temperature = hour.get("value")
    if wet_bulb_temperature is not None and hour["datatype"] == "AWND":
        hourly_wet_bulb_temperatures.append(wet_bulb_temperature)

# Print the hourly wet bulb temperature data for January 1, 2022
print(hourly_wet_bulb_temperatures)
