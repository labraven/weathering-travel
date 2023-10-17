

"""
This script is used to get the weather and aviation stations from the NOAA website.
https://www.aviationweather.gov/docs/metar/stations.txt
"""

# Open text file
# If a line starts with "!" then skip it
# if a line is blank then skip it
# if the first word in a line is >2 characters then substring[0] is "state_name", and substring[1] is "as_of_date"
# if first word == 2 chars, and is "CD", verify that the "CD" line contains all headers
# if first word == 2 chars, and is not "CD", then collect data into a dictionary
# collect data from each line into a dictionary until you hit a blank line.
# save the dictionary to a json file

""" Dictionary fields
STATE_PROV_CODE,
STATION_DATA_AS_OF,
CD,
STATION,
ICAO,
IATA,
SYNOP,
LAT,
LONG,
ELEV,
M,
N,
V,
U,
A,
C
"""

station_data_file = "/Users/ryan/Developer/weathering-travel/data/aviationweather.gov_docs_metar_stations.txt"
with open(station_data_file) as f:
    for line in f:
        print(line)
        print(len(line))
        if line.startswith("\n"):
            print("skipping new line character")
        else:
            print("does not start with new line character")
            split = line.split()
            if split[0] == "!":
                print("skipping line with exclamation")
            elif split[0] == "CD":
                print("skipping header line")
            else:
                print("something is missing")