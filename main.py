import requests
import aiohttp
import math
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception, stop_after_delay
import asyncio
import time
import sqlite3
import pandas as pd
import os
import csv
import traceback
import logging
from datetime import datetime
from dateutil import parser

MASTER_PATH = "/Users/isaacadjei/Library/Mobile Documents/com~apple~CloudDocs/Clinical Trial/Clinical-Trial/"
CSV_FILE_NAME = "DATA.csv"
ERROR_FILE_NAME = "ERRORS"
STUDY_ID_FILE_NAME = "study_ids.txt"
ERROR_CSV_FILE_NAME = "ERRORS_CSV.csv"
DATABASE_2020_FILE_NAME = "population_data_2020.db"
DATABASE_2010_FILE_NAME = "census_data_2010.db"
PROCESSED_IDS_FILE_NAME = "PROCESSED.txt"
LOCATION_DATABASE_FILE_NAME = "LocationDatabase.db"
ERROR_IDS_FILE_NAME = "ERROR_IDS.txt"

GOOGLE_API_KEY = "GOOGLE API KEY"

address_latlng_map = {}

def validate_address_with_google(address):
    """
    Use the Google Maps Geocoding API to validate or autocomplete the given address.
    Returns a tuple (formatted_address, {lat, lng}) or (error_message, None) if failure.
    """
    google_api_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": GOOGLE_API_KEY
    }

    try:
        response = requests.get(google_api_url, params=params)
        response.raise_for_status()

        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            result = data["results"][0]
            formatted_address = result["formatted_address"]
            location = result["geometry"]["location"]
            return formatted_address, location
        else:
            return "Invalid address or no matches found.", None
    except requests.exceptions.RequestException as e:
        return f"An error occurred while making the Google API request: {e}", None


def get_urban_rural_status_by_coords(lat, lng):
    """
    Determine if a location (latitude/longitude) is Urban, Rural, or Mixed
    using the USCB Geocoder API.
    """
    api_url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    params = {
        "x": lng,
        "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Census2020_Current",
        "format": "json"
    }

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()

        data = response.json()
        if "result" in data and "geographies" in data["result"]:
            geographies = data["result"]["geographies"]
            if "Census Blocks" in geographies:
                block_info = geographies["Census Blocks"][0]
                ur_code = block_info.get("UR", "N/A")
                if ur_code == "U":
                    return "Urban"
                elif ur_code == "R":
                    return "Rural"
                elif ur_code == "M":
                    return "Mixed"
                else:
                    return "UR code not available or unknown"
            else:
                return "No Census Block information found in response."
        else:
            return "Geographies not found in the response."
    except requests.exceptions.RequestException as e:
        return f"An error occurred while making the API request: {e}"


def get_urban_rural_status(address):
    """
    Validate an address using Google Maps API (unless we have it cached),
    retrieve coordinates, and determine if the location is Urban/Rural/Mixed.
    """
    if not address or address == "N/A":
        return "No address provided"

    if address in address_latlng_map:
        lat, lng, validated_address = address_latlng_map[address]
        if lat is None or lng is None:
            return f"Address validation failed: Invalid address or no matches found."
    else:
        validated_address, location = validate_address_with_google(address)
        if location is None:
            return f"Address validation failed: {validated_address}"
        lat = location["lat"]
        lng = location["lng"]
        address_latlng_map[address] = (lat, lng, validated_address)

    status = get_urban_rural_status_by_coords(lat, lng)
    return f"Validated Address: {validated_address}\nUrban/Rural Status: {status}"


def parse_date(date_string):
    if not date_string or date_string == "Not Available":
        return None, None, None

    formats = ['%Y-%m-%d', '%Y-%m', '%Y', '%m-%d-%Y', '%m-%Y']
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            if fmt in ['%Y-%m-%d', '%m-%d-%Y']:
                return parsed_date.year, parsed_date.month, parsed_date.day
            elif fmt in ['%Y-%m', '%m-%Y']:
                return parsed_date.year, parsed_date.month, None
            elif fmt == '%Y':
                return parsed_date.year, None, None
        except ValueError:
            pass
    try:
        parsed_date = parser.parse(date_string, yearfirst=False, dayfirst=False)
        return (
            parsed_date.year,
            parsed_date.month if parsed_date.month else None,
            parsed_date.day if parsed_date.day else None
        )
    except (ValueError, TypeError):
        return None, None, None


def calculate_study_duration_in_weeks(start_date_str, end_date_str):
    start_year, start_month, start_day = parse_date(start_date_str)
    end_year, end_month, end_day = parse_date(end_date_str)
    if not start_year or not end_year:
        return None
    start_month = start_month if start_month else 1
    start_day = start_day if start_day else 1
    end_month = end_month if end_month else 1
    end_day = end_day if end_day else 1
    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)
    duration_in_days = (end_date - start_date).days
    duration_in_weeks = duration_in_days / 7
    return duration_in_weeks


race_ethnicity_keywords = [
    "White", "Caucasian", "Black", "African American", "Asian",
    "Hispanic", "Latino", "Native American", "Pacific Islander",
    "Middle Eastern", "Alaska Native"
]

def check_race_ethnicity_in_title(title):
    mentioned_races = [race for race in race_ethnicity_keywords if race.lower() in title.lower()]
    if mentioned_races:
        return 'Y', ', '.join(mentioned_races)
    else:
        return 'N', None


def check_google_maps_api(api_key):
    test_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": "1600 Amphitheatre Parkway, Mountain View, CA", "key": api_key}
    response = requests.get(test_url, params=params)
    if response.status_code == 200 and response.json().get('status') == 'OK':
        pass
    else:
        raise Exception(
            "Google Maps API check failed. Status code: {}, Response: {}".format(
                response.status_code, response.json()
            )
        )

def check_census_api():
    test_url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    params = {
        "x": "-122.435",
        "y": "37.773",
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json"
    }
    response = requests.get(test_url, params=params)
    if response.status_code == 200 and 'result' in response.json():
        pass
    else:
        raise Exception(
            "Census API check failed. Status code: {}, Response: {}".format(
                response.status_code, response.json()
            )
        )

def check_clinical_trials_api():
    test_url = "https://clinicaltrials.gov/api/v2/studies"
    params = {
        "query.id": "NCT00118053",
        "fields": "NCTId"
    }
    response = requests.get(test_url, params=params)
    if response.status_code == 200:
        pass
    else:
        raise Exception(
            "ClinicalTrials.gov API check failed. Status code: {}, Response: {}".format(
                response.status_code, response.json()
            )
        )

def run_api_checks():
    google_api_key = GOOGLE_API_KEY
    try:
        check_google_maps_api(google_api_key)
        check_census_api()
        check_clinical_trials_api()
        print("All API checks passed.")
        return [True]
    except Exception as e:
        print("API check failed:", e)
        return [False, f"API check failed:, {e}"]


def GET_API_DATA(STUDY_ID):
    url = "https://clinicaltrials.gov/api/v2/studies"
    params = {
        "query.id": STUDY_ID,
        "fields": (
            "NCTId,"
            "BriefTitle,"
            "StudyFirstPostDate,"
            "resultsSection.baselineCharacteristicsModule.measures,"
            "protocolSection.contactsLocationsModule,"
            "protocolSection.statusModule.startDateStruct,"
            "protocolSection.statusModule.completionDateStruct,"
            "protocolSection.conditionsModule.conditions,"
            "protocolSection.eligibilityModule.eligibilityCriteria,"
            "protocolSection.eligibilityModule.healthyVolunteers,"
            "protocolSection.eligibilityModule.sex,"
            "protocolSection.eligibilityModule.genderBased,"
            "protocolSection.eligibilityModule.genderDescription,"
            "protocolSection.eligibilityModule.minimumAge,"
            "protocolSection.eligibilityModule.maximumAge,"
            "protocolSection.eligibilityModule.studyPopulation,"
            "protocolSection.eligibilityModule.samplingMethod"
        )
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        try:
            DATA = response.json()
            return DATA
        except ValueError:
            print("RESPONSE IS NOT IN JSON FORMAT")
            return None
    else:
        print(f"ERROR RETRIEVING STUDIES. STATUS CODE: {response.status_code}")
        return None

def insert_data_into_location_db(address, all_totals):
    """
    We have 1 (location_name) + 25 (totals) = 26 placeholders in the INSERT query.
    So all_totals must have length == 25.
    """
    LocationDatabaseFilepath = os.path.join(MASTER_PATH, LOCATION_DATABASE_FILE_NAME)
    conn = sqlite3.connect(LocationDatabaseFilepath)
    cur = conn.cursor()

    all_values_to_insert = [address] + all_totals
    cur.execute('''
        INSERT OR REPLACE INTO LocationDatabase(
            location_name, 
            total_whites, total_whites_males, total_whites_females,
            total_blacks, total_blacks_males, total_blacks_females,
            total_american_indian_and_alaska_native, total_american_indian_and_alaska_native_males, total_american_indian_and_alaska_native_females,
            total_asians, total_asian_males, total_asian_females,
            total_native_hawaiian_and_other_pacific_islander, total_native_hawaiian_and_other_pacific_islander_males, total_native_hawaiian_and_other_pacific_islander_females,
            total_other, total_other_male, total_other_female,
            total_multiple_races, total_multiple_races_males, total_multiple_races_females,
            total_hispanic_or_latino, total_hispanic_or_latino_male, total_hispanic_or_latino_female,
            total_not_hispanic_or_latino
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    ''', all_values_to_insert)

    conn.commit()
    conn.close()


def EXTRACT_STUDIES_DATA(GET_API_DATA, LINK):
    """
    Extracts all data for a single study. 
    Also times how long it takes to process the entire study 
    and each location separately.
    """
    study_start_time = time.perf_counter()  

    print("Extracting study data...")
    STUDY_LINK = LINK

    if (not GET_API_DATA or 'studies' not in GET_API_DATA 
        or len(GET_API_DATA['studies']) == 0):
        print("No studies data available!")
        return

    study_data = GET_API_DATA['studies'][0]
    protocol_section = study_data.get('protocolSection', {})
    status_module = protocol_section.get('statusModule', {})

    STUDY_NAME = protocol_section.get('identificationModule', {}).get('briefTitle', "Not Available")

    conditions_module = protocol_section.get('conditionsModule', {})
    conditions_list = conditions_module.get('conditions', [])
    condition_disease_studied = "; ".join(conditions_list) if conditions_list else "Not Available"

    START_DATE = status_module.get('startDateStruct', {}).get('date', "Not Available")
    END_DATE = status_module.get('completionDateStruct', {}).get('date', "Not Available")

    ALL_LOCATIONS = set()
    LOCATIONS = protocol_section.get('contactsLocationsModule', {}).get('locations', [])
    for LOCATION in LOCATIONS:
        LOCATION_STRING = (
            f"{LOCATION.get('facility', 'N/A')}, "
            f"{LOCATION.get('city', 'N/A')}, "
            f"{LOCATION.get('state', 'N/A')}, "
            f"{LOCATION.get('zip', 'N/A')}, "
            f"{LOCATION.get('country', 'N/A')}"
        )
        ALL_LOCATIONS.add(LOCATION_STRING)

    START_YEAR, START_MONTH, START_DAY = parse_date(START_DATE)
    END_YEAR, END_MONTH, END_DAY = parse_date(END_DATE)

    STUDY_DURATION_IN_WEEKS = calculate_study_duration_in_weeks(START_DATE, END_DATE)
    RACE_ETHNICITY_MENTIONED, SPECIFIC_RACES = check_race_ethnicity_in_title(STUDY_NAME)

    LOCATION_LIST = ["N/A"] * 13
    if len(ALL_LOCATIONS) > 13:
        print("Number of locations exceeds 13. Skipping this study.")
        return
    i = 0
    for location in ALL_LOCATIONS:
        LOCATION_LIST[i] = location
        i += 1

    measures = study_data.get('resultsSection', {}) \
                        .get('baselineCharacteristicsModule', {}) \
                        .get('measures', [])
    sex_measure = next(
        (m for m in measures if 'Sex: Female, Male' in m.get('title', "")), 
        None
    )
    NUMBER_OF_FEMALES_PARTICIPATED = 0
    NUMBER_OF_MALES_PARTICIPATED = 0

    if sex_measure:
        for category in sex_measure.get('classes', [])[0].get('categories', []):
            measurements_list = category.get('measurements', [])
            total_value = 0
            if measurements_list:
                total_value = measurements_list[-1].get('value', 0)

            if category.get('title') == "Female":
                NUMBER_OF_FEMALES_PARTICIPATED = total_value
            elif category.get('title') == "Male":
                NUMBER_OF_MALES_PARTICIPATED = total_value

    if (NUMBER_OF_FEMALES_PARTICIPATED == 'NA' or NUMBER_OF_MALES_PARTICIPATED == 'NA'):
        TOTAL_NUMBER_OF_PARTICIPANTS = 'NA'
    else:
        TOTAL_NUMBER_OF_PARTICIPANTS = int(float(NUMBER_OF_FEMALES_PARTICIPATED)) + \
                                       int(float(NUMBER_OF_MALES_PARTICIPATED))

    race_mapping = {
        "White": ["White", "Caucasian"],
        "Black": ["Black", "Black or African American", "Black, African American, or African"],
        "Hispanic": ["Hispanic", "Hispanic or Latino"],
        "Non Hispanic": ["Not Hispanic or Latino"],
        "Asian": ["Asian", "Asian or Pacific Islander"],
        "Other": ["Other"],
        "American Indian": ["American Indian or Alaska Native"],
        "Native Hawaiian": ["Native Hawaiian or Other Pacific Islander"],
        "Multiple Races": ["More than one race", "More than One"],
        "Middle Eastern": ["Middle Eastern or North African"],
        "None": ["None of the above"],
        "No Answer": ["Prefer not to answer", "no response"],
        "Unknown": ["Unknown or Not Reported"]
    }

    race_totals = {key: "not reported" for key in race_mapping.keys()}
    for measure in measures:
        if measure.get('title') in ['Race/Ethnicity, Customized', 'Race (NIH/OMB)']:
            for class_ in measure.get('classes', []):
                for category in class_.get('categories', []):
                    race = category.get('title')
                    for measurement in category.get('measurements', []):
                        for race_group, races in race_mapping.items():
                            if race in races:
                                value = measurement.get('value', 0)
                                race_totals[race_group] = int(value) if value != 'NA' else 0

    def extract_address(input_text):
        if input_text.startswith("For additional information regarding investigative sites for this trial, contact"):
            # approximate extraction
            parts = input_text.rsplit(',', 4)
            address_part = (
                parts[-4].strip() + " " +
                parts[-3].strip() + " " +
                parts[-2].strip() + " " +
                parts[-1].strip()
            )
        else:
            address_part = input_text.strip()
        return address_part

    async def get_geocode_data(session, place_name, api_key):
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {"address": place_name, "key": api_key}
        try:
            async with session.get(base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "OK":
                        result = data["results"][0]
                        lat = result["geometry"]["location"]["lat"]
                        lng = result["geometry"]["location"]["lng"]
                        full_address = result["formatted_address"]
                        print(f"Geocoded '{place_name}' to ({lat}, {lng})")
                        return lat, lng, full_address
                    else:
                        print(f"No geocode results for '{place_name}'")
                        return None, None, None
                else:
                    print(f"API response error for '{place_name}': {response.status}")
                    return None, None, None
        except Exception as err:
            print(f"Exception for '{place_name}': {err}")
            return None, None, None

    async def main(places):
        api_key = GOOGLE_API_KEY
        async with aiohttp.ClientSession() as session:
            tasks = []
            for place in places:
                clean_address = extract_address(place)
                if clean_address:
                    task = get_geocode_data(session, clean_address, api_key)
                    tasks.append(task)
                else:
                    print(f"Invalid address format: {place}")
            results = await asyncio.gather(*tasks)
            return results

    def getting_the_lats_and_longs_within_radius(lat, lng, radius_in_miles):
        radius = radius_in_miles * 1.60934

        def is_point_within_radius(center_latitude, center_longitude, 
                                   point_latitude, point_longitude, rad):
            lat1, lon1, lat2, lon2 = map(math.radians, [
                center_latitude, center_longitude, point_latitude, point_longitude
            ])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat / 2)**2 + \
                math.cos(lat1)*math.cos(lat2)*math.sin(dlon / 2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            distance = 6371 * c
            return distance <= rad

        def generate_points_within_radius(center_latitude, center_longitude, rad, step=0.1):
            latitudes = [lat]
            longitudes = [lng]

            min_latitude = center_latitude - math.degrees(rad / 6371)
            max_latitude = center_latitude + math.degrees(rad / 6371)
            min_longitude = center_longitude - math.degrees(
                rad / 6371 / math.cos(math.radians(center_latitude))
            )
            max_longitude = center_longitude + math.degrees(
                rad / 6371 / math.cos(math.radians(center_latitude))
            )

            current_lat = min_latitude
            while current_lat <= max_latitude:
                current_lon = min_longitude
                while current_lon <= max_longitude:
                    if is_point_within_radius(center_latitude, center_longitude, 
                                              current_lat, current_lon, rad):
                        latitudes.append(current_lat)
                        longitudes.append(current_lon)
                    current_lon += step
                current_lat += step

            return list(zip(latitudes, longitudes))

        return generate_points_within_radius(lat, lng, radius, step=0.1)

    max_delay = 300 

    def is_504_or_502_error(exception):
        return (isinstance(exception, Exception) and 
                ('504' in str(exception) or '502' in str(exception)))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    @retry(wait=wait_exponential(multiplier=1, min=4, max=max_delay),
           stop=stop_after_delay(max_delay),
           retry=retry_if_exception(is_504_or_502_error))
    async def fetch_codes(session, semaphore, pair):
        base_url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
        lat, lng = pair
        params = {
            "x": lng,
            "y": lat,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",
            "format": "json"
        }
        async with semaphore:
            try:
                async with session.get(base_url, params=params) as response:
                    if response.status != 200:
                        raise Exception(f'HTTP error: {response.status}')
                    if response.content_type != 'application/json':
                        raise Exception(
                            f"Expected JSON response but got '{response.content_type}'. "
                            f"Status: {response.status}"
                        )

                    data = await response.json()
                    if ('result' in data and
                        'geographies' in data['result'] and
                        '2020 Census Blocks' in data['result']['geographies']):
                        block_data = data['result']['geographies']['2020 Census Blocks'][0]
                        state = block_data['STATE']
                        county = block_data['COUNTY']
                        tract = block_data['TRACT']
                        block = block_data['BLOCK']
                        return (state, county, tract, block)
                    else:
                        return None
            except Exception as e:
                raise

    async def get_geography_codes(lat_lng_pairs):
        geography_codes = set()
        semaphore = asyncio.Semaphore(20)
        async with aiohttp.ClientSession() as session:
            tasks = []
            for pair in lat_lng_pairs:
                task = fetch_codes(session, semaphore, pair)
                tasks.append(task)
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        pass
                    elif result is not None:
                        geography_codes.add(result)
                return geography_codes
            except Exception as e:
                return None

    TOTAL_WHITES = 0
    TOTAL_WHITES_MALES = 0
    TOTAL_WHITES_FEMALES = 0
    TOTAL_BLACKS = 0
    TOTAL_BLACKS_MALES = 0
    TOTAL_BLACKS_FEMALES = 0
    TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE = 0
    TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_MALES = 0
    TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_FEMALES = 0
    TOTAL_ASIANS = 0
    TOTAL_ASIAN_MALES = 0
    TOTAL_ASIAN_FEMALES = 0
    TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER = 0
    TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_MALES = 0
    TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_FEMALES = 0
    TOTAL_OTHER = 0
    TOTAL_OTHER_MALE = 0
    TOTAL_OTHER_FEMALE = 0
    TOTAL_MULTIPLE_RACES = 0
    TOTAL_MULTIPLE_RACES_MALES = 0
    TOTAL_MULTIPLE_RACES_FEMALES = 0
    TOTAL_HISPANIC_OR_LATINO = 0
    TOTAL_HISPANIC_OR_LATINO_MALE = 0
    TOTAL_HISPANIC_OR_LATINO_FEMALE = 0
    TOTAL_NOT_HISPANIC_OR_LATINO = 0

    print("Starting geocoding of locations...")
    try:
        results = asyncio.run(main(ALL_LOCATIONS))
    except Exception as e:
        results = []

    LATS_AND_LONS_FOR_ALL_STUDY_LOCATIONS = []
    idx = 0
    for place in ALL_LOCATIONS:
        if idx < len(results):
            lat, lon, full_addr = results[idx]
            idx += 1
            if lat is not None and lon is not None:
                address_latlng_map[place] = (lat, lon, full_addr)
                LATS_AND_LONS_FOR_ALL_STUDY_LOCATIONS.append((lat, lon, place))
        else:
            pass

    if END_DATE is None:
        print("Study Date is None.")
        return

    Study_Year = None
    if END_DATE and len(END_DATE) >= 4:
        Study_Year = int(END_DATE[:4])

    # ---------------------------------------------
    #   2010 columns => 21 real + 4 "fake" Hispanic
    # ---------------------------------------------
    race_columns_2010_real = [
        # White totals and M/F
        "P003002",   # TOTAL WHITES
        "P012A002",  # MALE WHITES
        "P012A026",  # FEMALE WHITES

        # Black totals and M/F
        "P003003",
        "P012B002",
        "P012B026",

        # American Indian totals and M/F
        "P003004",
        "P012C002",
        "P012C026",

        # Asian totals and M/F
        "P003005",
        "P012D002",
        "P012D026",

        # Native Hawaiian totals and M/F
        "P003006",
        "P012E002",
        "P012E026",

        # Other Race totals and M/F
        "P003007",
        "P012F002",
        "P012F026",

        # Multiple Races totals and M/F
        "P003008",
        "P012G002",
        "P012G026"
    ]
    # We'll add 4 "fake" Hispanic columns (default=0) so total = 25 columns
    fake_hispanic_2010 = [
        "FAKE_HISPANIC",       # total_hispanic_or_latino
        "FAKE_HISPANIC_MALE",  # total_hispanic_or_latino_male
        "FAKE_HISPANIC_FEMALE",# total_hispanic_or_latino_female
        "FAKE_NOT_HISPANIC"    # total_not_hispanic_or_latino
    ]
    race_columns_2010 = race_columns_2010_real + fake_hispanic_2010

    # ---------------------------------------------
    #   2020 columns => 25 real (including Hispanic)
    # ---------------------------------------------
    race_columns_2020 = [
        "P8_003N",   # TOTAL WHITES
        "P12A_002N", # WHITES (MALE)
        "P12A_026N", # WHITES (FEMALE)

        "P8_004N",   # TOTAL BLACKS
        "P12B_002N", # BLACKS (MALE)
        "P12B_026N", # BLACKS (FEMALE)

        "P8_005N",   # TOTAL AMERICAN INDIAN
        "P12C_002N", # AMERICAN INDIAN MALE
        "P12C_026N", # AMERICAN INDIAN FEMALE

        "P8_006N",   # TOTAL ASIANS
        "P12D_002N",
        "P12D_026N",

        "P8_007N",   # TOTAL NATIVE HAWAIIAN
        "P12E_002N",
        "P12E_026N",

        "P8_008N",   # TOTAL OTHER
        "P12F_002N",
        "P12F_026N",

        "P8_009N",   # TOTAL MULTIPLE RACES
        "P12G_002N",
        "P12G_026N",

        "P9_003N",   # total_hispanic_or_latino
        "P9_002N",   # total_not_hispanic_or_latino?
        "P12H_002N", # possibly male hispanic
        "P12H_026N"  # possibly female hispanic
    ]

    if Study_Year and Study_Year < 2016:
        db_path = os.path.join(MASTER_PATH, DATABASE_2010_FILE_NAME)
        table_name = "CensusData"
        race_columns = race_columns_2010
        real_2010_cols = set(race_columns_2010_real)  # columns actually in DB
    else:
        db_path = os.path.join(MASTER_PATH, DATABASE_2020_FILE_NAME)
        table_name = "PopulationData"
        race_columns = race_columns_2020
        real_2010_cols = set()  # not used in 2020 branch

    if Study_Year and Study_Year < 2016:
        select_query_cols = [c for c in race_columns if c in real_2010_cols]
    else:
        select_query_cols = race_columns

    select_query = (
        "SELECT NAME," + ",".join(select_query_cols) + 
        f" FROM {table_name} WHERE state=? AND county=? AND tract=? AND block=?;"
    )

    totals_dict = {col: 0 for col in race_columns}

    for data in LATS_AND_LONS_FOR_ALL_STUDY_LOCATIONS:
        location_start_time = time.perf_counter()

        lat, lon, address = data
        print(f"Processing demographic data for location: {address}")

        latitude_and_longitude_pairs_in_67_mile_around_location = \
            getting_the_lats_and_longs_within_radius(lat, lon, 67)
        Geography_Codes_Of_Each_Lat_and_Lon_Pair_Around_Radius = \
            asyncio.run(get_geography_codes(latitude_and_longitude_pairs_in_67_mile_around_location))

        if not Geography_Codes_Of_Each_Lat_and_Lon_Pair_Around_Radius:
            print(f"No geography codes found for location: {address}. Skipping this location.")
            location_end_time = time.perf_counter()
            print(f"Time for this location: {location_end_time - location_start_time:.2f} sec")
            continue

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Each location must have 25 items (besides address) 
        # because the final insert expects 26 total (1 for address + 25 totals).
        location_totals = [0]*len(race_columns)

        for (state, county, tract, block) in Geography_Codes_Of_Each_Lat_and_Lon_Pair_Around_Radius:
            cur.execute(select_query, (state, county, tract, block))
            rows = cur.fetchall()
            for row in rows:
                # Accumulate each column if it exists in "row"
                for i, col in enumerate(race_columns):
                    if col in row.keys():  # real column
                        val = row[col]
                        val_int = 0
                        if val is not None and str(val).strip() != "":
                            val_int = int(float(val))
                        location_totals[i] += val_int
                        totals_dict[col] += val_int
                    else:
                        # It's a "fake" column (for 2010), just keep 0
                        pass

        conn.close()

        insert_data_into_location_db(address, location_totals)

        location_end_time = time.perf_counter()
        print(f"Successfully processed location: {address}")
        print(f"Time for this location: {location_end_time - location_start_time:.2f} sec")

    if Study_Year and Study_Year < 2016:
        TOTAL_WHITES = totals_dict["P003002"]
        TOTAL_WHITES_MALES = totals_dict["P012A002"]
        TOTAL_WHITES_FEMALES = totals_dict["P012A026"]

        TOTAL_BLACKS = totals_dict["P003003"]
        TOTAL_BLACKS_MALES = totals_dict["P012B002"]
        TOTAL_BLACKS_FEMALES = totals_dict["P012B026"]

        TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE = totals_dict["P003004"]
        TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_MALES = totals_dict["P012C002"]
        TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_FEMALES = totals_dict["P012C026"]

        TOTAL_ASIANS = totals_dict["P003005"]
        TOTAL_ASIAN_MALES = totals_dict["P012D002"]
        TOTAL_ASIAN_FEMALES = totals_dict["P012D026"]

        TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER = totals_dict["P003006"]
        TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_MALES = totals_dict["P012E002"]
        TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_FEMALES = totals_dict["P012E026"]

        TOTAL_OTHER = totals_dict["P003007"]
        TOTAL_OTHER_MALE = totals_dict["P012F002"]
        TOTAL_OTHER_FEMALE = totals_dict["P012F026"]

        TOTAL_MULTIPLE_RACES = totals_dict["P003008"]
        TOTAL_MULTIPLE_RACES_MALES = totals_dict["P012G002"]
        TOTAL_MULTIPLE_RACES_FEMALES = totals_dict["P012G026"]

        # "Fake" columns (default=0)
        TOTAL_HISPANIC_OR_LATINO = totals_dict["FAKE_HISPANIC"]
        TOTAL_HISPANIC_OR_LATINO_MALE = totals_dict["FAKE_HISPANIC_MALE"]
        TOTAL_HISPANIC_OR_LATINO_FEMALE = totals_dict["FAKE_HISPANIC_FEMALE"]
        TOTAL_NOT_HISPANIC_OR_LATINO = totals_dict["FAKE_NOT_HISPANIC"]

    else:
        # 2020 references
        TOTAL_WHITES = totals_dict["P8_003N"]
        TOTAL_WHITES_MALES = totals_dict["P12A_002N"]
        TOTAL_WHITES_FEMALES = totals_dict["P12A_026N"]

        TOTAL_BLACKS = totals_dict["P8_004N"]
        TOTAL_BLACKS_MALES = totals_dict["P12B_002N"]
        TOTAL_BLACKS_FEMALES = totals_dict["P12B_026N"]

        TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE = totals_dict["P8_005N"]
        TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_MALES = totals_dict["P12C_002N"]
        TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_FEMALES = totals_dict["P12C_026N"]

        TOTAL_ASIANS = totals_dict["P8_006N"]
        TOTAL_ASIAN_MALES = totals_dict["P12D_002N"]
        TOTAL_ASIAN_FEMALES = totals_dict["P12D_026N"]

        TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER = totals_dict["P8_007N"]
        TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_MALES = totals_dict["P12E_002N"]
        TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_FEMALES = totals_dict["P12E_026N"]

        TOTAL_OTHER = totals_dict["P8_008N"]
        TOTAL_OTHER_MALE = totals_dict["P12F_002N"]
        TOTAL_OTHER_FEMALE = totals_dict["P12F_026N"]

        TOTAL_MULTIPLE_RACES = totals_dict["P8_009N"]
        TOTAL_MULTIPLE_RACES_MALES = totals_dict["P12G_002N"]
        TOTAL_MULTIPLE_RACES_FEMALES = totals_dict["P12G_026N"]

        TOTAL_HISPANIC_OR_LATINO = totals_dict["P9_003N"]
        TOTAL_NOT_HISPANIC_OR_LATINO = totals_dict["P9_002N"]
        TOTAL_HISPANIC_OR_LATINO_MALE = totals_dict["P12H_002N"]
        TOTAL_HISPANIC_OR_LATINO_FEMALE = totals_dict["P12H_026N"]

    eligibility_module = protocol_section.get('eligibilityModule', {})
    eligibility_criteria = eligibility_module.get('eligibilityCriteria', '')
    healthy_volunteers = eligibility_module.get('healthyVolunteers', '')
    sex = eligibility_module.get('sex', '')
    gender_based = eligibility_module.get('genderBased', '')
    gender_description = eligibility_module.get('genderDescription', '')
    minimum_age = eligibility_module.get('minimumAge', '')
    maximum_age = eligibility_module.get('maximumAge', '')
    study_population = eligibility_module.get('studyPopulation', '')
    sampling_method = eligibility_module.get('samplingMethod', '')

    DF_ROW = pd.DataFrame({
        "STUDY TITLE": [STUDY_NAME],
        "Condition/Disease Being Studied": [condition_disease_studied],
        "STUDY START DATE": [START_DATE],
        "START YEAR": [START_YEAR],
        "START MONTH": [START_MONTH],
        "START DAY": [START_DAY],
        "STUDY END DATE": [END_DATE],
        "END YEAR": [END_YEAR],
        "END MONTH": [END_MONTH],
        "END DAY": [END_DAY],
        "STUDY DURATION (WEEKS)": [STUDY_DURATION_IN_WEEKS],
        "RACE/ETHNICITY MENTIONED": [RACE_ETHNICITY_MENTIONED],
        "SPECIFIC RACES MENTIONED": [SPECIFIC_RACES],
        "TOTAL NUMBER OF PARTICIPANTS": [TOTAL_NUMBER_OF_PARTICIPANTS],
        "NUMBER OF MALE PARTICIPANTS": [NUMBER_OF_MALES_PARTICIPATED],
        "NUMBER OF FEMALE PARTICIPANTS": [NUMBER_OF_FEMALES_PARTICIPATED],
        "Eligibility Criteria": [eligibility_criteria],
        "Accepts Healthy Volunteers": [healthy_volunteers],
        "Sex/Gender": [sex],
        "Gender Based": [gender_based],
        "Gender Eligibility Description": [gender_description],
        "Minimum Age": [minimum_age],
        "Maximum Age": [maximum_age],
        "Study Population Description": [study_population],
        "Sampling Method": [sampling_method],
        "NUMBER OF WHITE/CAUCASIAN PARTICIPANTS": [race_totals["White"]],
        "NUMBER OF MALE WHITE/CAUCASIAN IN RADIUS": [TOTAL_WHITES_MALES],
        "NUMBER OF FEMALE WHITE/CAUCASIAN IN RADIUS": [TOTAL_WHITES_FEMALES],
        "TOTAL NUMBER OF WHITES IN RADIUS": [TOTAL_WHITES],
        "NUMBER OF BLACK PARTICIPANTS": [race_totals["Black"]],
        "NUMBER OF MALE BLACKS IN RADIUS": [TOTAL_BLACKS_MALES],
        "NUMBER OF FEMALE BLACKS IN RADIUS": [TOTAL_BLACKS_FEMALES],
        "TOTAL NUMBER OF BLACKS IN RADIUS": [TOTAL_BLACKS],
        "NUMBER OF ASIAN PARTICIPANTS": [race_totals["Asian"]],
        "NUMBER OF MALE ASIAN IN RADIUS": [TOTAL_ASIAN_MALES],
        "NUMBER OF FEMALE ASIAN IN RADIUS": [TOTAL_ASIAN_FEMALES],
        "TOTAL NUMBER OF ASIANS IN RADIUS": [TOTAL_ASIANS],
        "NUMBER OF AMERICAN INDIAN": [race_totals["American Indian"]],
        "NUMBER OF AMERIAN INDIAN MALES IN RADIUS": [TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_MALES],
        "NUMBER OF AMERICAN INDIAN FEMALES IN RADIUS": [TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE_FEMALES],
        "TOTAL NUMBER OF AMERICAN INDIANS IN RADIUS": [TOTAL_AMERICAN_INDIAN_AND_ALASKA_NATIVE],
        "NUMBER OF NATIVE HAWAIIAN": [race_totals["Native Hawaiian"]],
        "NUMBER OF MALE NATIVE HAWAIIAN IN RADIUS": [TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_MALES],
        "NUMBER OF FEMALE NATIVE HAWAIIAN IN RADIUS": [TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER_FEMALES],
        "TOTAL NUMBER OF NATIVE HAWAIIAN IN RADIUS": [TOTAL_NATIVE_HAWAIIAN_AND_PACIFIC_ISLANDER],
        "NUMBER OF MULTIPLE RACES": [race_totals["Multiple Races"]],
        "NUMBER OF MALE MULTIPLE RACES IN RADIUS": [TOTAL_MULTIPLE_RACES_MALES],
        "NUMBER OF FEMALE MULTIPLE RACES IN RADIUS": [TOTAL_MULTIPLE_RACES_FEMALES],
        "TOTAL NUMBER OF PEOPLE OF MULTIPLE RACES IN RADIUS": [TOTAL_MULTIPLE_RACES],
        "NUMBER OF HISPANIC PARTICIPANTS": [race_totals["Hispanic"]],
        "NUMBER OF NON HISPANIC PARTICIPANTS": [race_totals["Non Hispanic"]],
        "TOTAL NUMBER OF NON HISPANIC PARTICIPANTS IN RADIUS": [TOTAL_NOT_HISPANIC_OR_LATINO],
        "OTHER RACES": [race_totals["Other"]],
        "OTHER RACES MALES": [TOTAL_OTHER_MALE],
        "OTHER RACES FEMALES": [TOTAL_OTHER_FEMALE],
        "TOTAL OTHER RACES IN RADIUS": [TOTAL_OTHER],
        "NUMBER OF MIDDLE EASTERN": [race_totals["Middle Eastern"]],
        "NONE OF THE RACES HERE": [race_totals["None"]],
        "NO ANSWER": [race_totals["No Answer"]],
        "UNKNOWN": [race_totals["Unknown"]],
        "LOCATION 1": [LOCATION_LIST[0]],
        "LOCATION 2": [LOCATION_LIST[1]],
        "LOCATION 3": [LOCATION_LIST[2]],
        "LOCATION 4": [LOCATION_LIST[3]],
        "LOCATION 5": [LOCATION_LIST[4]],
        "LOCATION 6": [LOCATION_LIST[5]],
        "LOCATION 7": [LOCATION_LIST[6]],
        "LOCATION 8": [LOCATION_LIST[7]],
        "LOCATION 9": [LOCATION_LIST[8]],
        "LOCATION 10": [LOCATION_LIST[9]],
        "LOCATION 11": [LOCATION_LIST[10]],
        "LOCATION 12": [LOCATION_LIST[11]],
        "LOCATION 13": [LOCATION_LIST[12]],
        "STUDY LINK": [STUDY_LINK],
    })

    # Now fill in the RURAL/URBAN status columns using the cached geocoding
    for i in range(1, 14):
        loc_col = f"LOCATION {i}"
        if loc_col in DF_ROW.columns:
            loc_address = DF_ROW[loc_col].iloc[0]
            status_col = f"LOCATION {i} RURAL/URBAN STATUS"
            DF_ROW[status_col] = [get_urban_rural_status(loc_address)]

    FULL_PATH = os.path.join(MASTER_PATH, CSV_FILE_NAME)
    if not os.path.isfile(FULL_PATH):
        DF_ROW.to_csv(FULL_PATH, mode="w", index=False)
        print(f"Created new CSV file and added study data for: {STUDY_NAME}")
    else:
        DF_ROW.to_csv(FULL_PATH, mode="a", header=False, index=False)
        print(f"Appended study data to CSV for: {STUDY_NAME}")

    study_end_time = time.perf_counter()
    total_study_time = study_end_time - study_start_time
    print(f"Time for entire study '{STUDY_NAME}': {total_study_time:.2f} sec")


logging.basicConfig(
    filename='script_error.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)

def load_processed_ids(file_path):
    try:
        with open(file_path, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def persist_processed_ids(file_path, ids):
    with open(file_path, 'a') as file:
        for id_ in ids:
            file.write(f"{id_}\n")

processed_ids = load_processed_ids(f"{MASTER_PATH}/{PROCESSED_IDS_FILE_NAME}")

api_check_result = run_api_checks()
if not api_check_result[0]:
    raise Exception(api_check_result[1])

try:
    with open(f"{MASTER_PATH}{STUDY_ID_FILE_NAME}", "r") as f, \
         open(f"{MASTER_PATH}{ERROR_CSV_FILE_NAME}", "a", newline='') as error_file:

        csv_writer = csv.writer(error_file)

        for line in f:
            ID = line.strip()
            ID_start_time = time.perf_counter() 

            if ID in processed_ids:
                print(f"ID {ID} has already been processed. Skipping.")
                continue

            try:
                STUDY_LINK = f"https://clinicaltrials.gov/study/{ID}"
                data = GET_API_DATA(ID)
                EXTRACT_STUDIES_DATA(data, STUDY_LINK)
                processed_ids.add(ID)

                with open(f"{MASTER_PATH}/{PROCESSED_IDS_FILE_NAME}", "a") as processed:
                    processed.write(f"{ID}\n")
                print(f"Added {ID} to processed_ids")

            except Exception as e:
                error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                error_message = str(e)
                error_traceback = traceback.format_exc()
                logging.error(f"Error processing ID {ID}: {error_message}\n{error_traceback}")
                print(f"Error processing ID {ID}: {error_message}")

                link = f"https://clinicaltrials.gov/study/{ID}"
                csv_writer.writerow([ID, link, error_message, error_time, error_traceback])

            finally:
                ID_end_time = time.perf_counter()
                print(f"Time to process ID {ID}: {ID_end_time - ID_start_time:.2f} sec")

finally:
    persist_processed_ids(f"{MASTER_PATH}/{PROCESSED_IDS_FILE_NAME}", processed_ids)
    print("Finished processing all IDs.")
