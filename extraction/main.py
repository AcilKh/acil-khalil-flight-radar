from FlightRadar24.api import FlightRadar24API
from datetime import datetime
from minio import Minio
import io
import json
import os
import traceback
import logging
import pycountry_convert as pc

def initialize_minio_client():
    minio_host = os.getenv('CURRENT_IP')+':9000'
    print(minio_host)
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'acil')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'acil1234')

    client = Minio(
        minio_host,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )

    return client

def generate_file_name():
    now = datetime.now()
    year = now.year
    month = now.strftime('%Y-%m')
    day = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%Y%m%d%H%M%S%f')[:-3]
    return f'bronzezone/tech_year={year}/tech_month={month}/tech_day={day}/flights{day}{timestamp}.json'


def save_json_to_minio(file_path, data, client: Minio, bucket_name, pretty_print=False):
    try:
        json_data = json.dumps(data, ensure_ascii=False,
                               indent=4 if pretty_print else None)
        client.put_object(
            bucket_name,
            file_path,
            io.BytesIO(json_data.encode('utf-8')),
            len(json_data.encode('utf-8')),
            content_type='application/json'
        )
        logging.info(f"Data written to {file_path}")
    except Exception as e:
        logging.error(
            f"An error occurred while writing data to {file_path}: {e}")
        logging.debug(traceback.format_exc())


def get_continent(country_name):
    country_code = pc.country_name_to_country_alpha2(country_name)
    continent_code = pc.country_alpha2_to_continent_code(country_code)
    continent_name = pc.convert_continent_code_to_continent_name(continent_code)

    return continent_name


def initialize_airports(fr_api: FlightRadar24API, client):
    airports = fr_api.get_airports()
    airports_data = {"airports": []}
    for airport in airports:
        try:
            continent = get_continent(airport['country'])
            airports_data['airports'].append({
                "name": airport['name'],
                "iata": airport['iata'],
                "continent": continent
            })
        except:
            print('No continent found for this country ::' +
                  airport['country'])
    print(airports_data)
    file_path = 'airports.json'
    save_json_to_minio(file_path, airports_data, client,
               "flight-data", pretty_print=False)


def init_airlines(fr_api: FlightRadar24API, client):
    airlines_data = {"airlines": []}
    with open(os.path.join(os.path.dirname(__file__), 'airlines.json')) as f:
        airlines_json = json.load(f)
        for airline in airlines_json:
            airlines_data['airlines'].append({
                "name": airline['name'],
                "icao": airline['icao'],
                "country": airline['country']
            })
    file_path = 'airlines.json'
    save_json_to_minio(file_path, airlines_data, client,
               "flight-data", pretty_print=False)


def main(fr_api: FlightRadar24API, client):
    try:
        start_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        log_file = f'flight_data_{start_time}.log'
        logging.basicConfig(filename=log_file, level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s')
        flights_data = []
        flights = fr_api.get_flights()
        for flight in flights:
            if flight.id is not None:
                try:
                    while len(flights_data) < 100 :
                        details = fr_api.get_flight_details(flight.id)
                        logging.debug(details)
                        airline = details.get('airline')
                        aircraft = details.get('aircraft')
                        if aircraft:
                            aircraft.pop('images', None)
                        time = details.get('time', {})
                        if airline is not None and "code" in airline:
                            icao = airline['code'].get('icao')
                            flight_details = {
                                "icao": icao,
                                "airline": airline,
                                "time": time,
                                "origin_airport": flight.origin_airport_iata,
                                "destination_airport": flight.destination_airport_iata,
                                "aircraft": aircraft,
                                "flying": True if time['real']['arrival'] is None else False
                            }
                            flights_data.append(flight_details)
                except Exception as ex:
                    logging.error(
                        f"An error occurred while retrieving flight details: {ex}")
                    logging.debug(traceback.format_exc())

        file_path = generate_file_name()
        save_json_to_minio(file_path, flights_data, client,
                   "flight-data", pretty_print=False)

    except Exception as e:
        logging.error(f"An error occurred in main function: {e}")
        logging.debug(traceback.format_exc())


if __name__ == "__main__":
    fr_api = FlightRadar24API(...)
    minio_client = initialize_minio_client()
    initialize_airports(fr_api, minio_client)
    initialize_airports(fr_api, minio_client)
    main(fr_api, minio_client)
