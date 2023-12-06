from pyspark.sql import SparkSession, functions as F, types as T, Window
from minio import Minio
from datetime import datetime
import os


def initialize_minio_client():
    minio_host = os.getenv('CURRENT_IP') + ':9000'
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'acil')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'acil1234')

    client = Minio(
        minio_host,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )

    return client


spark = SparkSession.builder \
    .config('spark.master', 'local') \
    .config('spark.app.name', 'transformer') \
    .getOrCreate()

minioClient = initialize_minio_client()

# Chemin de la bucket dans Minio pour le fichier "airlines.json"
airlines_json_minio_path = 'airlines.json'
# Chemin local pour sauvegarder le fichier téléchargé
local_airlines_json_path = './airlines.json'

try:
    # Télécharger le fichier depuis MinIO vers le répertoire local
    minioClient.fget_object('flight-data', airlines_json_minio_path, local_airlines_json_path)
    print(f'Downloaded {airlines_json_minio_path} to {local_airlines_json_path}')

except Exception as e:
    print(f'Error downloading {airlines_json_minio_path}: {e}')


df = spark.read.option("multiline", "true").json(local_airlines_json_path)

# Fonction explode pour dipsatcher dans un nouveau Dataframe le contenu de la colonne airlines qui est une liste de tous les éléments en plusieurs lignes.
# Une ligne pour chaque élément (name, icao, country) de la liste
explodedDf = df.select(F.explode(df["airlines"]))

#creer un new dataframe airlines avec les noms de colonnes name, icao, country de la structure de données
airlines = explodedDf.select(
    F.col("col.name"),
    F.col("col.icao"),
    F.col("col.country")
)

airlines_filtered = airlines.filter(
    (F.col("name").isNotNull() & (F.col("name") != "")) &
    (F.col("icao").isNotNull() & (F.col("icao") != "")) &
    (F.col("country").isNotNull() & (F.col("country") != ""))
)




# Chemin dans le stockage objet pour le fichier "airports.json"
airports_json_minio_path = 'airports.json'
# Chemin local pour sauvegarder le fichier téléchargé
local_airports_json_path = './airports.json'

try:
    # Télécharger le fichier depuis MinIO
    minioClient.fget_object('flight-data', airports_json_minio_path, local_airports_json_path)

    print(f'Downloaded {airports_json_minio_path} to {local_airports_json_path}')
except Exception as e:
    print(e)

df = spark.read.option("multiline", "true").json(local_airports_json_path)

df = spark.read.option("multiline", "true").json('airports.json')
explodedDf = df.select(F.explode(df["airports"]))

airports = explodedDf.select(
    F.col("col.iata"),
    F.col("col.name"),
    F.col("col.continent")
)

airports_filtered = airports.filter(
    (F.col("iata").isNotNull() & (F.col("iata") != "")) &
    (F.col("name").isNotNull() & (F.col("name") != "")) &
    (F.col("continent").isNotNull() & (F.col("continent") != ""))
)

current_date = datetime.now()
year = current_date.strftime('%Y')
month = current_date.strftime('%Y-%m')
day = current_date.strftime('%Y-%m-%d')

input_path = f'bronzezone/tech_year={year}/tech_month={month}/tech_day={day}'

bucket_name = 'flight-data'

try:
    objects = minioClient.list_objects(bucket_name, prefix=input_path, recursive=True)
    for obj in objects:
        local_file_path = f'./{obj.object_name}' # télécharge le dernier fichier depuis la bucket Minio dans le reprtoire courant ou s'execute le script ./
        minioClient.fget_object(bucket_name, obj.object_name, local_file_path)
        print(f'Downloaded {obj.object_name} to {local_file_path}')
except Exception as e:
    print(e)

flights = spark.read.option('multiline', 'true').json(local_file_path)
tranformed_flights = flights.select(
    F.col("aircraft.age").alias("aircraft_age"),
    F.col("aircraft.countryId").alias("aircraft_countryId"),
    F.col("aircraft.hex").alias("aircraft_hex"),
    F.col("aircraft.model.code").alias("aircraft_model_code"),
    F.col("aircraft.model.text").alias("aircraft_model_text"),
    F.col("aircraft.msn").alias("aircraft_msn"),
    F.col("aircraft.registration"),
    F.col("airline.code.iata").alias("airline_iata"),
    F.col("airline.code.icao").alias("airline_icao"),
    F.col("airline.name").alias("airline_name"),
    F.col("airline.short").alias("airline_short"),
    F.col("airline.url").alias("airline_url"),
    F.col("origin_airport"),
    F.col("destination_airport"),
    F.col("flying").cast(T.IntegerType()),
    F.col("time.historical.delay").alias("historical_delay"),
    F.col("time.historical.flighttime").alias("historical_flighttime"),
    F.col("time.other.eta").alias("other_eta"),
    F.col("time.other.updated").alias("other_updated"),
    F.from_unixtime(F.col("time.estimated.arrival")).alias("estimated_arrival"),
    F.from_unixtime(F.col("time.estimated.departure")).alias("estimated_departure"),
    F.from_unixtime(F.col("time.real.arrival")).alias("real_arrival"),
    F.from_unixtime(F.col("time.real.departure")).alias("real_departure"),
    F.from_unixtime(F.col("time.scheduled.arrival")).alias("scheduled_arrival"),
    F.from_unixtime(F.col("time.scheduled.departure")).alias("scheduled_departure")
)

flights_filtered = tranformed_flights.dropDuplicates()

airlines_filtered.coalesce(1).write.parquet('airlines', mode='overwrite')
airports_filtered.coalesce(1).write.parquet('airports', mode='overwrite')
flights_filtered.coalesce(1).write.parquet('flights', mode='overwrite')


def upload_to_minio(directory_name, prefix):
    # Lister le contenu du répertoire spécifié
    files_in_directory = os.listdir(directory_name)
    print("lister le contenu du repertoir specifie " + str(files_in_directory))

    # Filtrez tous les sous-répertoires et fichiers non-Parquet, en ne conservant que les fichiers Parquet
    parquet_files_only = [f for f in files_in_directory if
                          os.path.isfile(os.path.join(directory_name, f)) and f.endswith('.parquet')]

    if not parquet_files_only:
        print(f"No Parquet files found in {directory_name}")
        return

    for file_name in parquet_files_only:
        # Construction de chemin d'accès au fcihier
        file_path = os.path.join(directory_name, file_name)
        print("le file path est :" + file_path)

        # Construction du nom de l'objet dans MinIO
        object_name = f'{prefix}/{file_name}'

        print("le nom de l'object est : " + object_name)

        # Téléchargez le fichier sur MinIO
        minioClient.fput_object(bucket_name, object_name, file_path)
        print(f'Uploaded {file_path} to {object_name}')


# appel de la fonction pour chacun de nos répertoires
upload_to_minio('airlines', f'silverzone/tech_year={year}/tech_month={month}/tech_day={day}/airlines')
upload_to_minio('airports', f'silverzone/tech_year={year}/tech_month={month}/tech_day={day}/airports')
upload_to_minio('flights', f'silverzone/tech_year={year}/tech_month={month}/tech_day={day}/flights')
