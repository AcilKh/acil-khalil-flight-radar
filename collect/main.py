from pyspark.sql import SparkSession, functions as F, Window
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
    .config('spark.app.name', 'analyser') \
    .getOrCreate()

minioClient = initialize_minio_client()

current_date = datetime.now()
year = current_date.strftime('%Y')
month = current_date.strftime('%Y-%m')
day = current_date.strftime('%Y-%m-%d')

flights_path = f'silverzone/tech_year={year}/tech_month={month}/tech_day={day}/flights'
airports_path = f'silverzone/tech_year={year}/tech_month={month}/tech_day={day}/airports'
airlines_path = f'silverzone/tech_year={year}/tech_month={month}/tech_day={day}/airlines'

bucket_name = 'flight-data'

try:
    objects = minioClient.list_objects(bucket_name, prefix=flights_path, recursive=True)
    for obj in objects:
        local_file_path = f'./{obj.object_name}'
        minioClient.fget_object(bucket_name, obj.object_name, local_file_path)
        print(f'Downloaded {obj.object_name} to {local_file_path}')
        flights = spark.read.parquet(local_file_path)
except Exception as e:
    print(e)



try:
    objects = minioClient.list_objects(bucket_name, prefix=airports_path, recursive=True)
    for obj in objects:
        local_file_path = f'./{obj.object_name}'
        minioClient.fget_object(bucket_name, obj.object_name, local_file_path)
        print(f'Downloaded {obj.object_name} to {local_file_path}')

        airports = spark.read.parquet(local_file_path)
except Exception as e:
    print(e)


try:
    objects = minioClient.list_objects(bucket_name, prefix=airlines_path, recursive=True)
    for obj in objects:
        local_file_path = f'./{obj.object_name}'
        minioClient.fget_object(bucket_name, obj.object_name, local_file_path)
        print(f'Downloaded {obj.object_name} to {local_file_path}')
        airlines = spark.read.parquet(local_file_path)
except Exception as e:
    print(e)



# airlines.show(truncate=False)
# airports.show(truncate=False)
# flights.show(truncate=False)


question1= flights \
    .filter(F.col("flying") == 1) \
    .groupBy("airline_name") \
    .agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")) \
    .limit(1)

question1.show()


joined_df = (
    flights
    .join(airlines, flights["airline_icao"] == airlines["icao"])
    .join(
        airports.alias("origin_airports").withColumnRenamed("continent", "origin_continent"),
        flights["origin_airport"] == F.col("origin_airports.iata")
    )
    .join(
        airports.alias("destination_airports").withColumnRenamed("continent", "destination_continent"),
        flights["destination_airport"] == F.col("destination_airports.iata")
    )
)

print("Joined DataFrame:")
joined_df.show()

regional_flights_df = joined_df.filter(joined_df["origin_continent"] == joined_df["destination_continent"])
print("Regional Flights DataFrame:")
regional_flights_df.show()
window_spec = Window.partitionBy("origin_continent").orderBy(F.desc("flight_count"))

count_df = (
    regional_flights_df
    .groupBy("origin_continent", "airline_name")
    .agg(F.count("*").alias("flight_count"))
    .withColumn("rank", F.dense_rank().over(window_spec))
)
print("Count DataFrame:")
count_df.show()

question2 = count_df.filter(count_df["rank"] == 1).drop("rank")

question2.show()

question3 = flights \
    .select("registration", "origin_airport", "destination_airport", "scheduled_arrival", "real_departure") \
    .filter(F.col("flying") == 1) \
    .withColumn("duration_hours", F.round(
    ((F.unix_timestamp(F.col("scheduled_arrival")) - F.unix_timestamp(F.col("real_departure"))) / 3600), 2))

max_value = question3.agg(F.max(F.col("duration_hours"))).collect()[0][0]

row_with_max_value = question3.filter(F.col("duration_hours") == max_value)
row_with_max_value.show()
question3 = row_with_max_value.distinct()

joined_df = (
    flights
    .join(airports.alias("origin_airports"), flights["origin_airport"] == F.col("origin_airports.iata"))
    .join(airports.alias("destination_airports"), flights["destination_airport"] == F.col("destination_airports.iata"))
)

active_flights_df = joined_df.filter(joined_df["flying"] == 1)

with_duration_df = active_flights_df.withColumn(
    "duration",
    (F.unix_timestamp("scheduled_arrival") - F.unix_timestamp("scheduled_departure")) / 3600
)

question4 = (
    with_duration_df
    .groupBy(F.col("origin_airports.continent"))
    .agg(F.avg("duration").alias("avg_duration"))
)

question4.show()

active_flights = flights.filter(flights.flying == 1)
active_flights = active_flights.withColumn('construction_company', F.split(active_flights.aircraft_model_text, ' ')[0])
active_flights_count = active_flights.groupBy('construction_company').count()
question5 = active_flights_count.orderBy(F.desc('count')).limit(1)
question5.show()

joined_df = flights.join(airlines, flights.airline_icao == airlines.icao)
model_counts = joined_df.groupBy('country', 'aircraft_model_text').count()
window_spec = Window.partitionBy('country').orderBy(F.desc('count'))
ranked_models = model_counts.withColumn('rank', F.row_number().over(window_spec))
question6 = ranked_models.filter(ranked_models.rank <= 3)
question6.show()


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



# Write result into Minio

question1.write.parquet('questions', mode='overwrite')
question2.write.parquet('questions', mode='overwrite')
question3.write.parquet('questions', mode='overwrite')
question4.write.parquet('questions', mode='overwrite')
question5.write.parquet('questions', mode='overwrite')
question6.write.parquet('questions', mode='overwrite')

upload_to_minio('questions',f'goldzone/tech_year={year}/tech_month={month}/tech_day={day}/question1')
upload_to_minio('questions',f'goldzone/tech_year={year}/tech_month={month}/tech_day={day}/question2')
upload_to_minio('questions',f'goldzone/tech_year={year}/tech_month={month}/tech_day={day}/question3')
upload_to_minio('questions',f'goldzone/tech_year={year}/tech_month={month}/tech_day={day}/question4')
upload_to_minio('questions',f'goldzone/tech_year={year}/tech_month={month}/tech_day={day}/question5')
upload_to_minio('questions',f'goldzone/tech_year={year}/tech_month={month}/tech_day={day}/question6')
