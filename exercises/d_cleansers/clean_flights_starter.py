# Exercise:
# “clean” a CSV file using PySpark.
# * Grab sample data from https://packages.revolutionanalytics.com/datasets/AirlineSubsetCsv.tar.gz
#   A copy of the data can be found on our S3 bucket (link shared in class).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with PySpark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective)
#   - fix data types
#   - flag missing or unknown data
#   - remove redundant data
# * Write the data to a parquet file. How big is the parquet file compared
#   to the compressed csv file? And compared to the uncompressed csv file?
#   How long does your processing take?
# * While your job is running, open up the Spark UI and get a feel for what's
#   there (together with the instructor).
# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType


def read_data(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(
        str(path),
        sep=',',
        header=True
    )


def clean(frame: DataFrame) -> DataFrame:
    frame = frame.drop('MONTH', 'DAY_OF_MONTH', 'CANCELLED', 'ORIGIN_AIRPORT_ID', 'DEST_AIRPORT_ID', 'ARR_DEL15')
    frame = frame.drop('DEP_DEL15', 'DISTANCE_GROUP', 'ARR_DELAY_GROUP', 'DEP_DELAY_GROUP', 'DEP_TIME', 'ARR_TIME', 'ACTUAL_ELAPSED_TIME')
    frame = frame.drop('_c44', 'FLIGHTS')
    frame = frame.withColumnRenamed("FL_DATE", "FLIGHT_DATE")
    frame = frame.withColumnRenamed("TAIL_NUM", "TAIL_ID")
    frame = frame.withColumnRenamed("FL_NUM", "FLIGHT_NUMBER")
    frame = frame.withColumnRenamed("CRS_DEP_TIME", "SCHEDULED_DEPARTURE_TIME")
    frame = frame.withColumnRenamed("CRS_ARR_TIME", "SCHEDULED_ARRIVAL_TIME")
    frame = frame.withColumnRenamed("FL_NUM", "FLIGHT_NUMBER")
    frame = frame.withColumnRenamed("CRS_ELAPSED_TIME", "SCHEDULED_DURATION")
    frame = frame.withColumn("FLIGHT_NUMBER",frame['FLIGHT_NUMBER'].cast('int'))
    frame = frame.withColumn("DEP_DELAY",frame['DEP_DELAY'].cast('int'))
    frame = frame.withColumn("DEP_DELAY_NEW",frame['DEP_DELAY_NEW'].cast('int'))
    frame = frame.withColumn("TAXI_OUT",frame['TAXI_OUT'].cast('int'))
    frame = frame.withColumn("TAXI_IN",frame['TAXI_IN'].cast('int'))
    frame = frame.withColumn("ARR_DELAY",frame['ARR_DELAY'].cast('int'))
    frame = frame.withColumn("ARR_DELAY_NEW",frame['ARR_DELAY_NEW'].cast('int'))
    frame = frame.withColumn("DIVERTED",frame['DIVERTED'].cast('int'))
    frame = frame.withColumn("SCHEDULED_DURATION",frame['SCHEDULED_DURATION'].cast('int'))
    frame = frame.withColumn("AIR_TIME",frame['AIR_TIME'].cast('int'))
    frame = frame.withColumn("DISTANCE",frame['DISTANCE'].cast('int'))
    frame = frame.withColumn("CARRIER_DELAY",frame['CARRIER_DELAY'].cast('int'))
    frame = frame.withColumn("NAS_DELAY",frame['NAS_DELAY'].cast('int'))
    frame = frame.withColumn("WEATHER_DELAY",frame['WEATHER_DELAY'].cast('int'))
    frame = frame.withColumn("DIVERTED",frame['DIVERTED'].cast(BooleanType()))
    frame = frame.withColumn("SECURITY_DELAY",frame['SECURITY_DELAY'].cast('int'))
    frame = frame.withColumn("LATE_AIRCRAFT_DELAY",frame['LATE_AIRCRAFT_DELAY'].cast('int'))
    frame = frame.withColumn("NAS_DELAY",frame['NAS_DELAY'].cast('int'))
    
    return frame


if __name__ == "__main__":
    # use relative paths, so that the location of this project on your system
    # won't mean editing paths
    
    resources_dir = Path("/workspace/effective_pyspark/exercises") / "resources"

    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    #target_dir.mkdir(exist_ok=True)

    # Extract
    frame = read_data(resources_dir / "flight" )
    carriers = read_data(resources_dir / "carriers.csv")

    # Transform
    cleaned_frame = clean(frame)
    cleaned_frame.show(5)

    # Question 1
    print("Question 1:")
    q1 = cleaned_frame.filter("""UNIQUE_CARRIER == 'AA' AND YEAR == '2011'""")
    print(q1.count())

    # Question 2
    print("Question 2:")
    q2 = q1.filter("""ARR_DELAY <= 10""")
    print(q2.count())
    
    # Question 3
    print("Question 3:")
    q3 = cleaned_frame.filter("""DEP_DELAY > 0""").select('DAY_OF_WEEK').groupBy('DAY_OF_WEEK')
    q3.count().show()

    # Question 4
    print("Question 4:")
    temp = cleaned_frame.filter("""YEAR == '2011'""")
    cd = temp.filter(cleaned_frame['CARRIER_DELAY']!= 0).count()
    wd = temp.filter(cleaned_frame['WEATHER_DELAY']!= 0).count()
    nd = temp.filter(cleaned_frame['NAS_DELAY']!= 0).count()
    sd = temp.filter(cleaned_frame['SECURITY_DELAY']!= 0).count()
    ad = temp.filter(cleaned_frame['LATE_AIRCRAFT_DELAY']!= 0).count()

    print(f'CARRIER_DELAYS: {cd}')
    print(f'WEATHER_DELAYS: {wd}')
    print(f'NAS_DELAYS: {nd}')
    print(f'SECURITY_DELAYS: {sd}')
    print(f'AIRCRAFT_DELAYS: {ad}')

    # calculate stats
    stat_frame = cleaned_frame.groupBy('UNIQUE_CARRIER').avg('CARRIER_DELAY')
    w = Window.partitionBy('CARRIER').orderBy(carriers['START_DATE_SOURCE'].desc())

    #adding rownumber to the data
    carriers = carriers.withColumn("rn", f.row_number().over(w))
    carriers = carriers.filter(carriers["rn"] == 1).select('CARRIER', 'CARRIER_NAME')

    stat_frame = stat_frame.join(carriers, stat_frame.UNIQUE_CARRIER==carriers.CARRIER).select('CARRIER_NAME', 'avg(CARRIER_DELAY)')
    stat_frame = stat_frame.sort(stat_frame['avg(CARRIER_DELAY)'].desc())
    stat_frame.show(10)