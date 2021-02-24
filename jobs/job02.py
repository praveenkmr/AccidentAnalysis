from shared import extract_data, load_data
from pyspark.sql.types import IntegerType


def analyse_data(spark,raw_primary_person_use_df):
    """
    This Function contain the main logic for the analysis
    :param spark: SparkSession Object
    :param raw_primary_person_use_df: Source Dataframe
    :return: Final Processed Dataframe
    """
    two_wheeler_booked_for_crashes = raw_primary_person_use_df \
        .where("PRSN_TYPE_ID=='DRIVER OF MOTORCYCLE TYPE VEHICLE'") \
        .count()

    return spark.createDataFrame([two_wheeler_booked_for_crashes], IntegerType())


def run_job(spark, config):
    """
    This Function will execute the Analytics job 2
    :param spark: SparkSession Object
    :param config: Config file which contain all the parameters
    :return: None
    """
    load_data(analyse_data(spark,extract_data(spark, f"{config.get('source_data_path')}/Primary_Person_use.csv")),f"{config.get('target_data_path')}/job02")

