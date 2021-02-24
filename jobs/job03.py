from shared import extract_data, load_data
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

def analyse_data(spark, raw_primary_person_use_df):
    """
    This Function contain the main logic for the analysis
    :param spark : SparkSession
    :param raw_primary_person_use_df: Source Dataframe
    :return: Final Processed Dataframe
    """
    highest_num_female_accidents_city = raw_primary_person_use_df \
        .where("PRSN_GNDR_ID == 'FEMALE'") \
        .groupBy("DRVR_LIC_STATE_ID") \
        .count() \
        .select('DRVR_LIC_STATE_ID', f.col('count')) \
        .orderBy(f.col("count").desc()) \
        .first()
    return spark.createDataFrame(highest_num_female_accidents_city,StringType())


def run_job(spark, config):
    """
    This Function will execute the Analytics job 3
    :param spark: SparkSession Object
    :param config: Config file which contain all the parameters
    :return: None
    """
    load_data(
        analyse_data(spark,extract_data(spark, f"{config.get('source_data_path')}/Primary_Person_use.csv")),
        f"{config.get('target_data_path')}/job03"
    )
