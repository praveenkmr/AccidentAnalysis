from shared import extract_data, load_data
from pyspark.sql.types import IntegerType


def analyse_data(spark, raw_primary_use_df):
    """
    This Function contain the main logic for the analysis
    :param spark: SparkSession Object
    :param raw_primary_use_df: Source dataframe
    :return: Final Processed Dataframe
    """
    male_death_count = raw_primary_use_df \
        .where("PRSN_GNDR_ID =='MALE' and PRSN_INJRY_SEV_ID=='KILLED'") \
        .count()

    return spark.createDataFrame([male_death_count], IntegerType())


def run_job(spark, config):
    """
    This Function will execute the Analytics job 1
    :param spark: SparkSession Object
    :param config: Config file which contain all the parameters
    :return: None
    """
    load_data(analyse_data(spark, extract_data(spark, f"{config.get('source_data_path')}/Primary_Person_use.csv")),f"{config.get('target_data_path')}/job01")
