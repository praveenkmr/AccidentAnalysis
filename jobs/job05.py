import pyspark.sql.functions as f
from shared import extract_data, load_data
from pyspark.sql.window import Window


def analyse_data(raw_primary_person_use_df, raw_units_use_df):
    """
    This Function contain the main logic for the analysis
    :param raw_primary_person_use_df: Source Primary_person_use Dataframe
    :param raw_units_use_df: Source Units_Use Dataframe
    :return: Final Processed Dataframe
    """
    w = Window().partitionBy(f.col("VEH_BODY_STYL_ID")).orderBy(f.col("count").desc())

    return raw_primary_person_use_df.join(raw_units_use_df, on=["CRASH_ID", "UNIT_NBR"], how="inner") \
        .where(~f.col("VEH_BODY_STYL_ID").isin("NA", "UNKNOWN","NOT REPORTED")) \
        .where(~f.col("PRSN_ETHNICITY_ID").isin("NA","UNKNOWN")) \
        .groupBy(["VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"]) \
        .count() \
        .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID","count") \
        .withColumn('row_num', f.row_number().over(w)) \
        .where("row_num==1") \
        .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")\
        .orderBy("VEH_BODY_STYL_ID")


def run_job(spark, config):
    """
    This Function will execute the Analytics job 5
    :param spark: SparkSession Object
    :param config: Config file which contain all the parameters
    :return: None
    """
    load_data(
        analyse_data(extract_data(spark, f"{config.get('source_data_path')}/Primary_Person_use.csv"),
                     extract_data(spark, f"{config.get('source_data_path')}/Units_use.csv")),
        f"{config.get('target_data_path')}/job05")
