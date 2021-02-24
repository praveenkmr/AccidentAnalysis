import pyspark.sql.functions as f
from shared import extract_data, load_data


def analyse_data(raw_units_use_df, raw_damages_use_df):
    """
    This Function contain the main logic for the analysis
    :param raw_units_use_df: Source Units_Use Dataframe
    :param raw_damages_use_df: Source Damages_use Dataframe
    :return: Final Processed Dataframe
    """
    intermediate_unit_df = raw_units_use_df \
        .select("CRASH_ID" , "VEH_DMAG_SCL_1_ID", "VEH_DMAG_SCL_2_ID","FIN_RESP_TYPE_ID") \
        .withColumn("DAMAGE_LEVEL_1", f.regexp_extract("VEH_DMAG_SCL_1_ID", "([A-Z])+ ([0-9])", 2)) \
        .withColumn("DAMAGE_LEVEL_2", f.regexp_extract("VEH_DMAG_SCL_2_ID", "([A-Z])+ ([0-9])", 2)) \
        .where(((f.col("DAMAGE_LEVEL_1") > 4) | (f.col("DAMAGE_LEVEL_2") > 4)) & (f.col("FIN_RESP_TYPE_ID").like("%INSURANCE%"))) \
        .selectExpr("CRASH_ID as unit_crash_id").persist()

    return intermediate_unit_df \
        .join(raw_damages_use_df, intermediate_unit_df.unit_crash_id==raw_damages_use_df.CRASH_ID, how="LEFT") \
        .where(raw_damages_use_df.CRASH_ID.isNull()) \
        .selectExpr("unit_crash_id as CRASH_ID").distinct()


def run_job(spark, config):
    """
    This Function will execute the Analytics job 7
    :param spark: SparkSession Object
    :param config: Config file which contain all the parameters
    :return: None
    """
    load_data(
        analyse_data(extract_data(spark, f"{config.get('source_data_path')}/Units_use.csv"),
                     extract_data(spark, f"{config.get('source_data_path')}/Damages_use.csv")
        ),
        f"{config.get('target_data_path')}/job07"
    )
