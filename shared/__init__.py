def extract_data(spark, file_path, file_format="csv", header_option=True):
    """
     This function will extracts data from file and return a Spark Dataframe
    :param spark: Current SparkSession object
    :param file_path: Contains the path of the file from where data needs to be extracted
    :param file_format: File Format of the datasource. Default is set to "csv".
    :param header_option: Consider first row as header. Default is set to true.
    :return: DataFrame containing the extracted data
    """
    return spark.read.format(file_format).option("header", header_option).load(file_path)


def load_data(final_df, destination_dir, save_mode="overwrite", output_format="csv", coalesce_value=1):
    """
    This function will save the dataframe to a target location
    :param final_df: Final Dataframe to be written to file.
    :param destination_dir: Destination directory where the file will be created.
    :param save_mode:  Specifies how to handle existing data if present. Default is set to "overwrite".
    :param output_format: Format of the output File. Default is set to "parquet".
    :param coalesce_value: Set the number of part output file. Default is set to 1.
    :return: None
    """
    final_df.coalesce(coalesce_value).write.mode(save_mode).format(output_format).save(destination_dir)

