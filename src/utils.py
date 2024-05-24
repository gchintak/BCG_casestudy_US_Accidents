from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('reading_csv_files').getOrCreate()


def load_csv_data_to_df(file_path):
    """
    Read CSV data
    return: dataframe
    """
    return spark.read.csv(file_path, header=True)

def save_csv(df,file_path):
    df.write.csv(file_path, header=True, mode='overwrite')