""""""
from pyspark import SparkFiles # SparkConf,
from pathlib import Path
from json import loads
from downloader import _download
from file_interpreter import _file_interpreter
from pyspark_utils import pandas_to_spark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import lit


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    tmp_dir = Path(SparkFiles.getRootDirectory())

    file_path = next(tmp_dir.glob('*.json'))
    data = None
    with open(file_path, 'rt') as fl:
        data = loads(fl.read())

    _download(data[0], tmp_dir)
    extracted = _file_interpreter(data[0], tmp_dir)
    print('Extracted: ', extracted)

    for file in extracted.get('files'):
        if not file.get('file_name'):
            continue

        print('Preparing to upload ...', file)
        df = pd.read_csv(str(tmp_dir / Path(file.get('file_name')).name), encoding='latin-1', dtype=str)
        spark_df = pandas_to_spark(df) 
        for col in spark_df.columns:
            spark_df = spark_df.withColumnRenamed(col, col.upper())

        print('Uploading ...')
        spark_df.withColumn("SOURCE_FILE", lit(Path(file.get('file_name')).name))\
            .withColumn("DATE_FILE", lit(file.get('date').replace('|', '')))\
            .withColumn("CATEGORIA", lit(file.get('title')))\
            .write.format("jdbc")\
            .option("url", "jdbc:mysql://34.132.92.199:3306/DENUE?useUnicode=true&characterEncoding=UTF-8&useSSL=false")\
            .option("driver", "com.mysql.jdbc.Driver")\
            .option("dbtable", "UNIDADES_ECONOMICAS_STG")\
            .option("user", "root")\
            .option("password", "challenge")\
            .save()
