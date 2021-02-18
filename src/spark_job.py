"""Teste de Engenharia de Dados - Matheus Oliveira."""
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import json
import boto3


BUCKET = 'teste-eng-dados'
INPUT_PREFIX = 'data/input/users/load.csv'
TYPES_MAPPING = 'config/types_mapping.json'
OUTPUT_PREFIX = 'data/output/output.parquet'


def _read_json_s3(bucket: str, prefix: str) -> dict:
    """Read json from S3 and load into dict.
    
    params:
    bucket -- S3 Bucket
    prefix -- S3 Prefix for file
    
    outputs:
    dict
    """
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, prefix)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def make_spark_session(appName: str) -> SparkSession:
    """Create Spark Session.
    
    params:
    appName -- Spark App Name
    
    outputs:
    SparkSession
    """
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def read_data(bucket: str, input_prefix: str, map_prefix: str) -> DataFrame:
    """Read data from S3 and apply type mapping.
    
    params:
    bucket -- S3 Bucket
    input_path -- S3 Prefix for input file
    map_path -- S3 Prefix for map file
    """
    input_path = 's3://' + bucket + '/' + input_prefix
    df = spark.read.csv(input_path, header=True)
    
    # Fix Columns
    df = df.withColumnRenamed('name', 'email2') \
        .withColumnRenamed('email', 'name') \
        .withColumnRenamed('email2', 'email')

    type_map = _read_json_s3(bucket, map_prefix)
    type_map['id'] = 'integer'
    type_map['email'] = 'string'
    type_map['name'] = 'string'
    type_map['phone'] = 'string'
    type_map['address'] = 'string'
    
    df = df.select([col(k).cast(v) for k, v in type_map.items()])
    
    return df


def transform_data(df: DataFrame) -> DataFrame:
    """Apply data deduplication.
    
    params:
    df -- Spark DataFrame
    
    outputs:
    Spark DataFrame
    """
    w = Window.partitionBy('id').orderBy(col('update_date').desc())

    df = df.withColumn('rownum', row_number().over(w)) \
        .where(col('rownum') == 1) \
        .drop('rownum')
    
    return df

def write_parquet(df: DataFrame, bucket: str, output_prefix: str) -> None:
    """Write DataFrame to .parquet and save to S3.
    
    params:
    df -- Spark DataFrame
    bucket -- S3 Bucket
    output_prefix -- S3 Output file Prefix
    """
    output_path = 's3://' + bucket + '/' + output_prefix
    df.write.mode('overwrite').parquet(output_path)


if __name__ == '__main__':
    spark = make_spark_session('Teste_Cognitivo')
    df = read_data(BUCKET, INPUT_PREFIX, TYPES_MAPPING)
    df = transform_data(df)
    write_parquet(df, BUCKET, OUTPUT_PREFIX)
