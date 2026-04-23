import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date, monotonically_increasing_id

RAW_BUCKET = os.getenv('S3_BUCKET_NAME', 'wistia-analytics-raw-joe')
OUT_BUCKET = os.getenv('S3_PROCESSED_BUCKET', 'wistia-analytics-processed-joe')
MEDIA_IDS  = ['gskhw4w4lm', 'v08dlrgr7v']

spark = (
    SparkSession.builder
    .appName('WistiaTransform')
    .config('spark.sql.legacy.timeParserPolicy', 'LEGACY')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

def read_json(bucket, prefix, media_id):
    path = f's3://{bucket}/{prefix}/{media_id}/*.json'
    print(f'Reading: {path}')
    return spark.read.option('multiline', True).json(path)

def write_parquet(df, bucket, folder):
    path = f's3://{bucket}/{folder}/'
    df.write.mode('overwrite').parquet(path)
    print(f'Written {df.count()} rows -> {path}')

def transform_media():
    print('\n--- dim_media ---')
    dfs = []
    for mid in MEDIA_IDS:
        df = read_json(RAW_BUCKET, 'media', mid)
        df = df.withColumn('source_media_id', lit(mid)).select(
            col('source_media_id').alias('media_id'),
            col('play_count').cast('integer'),
            col('play_rate').cast('double'),
            col('hours_watched').cast('double').alias('total_watch_time'),
            col('engagement').cast('double').alias('avg_engagement_rate'),
            col('visitors').cast('integer').alias('visitor_count'),
            col('load_count').cast('integer'),
            current_timestamp().alias('ingested_at')
        )
        dfs.append(df)
    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)
    write_parquet(result.dropDuplicates(['media_id']), OUT_BUCKET, 'dim_media')

def transform_visitors():
    print('\n--- dim_visitor ---')
    dfs = []
    for mid in MEDIA_IDS:
        df = read_json(RAW_BUCKET, 'visitors', mid).select(
            col('visitor_key').alias('visitor_id'),
            col('ip').alias('ip_address'),
            col('country'),
            col('created_at').alias('first_seen_at'),
        )
        dfs.append(df)
    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)
    write_parquet(result.dropDuplicates(['visitor_id']), OUT_BUCKET, 'dim_visitor')

def transform_fact_engagement():
    print('\n--- fact_engagement ---')
    dfs = []
    for mid in MEDIA_IDS:
        df = read_json(RAW_BUCKET, 'visitors', mid)
        df = df.withColumn('media_id', lit(mid)).select(
            col('media_id'),
            col('visitor_key').alias('visitor_id'),
            to_date(col('created_at')).alias('date'),
            col('play_count').cast('integer'),
            col('load_count').cast('integer'),
            current_timestamp().alias('ingested_at')
        )
        dfs.append(df)
    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)
    result = result.withColumn('engagement_id', monotonically_increasing_id())
    write_parquet(result, OUT_BUCKET, 'fact_engagement')

def run():
    print('=== Wistia PySpark Transformation Starting ===')
    transform_media()
    transform_visitors()
    transform_fact_engagement()
    print('\n=== Transformation Complete ===')
    spark.stop()

if __name__ == '__main__':
    run()
