import os
import shutil
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from pyspark.sql.types import IntegerType, DoubleType
from sqlalchemy.sql.functions import coalesce


@dag(
    dag_id="waiting_files",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),  # start_date обязателен для запуска
    catchup=False,
    description="This dag waiting file in folder --DATA--",
    tags=["sensor_folder"]
)
def bike_pipe():
    @task
    #create bucket on localstack
    def create_bucket():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        bucket_name = 'tbucket'
        hook = S3Hook(aws_conn_id='aws_localstack')
        if not hook.check_for_bucket(bucket_name):
            hook.create_bucket(bucket_name)
        else:
            print('tbucket is exist')
        return bucket_name
    @task
    def set_notification_func():
        import boto3

        s3 = boto3.client(
            's3',
            endpoint_url="http://host.docker.internal:4566",
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        config = {
            'LambdaFunctionConfigurations': [{
                'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:000000000000:function:my-first-lambda',
                'Events': ['s3:ObjectCreated:*']
            }]
        }
        s3.put_bucket_notification_configuration(Bucket='tbucket', NotificationConfiguration=config)
    #scanning folder
    scan_folder = FileSensor(
        task_id="scan_folder",
        fs_conn_id="fs_default",
        filepath="database.csv",
        poke_interval=5
    )

    #access log
    log_success = BashOperator(
        task_id='bash_log_message',
        bash_command='echo "Файл успешно найден! Начинаю обработку в $(date)"'
    )

    @task
    #checking file in bucket
    def upload_csv_to_s3_if_not_exists(file_path, bucket_name, s3_key):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id='aws_localstack')
        if hook.check_for_key(key=s3_key, bucket_name=bucket_name):
            return False
        else:
            hook.load_file(
                filename='/opt/airflow/data/database.csv',
                key='raw_database.csv',
                bucket_name=bucket_name,
                replace=True
            )
            return "File uploaded successfully"

    @task
    def save_spark_data():
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F
        spark = SparkSession.builder \
            .appName("StationMetrics") \
            .getOrCreate()
        df = spark.read.csv('/opt/airflow/data/database.csv', header=True, inferSchema=True)
        df = df.withColumn(
            "departure_id",
            F.regexp_replace(F.col("departure_id"), r"\.0$", "")
        ).withColumn(
            "return_id",
            F.regexp_replace(F.col("return_id"), r"\.0$", "")
        )
        metric_1 = df.groupby('departure_name','departure_id').count()\
            .withColumnRenamed("count", "departure_count")\
            .orderBy(F.desc("departure_count"))
        metric_2 = df.groupby('return_name','return_id').count()\
            .withColumnRenamed("count", "return_count")\
            .orderBy(F.desc("return_count"))
        combinned_df = metric_1.join(metric_2, metric_1.departure_id==metric_2.return_id, 'full_outer') \
            .select(
            F.coalesce(F.col("departure_name"), F.col("return_name")).alias("station_name"),
            F.col("departure_count"), F.lit(0),
            F.col("return_count"), F.lit(0) )
        path = '/opt/airflow/data/metrics'
        final_path = '/opt/airflow/data/spark_metrics.csv'
        combinned_df.coalesce(1).write.mode('overwrite').option('header','True').csv(path)
        file = [f for f in os.listdir(path) if f.endswith('.csv')]
        if file:
            shutil.move(os.path.join(path, file[0]), final_path)
            shutil.rmtree(path)
        return final_path

    @task
    def upload_to_s3(path, key, bucket):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id='aws_localstack')
        hook.load_file(
            filename=f'{path}',
            key=f'{key}',
            bucket_name=f'{bucket}',
            replace=True
        )
        return 'done'
    set_notification = set_notification_func()
    bucket_name_t = create_bucket()
    save_spark_data = save_spark_data()
    upload_csv_to_s3 = upload_csv_to_s3_if_not_exists(
        '/opt/airflow/data/database.csv',
        f"{bucket_name_t}",
        'raw_database.csv'
    )
    upload_metric = upload_to_s3(
        f'{save_spark_data}',
        'metrics_spark',
        f'{bucket_name_t}'
    )

    # Устанавливаем зависимости
    bucket_name_t >> set_notification >> scan_folder >> log_success >> upload_csv_to_s3 >> save_spark_data >> upload_metric


# Регистрация DAG
bike_pipe()