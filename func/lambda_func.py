import json
import boto3
import os


# Используем твое имя таблицы из терминала
TABLE_NAME = 'FileCount'

# Определяем URL для Localstack
endpoint = "http://localstack:4566" if os.environ.get('LOCALSTACK_HOSTNAME') else "http://localhost:4566"
dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint)
table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    # Получаем имя файла из события S3
    key = event['Records'][0]['s3']['object']['key']
    batch_id = "current_batch"

    print(f"--- Event: File {key} arrived ---")

    # Определяем тип файла
    file_type = ""
    if "raw_database" in key.lower():
        file_type = "raw"
    elif "metrics_spark" in key.lower():
        file_type = "spark"

    if not file_type:
        print(f"Skipping: {key} (not raw_database or metrics_spark)")
        return {"statusCode": 200}

    # 1. Записываем 'True' для пришедшего файла
    table.update_item(
        Key={'BatchId': batch_id},
        UpdateExpression=f"SET {file_type}_received = :val",
        ExpressionAttributeValues={':val': True}
    )

    # 2. Читаем актуальное состояние из базы
    res = table.get_item(Key={'BatchId': batch_id})
    item = res.get('Item', {})

    raw_ok = item.get('raw_received', False)
    spark_ok = item.get('spark_received', False)

    print(f"Status in DB: raw={raw_ok}, spark={spark_ok}")

    # 3. Если оба файла на месте — празднуем!
    if raw_ok and spark_ok:
        print("!!! SUCCESS: BOTH FILES ARE PRESENT !!!")
        # Здесь в будущем будет вызов Pandas

        # Сбрасываем флаги для следующего прогона Airflow
        table.put_item(Item={'BatchId': batch_id, 'raw_received': False, 'spark_received': False})
    else:
        print("Still waiting for the second part of the pair...")

    return {'statusCode': 200}