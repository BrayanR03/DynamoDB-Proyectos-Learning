
import boto3
import logging
from boto3.dynamodb.types import TypeDeserializer

# Configuración de logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Deserializador de DynamoDB Streams
deserializer = TypeDeserializer()

# Cliente DynamoDB de N. Virginia
dynamodb = boto3.resource(
    "dynamodb",
    region_name="us-east-1"
)

table = dynamodb.Table("transactions_back_us_east_1")


def deserialize_item(item):
    return {
        key: deserializer.deserialize(value)
        for key, value in item.items()
    }


def lambda_handler(event, context):

    records = event.get("Records", [])

    logger.info(
        f"Eventos recibidos desde DynamoDB Streams: {len(records)}"
    )

    for record in records:

        event_name = record["eventName"]

        # INSERT: nueva transacción
        if event_name == "INSERT":

            new_image = deserialize_item(
                record["dynamodb"]["NewImage"]
            )

            table.put_item(Item=new_image)

            logger.info(
                f"INSERT replicado: {new_image['transaction_id']}"
            )

        # MODIFY: actualización de transacción
        elif event_name == "MODIFY":

            new_image = deserialize_item(
                record["dynamodb"]["NewImage"]
            )

            table.put_item(Item=new_image)

            logger.info(
                f"MODIFY replicado: {new_image['transaction_id']}"
            )

        # REMOVE: eliminación de transacción
        elif event_name == "REMOVE":

            keys = deserialize_item(
                record["dynamodb"]["Keys"]
            )

            table.delete_item(
                Key={
                    "transaction_id": keys["transaction_id"]
                }
            )

            logger.info(
                f"REMOVE replicado: {keys['transaction_id']}"
            )

    return {
        "statusCode": 200,
        "records_processed": len(records),
        "message": "Replicación multirregional completada"
    }
