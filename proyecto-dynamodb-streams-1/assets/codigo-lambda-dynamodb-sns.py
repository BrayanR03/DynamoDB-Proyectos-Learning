import json
import boto3


# Cliente de SNS
sns = boto3.client("sns")

# ARN de nuestro SNS Topic
SNS_TOPIC_ARN = "<ARN DEL topico SNS>"


def lambda_handler(event, context):

    print("===== EVENTO RECIBIDO DESDE DYNAMODB STREAM =====")

    for record in event["Records"]:

        event_name = record["eventName"]
        dynamodb = record["dynamodb"]

        print(f"EventName: {event_name}")

        # -------------------------------------------------
        # INSERT
        # -------------------------------------------------
        if event_name == "INSERT":

            new_image = dynamodb.get("NewImage", {})

            customer_id = new_image.get("CustomerId", {}).get("S", "N/A")
            order_id = new_image.get("OrderId", {}).get("S", "N/A")
            status = new_image.get("Status", {}).get("S", "N/A")
            amount = new_image.get("Amount", {}).get("N", "N/A")

            message = (
                "🆕 NUEVO PEDIDO\n\n"
                f"CustomerId: {customer_id}\n"
                f"OrderId: {order_id}\n"
                f"Status: {status}\n"
                f"Amount: {amount}"
            )

        # -------------------------------------------------
        # MODIFY
        # -------------------------------------------------
        elif event_name == "MODIFY":

            old_image = dynamodb.get("OldImage", {})
            new_image = dynamodb.get("NewImage", {})

            customer_id = new_image.get("CustomerId", {}).get("S", "N/A")
            order_id = new_image.get("OrderId", {}).get("S", "N/A")

            old_status = old_image.get("Status", {}).get("S", "N/A")
            new_status = new_image.get("Status", {}).get("S", "N/A")

            old_amount = old_image.get("Amount", {}).get("N", "N/A")
            new_amount = new_image.get("Amount", {}).get("N", "N/A")

            message = (
                "🔄 PEDIDO ACTUALIZADO\n\n"
                f"CustomerId: {customer_id}\n"
                f"OrderId: {order_id}\n\n"
                f"Estado anterior: {old_status}\n"
                f"Estado nuevo: {new_status}\n\n"
                f"Monto anterior: {old_amount}\n"
                f"Monto nuevo: {new_amount}"
            )

        # -------------------------------------------------
        # REMOVE
        # -------------------------------------------------
        elif event_name == "REMOVE":

            old_image = dynamodb.get("OldImage", {})

            customer_id = old_image.get("CustomerId", {}).get("S", "N/A")
            order_id = old_image.get("OrderId", {}).get("S", "N/A")

            message = (
                "🗑️ PEDIDO ELIMINADO\n\n"
                f"CustomerId: {customer_id}\n"
                f"OrderId: {order_id}"
            )

        else:
            print(f"Evento no contemplado: {event_name}")
            continue

        # -------------------------------------------------
        # PUBLICAR EN SNS
        # -------------------------------------------------

        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"DynamoDB - {event_name}",
            Message=message
        )

        print("Mensaje publicado correctamente en SNS.")
        print("MessageId:", response["MessageId"])

    return {
        "statusCode": 200,
        "body": json.dumps("Procesamiento completado")
    }