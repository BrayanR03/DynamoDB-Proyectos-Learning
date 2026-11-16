import boto3
import random
import uuid
from datetime import datetime, timezone
from decimal import Decimal

# DynamoDB - Región Ohio (us-east-2)
dynamodb = boto3.resource(
    "dynamodb",
    region_name="us-east-2"
)

table = dynamodb.Table("transactions")


def lambda_handler(event, context):

    transaction_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    transaction_status = ["COMPLETED", "PENDING", "REJECTED"]
    channels = ["MOBILE", "WEB", "AGENCY", "ATM"]
    currencies = ["PEN", "USD"]

    # Generación de 5 transacciones aleatorias
    transactions = []

    for _ in range(5):

        transaction_date = datetime.now(timezone.utc).isoformat()

        transaction = {
            "transaction_id": f"TXN-{uuid.uuid4()}",
            "account_id": f"ACC-{random.randint(10000, 99999)}",
            "transaction_date": transaction_date,
            "transaction_type": random.choice(transaction_types),
            "amount": Decimal(str(round(random.uniform(10, 10000), 2))),
            "currency": random.choice(currencies),
            "status": random.choice(transaction_status),
            "description": random.choice([
                "Transferencia interbancaria",
                "Depósito en cuenta",
                "Retiro de efectivo",
                "Pago de servicios",
                "Transferencia entre cuentas"
            ]),
            "channel": random.choice(channels),
            "created_at": transaction_date,
            "updated_at": transaction_date
        }

        # Escritura en DynamoDB
        table.put_item(Item=transaction)

        transactions.append(transaction)
        
    return {
        "statusCode": 200,
        "body": {
            "message": "Transacciones registradas correctamente",
            "region": "us-east-2",
            "records_inserted": len(transactions),
            "transactions": transactions
        }
    }