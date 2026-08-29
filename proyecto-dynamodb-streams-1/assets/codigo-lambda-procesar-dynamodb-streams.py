import json

def lambda_handler(event, context):

    print("===== EVENTO RECIBIDO DESDE DYNAMODB STREAM =====")

    for record in event["Records"]:
        print("EventID:", record["eventID"])
        print("EventName:", record["eventName"])
        print("DynamoDB:", json.dumps(record["dynamodb"], default=str))

    return {
        "statusCode": 200
    }