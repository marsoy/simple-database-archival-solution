import boto3
import os
import logging


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger = logging.getLogger()

REGION = os.getenv("REGION")
secret_client = boto3.client("secretsmanager", region_name=REGION)


def mask_sensitive_data(event):
    # remove sensitive data from request object before logging
    keys_to_redact = ["authorization"]
    result = {}
    for k, v in event.items():
        if isinstance(v, dict):
            result[k] = mask_sensitive_data(v)
        elif k in keys_to_redact:
            result[k] = "<redacted>"
        else:
            result[k] = v
    return result


def build_response(http_code, body):
    return {
        "headers": {
            # tell cloudfront and api gateway not to cache the response
            "Cache-Control": "no-cache, no-store",
            "Content-Type": "application/json",
        },
        "statusCode": http_code,
        "body": body,
    }


def lambda_handler(event, context):
    logger.info(mask_sensitive_data(event))
    headers = event["headers"]

    try:
        secret_value = secret_client.get_secret_value(SecretId="ARCHIVE_AUTH_TOKEN")
        AUTH_TOKEN = secret_value["SecretString"]
    except Exception as e:
        print(f"Fetching secret for ARCHIVE_AUTH_TOKEN failed with exception: {str(e)}")
        return {"isAuthorized": False}

    if not AUTH_TOKEN:
        print("Authentication token is null")
        return {"isAuthorized": False}

    authentication = f"Bearer {AUTH_TOKEN}"
    request_auth = headers["authorization"]

    if authentication != request_auth:
        return {"isAuthorized": False}

    return {"isAuthorized": True}
