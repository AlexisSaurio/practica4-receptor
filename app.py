import os
import time
import json
import uuid
import boto3

REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "cola-boletines")
DYNAMO_TABLE_NAME = os.getenv("DYNAMO_TABLE_NAME", "boletines_recibidos")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN") 
MOSTRADOR_BASE_URL = os.getenv("MOSTRADOR_BASE_URL", "http://localhost:8001") 

sqs_client = boto3.client('sqs', region_name=REGION)
sns_client = boto3.client('sns', region_name=REGION)
dynamo_client = boto3.resource('dynamodb', region_name=REGION)

def obtener_url_cola():
    response = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
    return response['QueueUrl']

def procesar_mensaje(mensaje, queue_url):
    try:
        cuerpo = json.loads(mensaje['Body'])
        correo = cuerpo['correo']
        texto_boletin = cuerpo['mensaje']
        imagen_url = cuerpo['imagen_url']
        
        boletin_id = str(uuid.uuid4())

        tabla = dynamo_client.Table(DYNAMO_TABLE_NAME)
        tabla.put_item(
            Item={
                'id': boletin_id,
                'correo': correo,
                'contenido': texto_boletin,
                'imagen_s3': imagen_url,
                'leido': False  
            }
        )
        print(f"Guardado en DynamoDB con ID: {boletin_id}")

        link_ver_boletin = f"{MOSTRADOR_BASE_URL}/boletines/{boletin_id}?correo={correo}"
        
        mensaje_sns = (
            f"¡Tienes un nuevo boletín!\n\n"
            f"Haz clic en el siguiente enlace para verlo y marcarlo como leído:\n"
            f"{link_ver_boletin}"
        )
        
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Nuevo Boletín Recibido",
            Message=mensaje_sns
        )
        print(f" Correo enviado a través de SNS")

        receipt_handle = mensaje['ReceiptHandle']
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print("Mensaje borrado de SQS")

    except Exception as e:
        print(f" Error procesando mensaje: {e}")

def iniciar_worker():
    print(" Iniciando el Worker del Receptor...")
    queue_url = obtener_url_cola()
    
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )

            if 'Messages' in response:
                for msg in response['Messages']:
                    print("\n--- Nuevo mensaje detectado ---")
                    procesar_mensaje(msg, queue_url)
            else:
                print("Esperando mensajes...")
                
        except Exception as e:
            print(f" Error en el worker: {e}")
            time.sleep(5) 

if __name__ == '__main__':
    iniciar_worker()