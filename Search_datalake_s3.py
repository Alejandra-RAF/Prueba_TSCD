import asyncio
import aiohttp
import boto3
import sys

# Inicializamos el cliente S3
s3 = boto3.client('s3', endpoint_url="http://localhost:4566")  # Configuración para LocalStack
bucket_name = "lambda-deployments-bucket"  # Cambia al nombre correcto de tu bucket

async def fetch(session, id):
    """Descarga un archivo desde Gutenberg."""
    try:
        async with session.get(f"https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt") as response:
            return (await response.text(), response.status)
    except Exception as e:
        print(f"Error al descargar el archivo {id}: {e}")
        return None, 500

async def save_text(session, id):
    """Descarga y guarda un archivo en S3."""
    try:
        text, status = await fetch(session, id)
        if status != 200:
            print(f"Error: El archivo {id} no pudo descargarse. Estado HTTP: {status}")
            return

        text = text.replace("\r\n", "\n")
        s3.put_object(Bucket=bucket_name, Key=f"Datalake/{id}.txt", Body=text)
        print(f"Archivo {id}.txt subido exitosamente a S3 en el bucket '{bucket_name}'")
    except Exception as e:
        print(f"Error al procesar el archivo {id}: {e}")

async def run_async():
    """Ejecuta la lógica principal de forma asíncrona."""
    async with aiohttp.ClientSession() as session:
        for i in range(1, 3):  # Cambia el rango según tus necesidades
            await save_text(session, i)

def main(event, context):
    """Handler de Lambda."""
    print("Ejecución de Lambda iniciada...")
    asyncio.run(run_async())
    print("Ejecución de Lambda finalizada.")
    return {"statusCode": 200, "body": "Archivos subidos exitosamente a S3"}
