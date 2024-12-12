import os
import zipfile
import boto3
import subprocess

# Configuración de credenciales para LocalStack
aws_access_key_id = "test"
aws_secret_access_key = "test"
aws_session_token = "test"
endpoint_url = "http://localhost:4566"

# Crear un cliente de S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    endpoint_url=endpoint_url
)

# Nombre del bucket y la carpeta en S3
bucket_name = "practica"
s3_folder = "codigo/"

# Verificar si el bucket existe
def check_bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except Exception:
        return False

# Crear el bucket si no existe
if not check_bucket_exists(bucket_name):
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' creado.")
else:
    print(f"Bucket '{bucket_name}' ya existe.")

# Ruta donde se encuentran los archivos .py
source_folder = "C:/Users/Usuario/Desktop/UNIVERSIDAD/CUARTOANO/PRIMERCUATRI/TSCD/Trabajo_Final/Primer_contacto"

# Nombre del archivo zip
zip_filename = "codigo_scripts.zip"

# Crear un archivo zip con los archivos .py
with zipfile.ZipFile(zip_filename, 'w') as zipf:
    for foldername, subfolders, filenames in os.walk(source_folder):
        for filename in filenames:
            if filename.endswith('.py'):
                file_path = os.path.join(foldername, filename)
                zipf.write(file_path, os.path.relpath(file_path, source_folder))
print(f"Archivo zip '{zip_filename}' creado con los archivos .py.")

# Subir el archivo zip a la carpeta 'codigo/' en S3
s3.upload_file(zip_filename, bucket_name, s3_folder + zip_filename)
print(f"Archivo '{zip_filename}' subido a S3 en la carpeta '{s3_folder}'.")

# Función para ejecutar un script .py
def run_python_script(script_name):
    try:
        result = subprocess.run(['python', script_name], check=True, capture_output=True, text=True)
        print(f"Ejecutando {script_name}:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar {script_name}: {e.output}")

# Ejecutar los scripts en el orden especificado
scripts_to_run = ['Search_datalake_s3.py', 'Create_datamart1.py', 'Create_datamart2.py', 'Functions_Search_API.py', 'app.py']
for script in scripts_to_run:
    run_python_script(os.path.join(source_folder, script))


