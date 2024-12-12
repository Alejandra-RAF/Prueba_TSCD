import boto3
import re
from collections import Counter

# Función para obtener archivos desde S3
def get_s3_files(bucket_name, prefix):
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [obj['Key'] for obj in objects.get('Contents', [])]

# Función para preprocesar las palabras
def preprocesado(texto):
    palabras = texto.split()
    palabras_filtradas = []
    
    for palabra in palabras:
        # Filtrar palabras que contienen solo letras alfabéticas (sin números ni símbolos)
        if palabra.isalpha():
            # Expresión regular para eliminar secuencias repetitivas de consonantes o vocales (3 o más)
            if not re.search(r'[aeiou]{3,}', palabra, re.IGNORECASE) and not re.search(r'[^aeiou]{3,}', palabra, re.IGNORECASE):
                palabras_filtradas.append(palabra)
    
    return palabras_filtradas

# Función para extraer palabras de 3 letras y contar su frecuencia
def crear_diccionario_palabras_letras(texto, num_letras):
    palabras = re.findall(rf'\b\w{{{num_letras}}}\b', texto.lower())  # Solo palabras con el número de letras especificado
    contador_palabras = Counter(palabras)
    return dict(contador_palabras)

# Función para guardar el diccionario en un archivo de texto
def guardar_diccionario_en_txt(diccionario, ruta_archivo):
    with open(ruta_archivo, 'w') as f:
        for palabra, frecuencia in diccionario.items():
            f.write(f"{palabra}: {frecuencia}\n")

# Función para subir archivo a S3
def subir_archivo_a_s3(ruta_archivo, bucket, nombre_s3):
    s3 = boto3.client('s3')
    with open(ruta_archivo, 'rb') as data:  # 'rb' lee el archivo en modo binario.
        s3.put_object(Bucket=bucket, Key=f"Datamart/{nombre_s3}.txt", Body=data)

def main():
    bucket_name = 'practica'  # Reemplaza con el nombre de tu bucket
    prefix = 'Datalake/'      # Prefijo de la carpeta
    
    # Obtener los archivos de S3
    files = get_s3_files(bucket_name, prefix)
    
    # Diccionario para almacenar las palabras de 3, 4 y 5 letras y su frecuencia
    diccionario_3 = Counter()
    diccionario_4 = Counter()
    diccionario_5 = Counter()

    for file_key in files:
        # Obtener el contenido del archivo
        s3 = boto3.client('s3')
        file_object = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_object['Body'].read().decode('utf-8')
        
        # Preprocesar el contenido del archivo
        palabras_filtradas = preprocesado(file_content)
        texto_filtrado = ' '.join(palabras_filtradas)

        # Crear diccionarios de palabras de 3, 4 y 5 letras para este archivo
        diccionario_palabras_3 = crear_diccionario_palabras_letras(texto_filtrado, 3)
        diccionario_palabras_4 = crear_diccionario_palabras_letras(texto_filtrado, 4)
        diccionario_palabras_5 = crear_diccionario_palabras_letras(texto_filtrado, 5)
        
        # Sumar los conteos a los diccionarios totales
        diccionario_3.update(diccionario_palabras_3)
        diccionario_4.update(diccionario_palabras_4)
        diccionario_5.update(diccionario_palabras_5)

    # Guardar los diccionarios en archivos .txt
    ruta_archivo_local3 = "Datamart_palabras_3.txt"
    guardar_diccionario_en_txt(diccionario_3, ruta_archivo_local3)

    ruta_archivo_local4 = "Datamart_palabras_4.txt"
    guardar_diccionario_en_txt(diccionario_4, ruta_archivo_local4)

    ruta_archivo_local5 = "Datamart_palabras_5.txt"
    guardar_diccionario_en_txt(diccionario_5, ruta_archivo_local5)

    # Subir los archivos a S3
    subir_archivo_a_s3(ruta_archivo_local3, bucket_name, 'Datamart_palabras_3')
    subir_archivo_a_s3(ruta_archivo_local4, bucket_name, 'Datamart_palabras_4')
    subir_archivo_a_s3(ruta_archivo_local5, bucket_name, 'Datamart_palabras_5')

    # Imprimir los diccionarios por pantalla
    print(f"Diccionario de 3 letras: {diccionario_3}")
    print(f"Diccionario de 4 letras: {diccionario_4}")
    print(f"Diccionario de 5 letras: {diccionario_5}")

if __name__ == "__main__":
    main()
