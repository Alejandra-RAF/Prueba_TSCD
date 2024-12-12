import boto3
import os

# Obtener el nombre del archivo
def obtener_nombre_archivo_en_s3(bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Obtener el nombre del archivo
    for obj in response.get('Contents', []):
        return obj['Key']  # Retorna el primer archivo encontrado

    return None  # Retorna None si no se encuentra ningún archivo

# Función para leer el archivo .txt desde S3
def leer_diccionario_desde_s3(bucket_name, input_key):
    """Lee un archivo de texto desde un bucket S3 y devuelve un diccionario de palabras y sus pesos."""
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=input_key)
    content = response['Body'].read().decode('utf-8')
    
    diccionario = {}
    for line in content.strip().split('\n'):
        palabra, peso = line.split(": ")  # Asumimos que las palabras y los pesos están separados por un : y espacio
        diccionario[palabra] = int(peso)  # Convierte el peso a entero

    return diccionario

# Función para verificar si dos palabras difieren en una letra
def difieren_en_una_letra(palabra1, palabra2):
    if len(palabra1) != len(palabra2):
        return False
    diferencia = sum(1 for a, b in zip(palabra1, palabra2) if a != b)
    return diferencia == 1

# Función para crear la lista de palabras y sus pesos
def lista_palabras_pesos(diccionario):
    lista_pesos = []
    palabras = list(diccionario.keys())
    
    for i in range(len(palabras)):
        for j in range(i + 1, len(palabras)):
            palabra1 = palabras[i]
            palabra2 = palabras[j]
            if difieren_en_una_letra(palabra1, palabra2):
                peso1 = diccionario[palabra1]
                peso2 = diccionario[palabra2]
                peso_conexion = abs(peso1 - peso2)
                lista_pesos.append((palabra1, palabra2, peso_conexion))
    return lista_pesos

# Función para guardar la lista de pesos en S3 y en el ordenador
def guardar_en_s3_y_local(bucket_name, key, lista_pesos, ruta_local):
    s3 = boto3.client('s3')
    
    # Guardar en S3
    contenido = "\n".join([f"{palabra1} {palabra2} {peso}" for palabra1, palabra2, peso in lista_pesos])
    s3.put_object(Bucket=bucket_name, Key=key, Body=contenido)

    # Guardar en el ordenador
    with open(ruta_local, 'w') as f:
        f.write(contenido)

def main():
    bucket_name = 'practica'  # Reemplaza con el nombre de tu bucket
    prefix = 'Datamart/'       # Prefijo de la carpeta

    # Obtener el nombre del archivo desde S3
    file_name = obtener_nombre_archivo_en_s3(bucket_name, prefix)

    if file_name is None:
        print("No se encontró ningún archivo en la carpeta Datamart.")
        return

    # Leer el diccionario desde S3
    diccionario = leer_diccionario_desde_s3(bucket_name, file_name)

    # Obtener la lista de palabras y sus pesos
    lista_pesos = lista_palabras_pesos(diccionario)

    # Definir la ruta local para guardar el archivo
    ruta_local = os.path.join(os.getcwd(), 'pesos_aristas.txt')

    # Guardar la lista de pesos en S3 y en el ordenador
    guardar_en_s3_y_local(bucket_name, 'Datamart/pesos_aristas.txt', lista_pesos, ruta_local)
    print(f"La lista de pesos de las aristas se ha guardado en S3 como 'pesos_aristas.txt' y en {ruta_local}.")

if __name__ == "__main__":
    main()
