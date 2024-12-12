from flask import Flask, request
from Functions_Search_API import obtener_nombre_archivo_en_s3, leer_diccionario_desde_s3, dijkstra, camino_mas_largo, detectar_nodos_aislados
from Functions_Search_API import Conectividad  # Importar la clase Conectividad

# Inicializar la aplicación Flask
app = Flask(__name__)

# Configuración de AWS S3
bucket_name = 'practica'
prefix = 'Datamart/'

# Rutas de la API
#http://127.0.0.1:5000/Dijkstra?start=the&target=foe
@app.route('/Dijkstra', methods=['GET'])
def api_dijkstra():
    file_key = obtener_nombre_archivo_en_s3(bucket_name, prefix)
    if file_key:
        diccionario, graph = leer_diccionario_desde_s3(bucket_name, file_key)

        start_word = request.args.get('start')
        target_word = request.args.get('target')

        path, distance = dijkstra(graph, start_word, target_word)  # Cambiado diccionario a graph

        if distance < float('infinity'):
            result = f"El camino más corto de '{start_word}' a '{target_word}' es: {' -> '.join(path)} con una distancia de {distance}"
        else:
            result = f"No hay un camino entre '{start_word}' y '{target_word}'"

        return result, 200  # Retornar como texto
    else:
        return "No se encontró el archivo en S3", 404  # Mensaje de error como texto



# Ruta de Flask para obtener la distancia máxima
#http://127.0.0.1:5000/camino_mas_largo?start=the&target=foe
#http://127.0.0.1:5000/camino_mas_largo
@app.route('/camino_mas_largo', methods=['GET'])
def camino_mas_largo_api():
    bucket_name = request.args.get('bucket', default='practica')  # Valor por defecto
    prefix = request.args.get('prefix', default='Datamart/')      # Valor por defecto
    start = request.args.get('start')
    end = request.args.get('end')

    input_key = obtener_nombre_archivo_en_s3(bucket_name, prefix)
    if input_key is None:
        return "No se encontró el archivo en S3", 404  # Mensaje de error como texto

    _, graph = leer_diccionario_desde_s3(bucket_name, input_key)

    if start and end:
        path, distance = camino_mas_largo(graph, start, end)
        if distance < float('infinity'):
            result = f"El camino más largo es de '{start}' a '{end}': {' -> '.join(path)} con una distancia de {distance}"
            return result, 200  # Retornar como texto
        else:
            return f"No hay camino entre '{start}' y '{end}'.", 404  # Mensaje de error como texto
    
    # Si no se especifican start y end, buscamos el camino más largo en general
    max_path, max_distance, start_word, target_word = camino_mas_largo(graph)
    if max_path:
        result = f"El camino más largo es de '{start_word}' a '{target_word}': {' -> '.join(max_path)} con una distancia de {max_distance}"
        return result, 200  # Retornar como texto
    else:
        return "No hay caminos disponibles en el grafo.", 404  # Mensaje de error como texto



# Ruta de Flask para detectar nodos aislados
#http://127.0.0.1:5000/nodos_aislados
@app.route('/nodos_aislados', methods=['GET'])
def nodos_aislados_api():
    input_key = obtener_nombre_archivo_en_s3(bucket_name, prefix)
    if input_key is None:
        return "No se encontró el archivo en S3", 404  # Mensaje de error como texto

    _, graph = leer_diccionario_desde_s3(bucket_name, input_key)

    nodos_aislados = detectar_nodos_aislados(graph)

    if nodos_aislados:
        return f"Nodos aislados: {', '.join(nodos_aislados)}", 200  # Retornar nodos aislados como texto
    else:
        return "No hay nodos aislados en el grafo.", 200  # Mensaje cuando no hay nodos aislados


# Ruta de Flask para nodos de alto nivel 
#http://localhost:5000/nodos_alto_grado
#http://localhost:5000/nodos_alto_grado?umbral=2
@app.route('/nodos_alto_grado', methods=['GET'])
def nodos_alto_grado_api():
    bucket_name = request.args.get('bucket', default='practica')  # Valor por defecto
    prefix = request.args.get('prefix', default='Datamart/')      # Valor por defecto

    input_key = obtener_nombre_archivo_en_s3(bucket_name, prefix)
    if input_key is None:
        return "No se encontró el archivo en S3", 404  # Mensaje de error como texto

    _, graph = leer_diccionario_desde_s3(bucket_name, input_key)
    conectividad = Conectividad(graph)  # Crear una instancia de la clase Conectividad

    umbral = request.args.get('umbral', default=1, type=int)
    nodos_alto_grado = conectividad.nodos_alto_grado(umbral)  # Llamar al método nodos_alto_grado

    if nodos_alto_grado:
        # Ordenar los nodos por grado de conectividad de mayor a menor
        sorted_nodos = sorted(nodos_alto_grado.items(), key=lambda x: x[1], reverse=True)

        # Crear un string con los resultados
        result_lines = []
        for nodo, grado in sorted_nodos:
            result_lines.append(f"El nodo '{nodo}' tiene un grado de conectividad de {grado}.")
        
        result_text = "Los nodos que tienen mayor conectividad son:\n" + "\n".join(result_lines)
        return result_text, 200  # Retornar como texto
    else:
        return "No hay nodos con un grado de conectividad superior al umbral especificado.", 200  # Mensaje cuando no hay nodos de alto grado

# Ruta de Flask para nodos de grado especifico 
#http://localhost:5000/nodos_grado_especifico?grado=3
@app.route('/nodos_grado_especifico', methods=['GET'])
def nodos_grado_especifico_api():
    bucket_name = request.args.get('bucket', default='practica')  # Valor por defecto
    prefix = request.args.get('prefix', default='Datamart/')      # Valor por defecto

    input_key = obtener_nombre_archivo_en_s3(bucket_name, prefix)
    if input_key is None:
        return "No se encontró el archivo en S3", 404  # Mensaje de error como texto

    _, graph = leer_diccionario_desde_s3(bucket_name, input_key)
    conectividad = Conectividad(graph)  # Crear una instancia de la clase Conectividad

    grado_deseado = request.args.get('grado', default=1, type=int)
    nodos_grado = conectividad.nodos_con_grado_especifico(grado_deseado)  # Llamar al método nodos_con_grado_especifico

    if nodos_grado:
        # Crear un string con los resultados
        result_lines = [nodo for nodo in nodos_grado.keys()]
        result_text = f"Los nodos con grado de conectividad de {grado_deseado} son: " + ", ".join(result_lines)
        return result_text, 200  # Retornar como texto
    else:
        return f"No hay nodos con el grado de conectividad especificado de {grado_deseado}.", 200  # Mensaje cuando no hay nodos con grado específico

if __name__ == '__main__':
    app.run(debug=True)
