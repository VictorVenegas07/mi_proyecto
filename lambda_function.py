import json
import base64
import uuid
import datetime
import sys
import os

# Agregar al path las dependencias
sys.path.insert(0, os.path.dirname(__file__))

import boto3
import mysql.connector

s3 = boto3.client('s3')

# Credenciales de la base de datos MySQL
rds_host = 'database-mysql.cf8cis6e8ylh.us-east-1.rds.amazonaws.com'
rds_user = 'admin'
rds_password = 'admin123'
rds_database = 'upload_file'

def lambda_handler(event, context):
    # Obtener el método HTTP de la solicitud
    http_method = event['requestContext']['http']['method']

    if http_method == 'POST':
        return handle_post_request(event)
    elif http_method == 'GET':
        return handle_get_request(event)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Método HTTP no válido'})
        }

def handle_post_request(event):
    # Inicializar el contenido del cuerpo a un valor predeterminado
    body_content = {}

    try:
        # Intentar parsear el cuerpo de la solicitud
        body_content = json.loads(event['body'])

        # Obtener el archivo en base64 y nombre del archivo
        archivo_base64 = body_content['archivo']
        nombre = body_content.get('nombre', 'archivo')
        contentType = body_content.get('contentType', 'application/octet-stream')

        # Decodificar el archivo base64
        archivo_bytes = base64.b64decode(archivo_base64)

        # Subir el archivo a S3
        s3_response = s3.put_object(Body=archivo_bytes, Bucket='my-bucket-lab-27', Key=nombre, ContentType=contentType)
        
        archivo_id = str(uuid.uuid4())
        
        # Obtener la hora local actual
        hora_actual_local = datetime.datetime.now().isoformat()
        
        # Guardar información en MySQL
        conn = mysql.connector.connect(
            host=rds_host,
            user=rds_user,
            password=rds_password,
            database=rds_database
        )
        cursor = conn.cursor()
        cursor.execute("INSERT INTO files (file_id, nombre_archivo, fecha_creacion) VALUES (%s, %s, %s)", (archivo_id, nombre, hora_actual_local))
        conn.commit()
        conn.close()

        # Devolver respuesta exitosa
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Archivo cargado correctamente'
            })
        }

    except Exception as e:
        # Imprimir el error en los logs de CloudWatch
        print('Error al procesar solicitud POST:', str(e))

        # Devolver respuesta con el contenido original del cuerpo
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error al procesar solicitud POST: {str(e)}', 'original_body': body_content})
        }

def handle_get_request(event):
    try:
        # Conectar a MySQL y obtener todos los archivos
        conn = mysql.connector.connect(
            host=rds_host,
            user=rds_user,
            password=rds_password,
            database=rds_database
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM files")
        files = cursor.fetchall()
        conn.close()

        # Formatear los archivos para la respuesta
        formatted_files = []
        for file in files:
            formatted_file = {
                'fileId': file[0],
                'NombreArchivo': file[1],
                'fechaCreacion': file[2].isoformat(),
                'EnlaceDescarga': ''  # Puedes dejar este campo vacío ya que no hay un enlace de descarga en S3 en este caso
            }
            formatted_files.append(formatted_file)

        # Devolver respuesta exitosa con la lista de archivos formateada
        return {
            'statusCode': 200,
            'body': json.dumps(formatted_files)
        }
    except Exception as e:
        # Imprimir el error en los logs de CloudWatch
        print('Error al procesar solicitud GET:', str(e))

        # Devolver respuesta con el error
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error al procesar solicitud GET: {str(e)}'})
        }
