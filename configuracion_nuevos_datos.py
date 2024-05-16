from pymongo import MongoClient
import mysql.connector

"""
Proyecto realizado por:

Eugenio Ribón, Jorge Kindelán
"""


def conectar_mongo():
    """Cambiar las credenciales de acceso para mongo"""

    CONNECTION_STRING = "mongodb://localhost:27017"
    database = "DatosReviews"
    colec = "Reviews" ""

    client = MongoClient(CONNECTION_STRING)
    databases = client.list_database_names()
    if database in databases:
        print("La base de datos ya existe")

    db = client[database]
    collection = db[colec]

    return collection


def conectar_mysql():
    """Cambiar las credenciales de acceso para sql"""
    host = "localhost"
    usuario = "eugenio"
    contrasena = "password"
    database = "DatosReviews"

    conn = mysql.connector.connect(host=host, user=usuario, password=contrasena)
    cursor = conn.cursor()

    return cursor, conn, database


def obtener_rutas():
    """Aqui se pueden modificar las rutas de los ficheros"""

    rutas_ficheros = ["./datasets/Office_Products_5.json"]
    categorias = ["office_products"]

    return rutas_ficheros, categorias
