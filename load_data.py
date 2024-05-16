import time
import json
from pymongo import MongoClient, InsertOne, UpdateOne, ReplaceOne
import mysql.connector
import configuracion

"""
Proyecto realizado por:

Eugenio Ribón, Jorge Kindelán
"""

# 1º Parte: Mongo


def get_client() -> MongoClient:
    """
        Funcion para obtener una conexion del cliente a Mongodb
    :return:
    """
    # Indicamos la cadena de conexion (en este caso, localhost)
    CONNECTION_STRING = "mongodb://localhost:27017"
    # creamos la conexion empleando mongoClient
    return MongoClient(CONNECTION_STRING)


def get_database(database: str):
    """
    Funcion para obtener la base de datos de MongoDB
    :param database el nombre de la base de datos
    :return: la base de datos
    """
    client = get_client()
    databases = client.list_database_names()
    if database in databases:
        print("La base de datos ya existe")

    # devolvemos la conexion de la bbdd
    return client[database]


def inserta_mongodb(path: str, categoria):
    """
    Funcion para leer los datos e introducirlos
    en la base de datos de mongo
    """
    global id_mongo

    collection = configuracion.conectar_mongo()

    # Lectura del archivo JSON línea por línea
    with open(path, "r") as file:
        for line in file:
            # Cargar la línea como un objeto JSON
            review = json.loads(line)

            data = {
                "reviewID": id_mongo,
                "helpful": review["helpful"],
                "reviewText": review["reviewText"],
                "summary": review["summary"],
                "category": categoria,
            }

            print(data)
            # Insertar el documento en la colección
            collection.insert_one(data)
            id_mongo += 1

    print("datos insertados en mongo")


# 2ª Parte: SQL
def create_table(cursor):
    # Crea la tabla si no existe
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS Reviews (
        reviewID INT,
        reviewerID VARCHAR(255),
        asin VARCHAR(255),
        overall FLOAT,
        unixReviewTime INT,
        reviewTime DATE
    )
    """
    )
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS Reviewers (
        reviewerID VARCHAR(255),
        reviewerName VARCHAR(255),
        UNIQUE (reviewerID, reviewerName)
        )
    """
    )
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS Products (
        asin VARCHAR(255),
        category VARCHAR(255),
        UNIQUE (asin, category)
        )
    """
    )
    print("Tablas creadas")


def iniciar_db(cursor, db):
    # Crear base de datos si no existe
    cursor.execute("CREATE DATABASE IF NOT EXISTS " + str(db))
    print(f"Base de datos {db} creada correctamente")
    cursor.execute("USE " + str(db))
    print("Base creada")


def inserta_datos_mysql(file_path, categoria):
    global id

    # Conexión a la base de datos MySQL
    cursor, conn, db = configuracion.conectar_mysql()
    print("Conectado a la bd", cursor, conn, db)

    # Creo las bases de datos si no existen y las tablas
    iniciar_db(cursor, db)

    # Crea la tabla si no existe
    create_table(cursor)

    # Lectura del archivo JSON línea por línea
    with open(file_path, "r") as file:
        for line in file:
            # Cargar la línea como un objeto JSON
            review = json.loads(line)
            print(review)

            # Extraer los valores de la revisión
            reviewerID = review["reviewerID"]
            asin = review["asin"]
            reviewerName = review.get("reviewerName", None)
            helpful_1, helpful_2 = review["helpful"]
            reviewText = review["reviewText"]
            overall = review["overall"]
            summary = review["summary"]
            unixReviewTime = review["unixReviewTime"]
            reviewTime = review["reviewTime"]

            # Insertar datos en la tabla Reviews
            insert_query = """
            INSERT INTO Reviews (reviewID, reviewerID, asin, overall, unixReviewTime, reviewTime)
            VALUES (%s, %s, %s, %s, %s, STR_TO_DATE(%s, %s))
            """
            data = (
                id,
                reviewerID,
                asin,
                overall,
                unixReviewTime,
                reviewTime,
                "%m %d, %Y",
            )
            cursor.execute(insert_query, data)

            # Insertar datos en la tabla Reviewers
            insert_query = """
            INSERT IGNORE INTO Reviewers (reviewerID, reviewerName)
            VALUES (%s, %s)
            """
            data = (reviewerID, reviewerName)
            cursor.execute(insert_query, data)

            # Insertar datos en la tabla Products
            insert_query = """
            INSERT IGNORE INTO Products (asin, category)
            VALUES (%s, %s)
            """
            data = (asin, categoria)
            cursor.execute(insert_query, data)

            id += 1  # Actualizo el id

    # Confirmar la inserción de todos los datos
    conn.commit()

    # Cerrar la conexión
    cursor.close()
    conn.close()


if __name__ == "__main__":
    rutas_ficheros, categorias = configuracion.obtener_rutas()
    

    # Descomentar para insertar en mysql
    id = 0
    for i in range(len(rutas_ficheros)):
        inserta_datos_mysql(rutas_ficheros[i], categorias[i])

    print("datos introducidos en mysql")

    # Descomentar para insertar en mongodb
    id_mongo = 0
    for i in range(len(rutas_ficheros)):
        inserta_mongodb(rutas_ficheros[i], categorias[i])

    print("Datos introducidos en mongo")
