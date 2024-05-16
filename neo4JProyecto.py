import mysql.connector
import json
import os
from neo4j import GraphDatabase

"""
Proyecto realizado por:

Eugenio Ribón, Jorge Kindelán
"""


def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost", user="eugenio", password="password", database="DatosReviews"
    )


# Función para ejecutar consultas y devolver resultados
def run_query_sql(query):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result


def get_neo4j_connection():
    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    return driver


def vaciar_db(driver):
    with driver.session() as session:
        # Comprobar que esta vacío
        consulta = "MATCH (n) DETACH DELETE n"
        session.run(consulta)
        consulta = """MATCH (n) RETURN n"""
        resultado = session.run(consulta)
        print("Comprobamos que este vacio")
        res = resultado.data()
        print(res)


def obtener_similitud(users):
    dic_similitudes = {}
    for u1 in users:
        dic_similitudes[u1] = {}
        query_u1 = f"""
            SELECT DISTINCT(asin)
            FROM Reviews
            WHERE reviewerID = '{u1}'
            """
        result = run_query_sql(query_u1)
        reviews_u1 = set([dat[0] for dat in result])

        for u2 in users:
            if u1 != u2:
                query_u1 = f"""
                            SELECT DISTINCT(asin)
                            FROM Reviews
                            WHERE reviewerID = '{u2}'
                            """
                result = run_query_sql(query_u1)
                reviews_u2 = set([dat[0] for dat in result])

                intersec = reviews_u1.intersection(reviews_u2)

                if len(intersec) > 0:
                    union = reviews_u1.union(reviews_u2)
                    sim = len(intersec) / len(union)
                    dic_similitudes[u1][u2] = sim

                else:
                    dic_similitudes[u1][u2] = 0

    with open("user_similarities.json", "w") as file:
        json.dump(dic_similitudes, file)

    print(dic_similitudes)
    return dic_similitudes


def mostrar_similitudes_neo4j(similitudes):
    driver = get_neo4j_connection()
    vaciar_db(driver)

    with driver.session() as session:

        for id in similitudes.keys():
            consulta = """CREATE (:USUARIO {reviewerID: $id})"""
            session.run(consulta, id=id)

            # Ahora volvemos a ejecutar el MATCH
            consulta = """
            MATCH (n) RETURN n
            """
            resultado = session.run(consulta)
            print("Comprobamos que se han introducido los datos")
            res = resultado.data()
            print(res)

        for u1 in similitudes:
            for u2 in similitudes:
                if u1 != u2:
                    if similitudes[u1][u2] != 0:
                        consulta = """MATCH (u1:USUARIO {reviewerID: $id_1}), (u2:USUARIO {reviewerID: $id_2}) 
                                            CREATE (u1) - [:SIMILITUD{valor:$valor}] -> (u2)"""
                        session.run(
                            consulta, id_1=u1, id_2=u2, valor=similitudes[u1][u2]
                        )


def enlaces_usuarios_articulos(n, cat):
    driver = get_neo4j_connection()

    query_productos = f"""
                    SELECT asin
                    FROM products
                    WHERE category = '{cat}'
                    ORDER BY RAND()
                    LIMIT {n}
                    """
    result = run_query_sql(query_productos)
    productos = [prod[0] for prod in result]

    vaciar_db(driver)

    with driver.session() as session:

        for prod in productos:
            query_asin = f"""
                        SELECT reviewerID, overall, reviewTime
                        FROM Reviews
                        WHERE asin = '{prod}'
                        """
            result = run_query_sql(query_asin)

            consulta = """CREATE (:PRODUCTO{asin: $asin})"""
            session.run(consulta, asin=prod)

            for review in result:
                consulta = f"MATCH (u:Usuario {{reviewerID: '$id'}}) RETURN u"
                res = session.run(consulta, id=review[0])

                usuario_existe = bool(res.single())

                if not usuario_existe:
                    consulta = """CREATE (:USUARIO {reviewerID: $id})"""
                    session.run(consulta, id=review[0])

                consulta = """MATCH (u:USUARIO {reviewerID: $id}), (p:PRODUCTO {asin: $asin}) 
                                            CREATE (u) - [:PUNTUADO{nota:$nota, fecha: $fecha}] -> (p)"""

                fecha_iso = review[2].isoformat()

                session.run(
                    consulta, id=review[0], asin=prod, nota=review[1], fecha=fecha_iso
                )

    print("Carga finalizada")


def enlaces_usuarios_categorias(usuarios):
    driver = get_neo4j_connection()

    # Introducir las categorías
    query_cat = """
                SELECT DISTINCT(category)
                FROM Products
                """
    result = run_query_sql(query_cat)

    vaciar_db(driver)

    with driver.session() as session:

        for cat in result:
            consulta = """CREATE (:CATEGORIA{category: $category})"""
            session.run(consulta, category=cat[0])

        print("categorias añadidas")

        for u in usuarios:
            query = f"""
                    SELECT p.category, COUNT(*)
                    FROM Reviews r
                    INNER JOIN Products p ON p.asin = r.asin
                    WHERE r.reviewerID = '{u}'
                    GROUP BY p.category
                    """

            result = run_query_sql(query)

            if len(result) > 1:
                # Añadir usuarios

                consulta = """CREATE (:USUARIO {reviewerID: $id})"""
                session.run(consulta, id=u)

                print(u, result)
                for cat in result:
                    consulta = """MATCH (u:USUARIO {reviewerID: $id}), (c:CATEGORIA {category: $category}) 
                                            CREATE (u) - [:REVISADO{cantidad:$cantidad}] -> (c)"""
                    session.run(consulta, id=u, category=cat[0], cantidad=cat[1])

    print("Usuarios introducidos")


def articulos_populares():
    driver = get_neo4j_connection()

    query_art = """
                SELECT asin, AVG(overall) as nota_media, COUNT(*)
                FROM Reviews 
                GROUP BY asin
                HAVING count(*) < 40
                ORDER BY nota_media DESC
                LIMIT 5
                """

    result = run_query_sql(query_art)

    vaciar_db(driver)

    with driver.session() as session:
        for art in result:
            consulta = """CREATE (:PRODUCTO{asin: $asin})"""
            session.run(consulta, asin=art[0])

            query_users = f"""
                        SELECT reviewerID
                        FROM Reviews
                        WHERE asin = '{art[0]}'
                        """
            result = run_query_sql(query_users)

            for user in result:
                consulta = f"MATCH (u:Usuario {{reviewerID: '$id'}}) RETURN u"
                res = session.run(consulta, id=user[0])

                usuario_existe = bool(res.single())

                if not usuario_existe:
                    consulta = """CREATE (:USUARIO {reviewerID: $id})"""
                    session.run(consulta, id=user[0])

                consulta = """MATCH (u:USUARIO {reviewerID: $id}), (p:PRODUCTO {asin: $asin}) 
                                            CREATE (u) - [:PUNTUADO] -> (p)"""

                session.run(consulta, id=user[0], asin=art[0])

    print("Datos cargados")


def elegir_opc():
    opc = -1
    while opc not in ["0", "1", "2", "3", "4"]:
        opc = input(
            """
                    Eliga la opción de visualización para neo4j:
                    0: Salir 
                    1: Similitudes entre usuarios
                    2: Enlaces entre usuarios y articulos
                    3: Usuarios con más visualizaciones de un artículo
                    4: Artículos populares 
                    """
        )
    return opc


def main():
    opc = -1
    while opc != "0":
        opc = elegir_opc()
        if opc == "1":
            n = 30
            query = f"""
                    SELECT reviewerID, COUNT(*)
                    FROM Reviews
                    GROUP BY reviewerID 
                    ORDER BY COUNT(*) DESC 
                    LIMIT {n}
                    """
            result = run_query_sql(query)
            users = [us[0] for us in result]

            # 4.1 Similitudes
            path_similitudes = "user_similarities.json"

            if os.path.exists(path_similitudes):
                with open(path_similitudes, "r") as file:
                    similitudes = json.load(file)
            else:
                similitudes = obtener_similitud(users)

            mostrar_similitudes_neo4j(similitudes)

        elif opc == "2":
            # 4.2 Enlaces usuarios artículos
            n = 10
            # cat = str(input("Introduce la categoría"))
            cat = "video_game"
            enlaces_usuarios_articulos(n, cat)

        elif opc == "3":
            # 4.3 Usuarios que han comprado más de un artículo
            query = f"""
                    SELECT reviewerID
                    FROM Reviewers
                    ORDER BY reviewerName 
                    LIMIT 400
                    """
            result = run_query_sql(query)
            users = [us[0] for us in result]
            enlaces_usuarios_categorias(users)

        elif opc == "4":
            # 4.4 Artículos populares
            articulos_populares()


if __name__ == "__main__":
    main()
