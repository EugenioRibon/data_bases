import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from pymongo import MongoClient
import re
from PIL import Image
import numpy as np

"""
Proyecto realizado por:

Eugenio Ribón, Jorge Kindelán

El comando para ejecutar el menú es “streamlit run <ruta_archivo.py>”
"""


# Función para obtener una conexión a MySQL
def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost", user="eugenio", password="password", database="DatosReviews"
    )


# Funcion para obtener una conexión a mongo
def get_mongodb_client():
    """
    Función para obtener un cliente de MongoDB.
    """
    client = MongoClient("mongodb://localhost:27017")
    return client


# Función para ejecutar consultas y devolver resultados
def run_query_sql(query):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result


# Funcion para obtener una conexión a mongo
def run_query_mongo(cat):
    # MongoDB
    client = get_mongodb_client()
    db = client["DatosReviews"]
    collection = db["Reviews"]
    result = collection.find({"category": cat}, {"_id": 0, "summary": 1})
    summaries = [review["summary"] for review in result]
    return summaries


def main():
    st.title("Consulta de Datos")

    # Consulta para obtener las categorías distintas
    query_categories = """
            SELECT DISTINCT(category)
            FROM Products
            """

    # Obtener las categorías distintas
    categories_result = run_query_sql(query_categories)
    categories_list = [category[0] for category in categories_result]
    categories_list.append("Todos")  # Agregar opción "Todos"

    # Menú de selección de categoría
    cat = st.sidebar.selectbox("Seleccione una categoría:", categories_list)

    # Opción para consultar por ASIN en lugar de categoría
    query_by_asin = False
    asin = None
    if cat == "Todos":
        query_by_asin = st.sidebar.checkbox("Consultar por ASIN en lugar de categoría")
        if query_by_asin:
            asin = st.sidebar.text_input("Introduzca el ASIN:")
        else:
            st.write("Resultados de la consulta:")
            st.write("---")

            # Consulta 1
            query1 = f"""
                SELECT YEAR(reviewTime), count(*)
                FROM Reviews
                GROUP BY YEAR(reviewTime)
                ORDER BY YEAR(reviewTime)
                """
            result1 = run_query_sql(query1)
            st.write(f"Reviews por año para todas las categorias")
            df1 = pd.DataFrame(result1, columns=["Año", "Count"])
            st.write(df1)

            # Gráfico de barras para Consulta 1
            plt.figure(figsize=(10, 6))
            plt.bar(df1["Año"], df1["Count"], color="blue")
            plt.xlabel("Año")
            plt.ylabel("Número de revisiones")
            plt.title(f"Reviews por año para la categoría: {cat}")
            st.pyplot(plt)
            st.write("---")

            # Consulta 2
            query2 = f"""
                    SELECT asin, count(*) AS Count
                    FROM Reviews
                    GROUP BY asin
                    ORDER BY COUNT(*) DESC
                    """
            result2 = run_query_sql(query2)
            st.write("Popularidad de los productos (reviews por producto)")
            df2 = pd.DataFrame(result2, columns=["ASIN", "Count"])
            st.write(df2)

            # Gráfico de línea para Consulta 2
            plt.figure(figsize=(10, 6))
            plt.plot(
                range(len(df2)), df2["Count"], marker="o", color="green", linestyle="-"
            )
            plt.xlabel("Número de artículo")
            plt.ylabel("Número de revisiones")
            plt.title("Popularidad de los productos (reviews por producto)")
            st.pyplot(plt)
            st.write("---")

            # Consulta 3
            query3 = f"""
                    SELECT overall, count(*) AS Count
                    FROM Reviews
                    GROUP BY overall
                    ORDER BY count(*) DESC
                    """
            result3 = run_query_sql(query3)
            st.write("Histograma por Nota")
            df3 = pd.DataFrame(result3, columns=["Nota", "Count"])
            st.write(df3)

            # Gráfico de barras para Consulta 3
            plt.figure(figsize=(10, 6))
            plt.bar(df3["Nota"], df3["Count"], color="orange")
            plt.xlabel("Nota")
            plt.ylabel("Número de revisiones")
            plt.title("Histograma por Nota")
            st.pyplot(plt)
            st.write("---")

            # Consulta 4
            query4 = f"""
                    SELECT reviewTime, COUNT(*)
                    FROM Reviews
                    GROUP BY reviewTime
                    ORDER BY reviewTime
                    """
            result4 = run_query_sql(query4)
            st.write("Popularidad de los productos por año")
            df4 = pd.DataFrame(result4, columns=["reviewTime", "Count"])
            st.write(df4)

            df4["Cumulative Count"] = df4["Count"].cumsum()

            # Gráfico de línea para Consulta 4
            plt.figure(figsize=(10, 6))
            plt.plot(
                range(len(df4)), df4["Cumulative Count"], color="green", linestyle="-"
            )
            plt.xlabel("Número de artículo")
            plt.ylabel("Número de revisiones")
            plt.title("Popularidad de los productos por año")
            st.pyplot(plt)
            st.write("---")

            # Consulta 5
            query5 = f"""
                    SELECT reviewerID, count(*)
                    FROM Reviews
                    GROUP BY reviewerID
                    """
            result5 = run_query_sql(query5)
            st.write(f"Histograma de reviews por usuario")
            df5 = pd.DataFrame(result5, columns=["Usuario", "Count"])
            st.write(df5)

            # Gráfico de barras para Consulta 5
            plt.figure(figsize=(10, 6))
            plt.hist(df5["Count"], bins=200, color="green")
            plt.xlabel("usuarios")
            plt.ylabel("Número de reviews")
            plt.title("Histograma de reviews por usuario")
            st.pyplot(plt)

    # Mostrar resultados de las consultas
    if cat != "Todos" or query_by_asin:
        wordcloud = st.sidebar.checkbox("¿Deseas mostrar una nube de palabras?")
        st.write("Resultados de la consulta:")
        st.write("---")

        # Consulta 1
        if not query_by_asin:
            query1 = f"""
                    SELECT YEAR(reviewTime) AS Año, count(*) AS Count
                    FROM Reviews
                    WHERE asin IN (SELECT asin 
                                    FROM Products
                                    WHERE category = '{cat}'
                                    )
                    GROUP BY YEAR(reviewTime)
                    ORDER BY YEAR(reviewTime)
                    """
            result1 = run_query_sql(query1)
            st.write(f"Reviews por año para la categoría: {cat}")
            df1 = pd.DataFrame(result1, columns=["Año", "Count"])
            st.write(df1)

            # Gráfico de barras para Consulta 1
            plt.figure(figsize=(10, 6))
            plt.bar(df1["Año"], df1["Count"], color="blue")
            plt.xlabel("Año")
            plt.ylabel("Número de revisiones")
            plt.title(f"Reviews por año para la categoría: {cat}")
            st.pyplot(plt)
            st.write("---")

        # Consulta 2
        if not query_by_asin:
            query2 = f"""
                    SELECT asin, count(*) AS Count
                    FROM Reviews
                    WHERE asin IN (SELECT asin 
                                    FROM Products
                                    WHERE category = '{cat}'
                                    )
                    GROUP BY asin
                    ORDER BY COUNT(*) DESC
                    """
        else:
            query2 = f"""
                    SELECT asin, count(*) AS Count
                    FROM Reviews
                    WHERE asin = '{asin}'
                    GROUP BY asin
                    ORDER BY COUNT(*) DESC
                    """
        result2 = run_query_sql(query2)
        st.write(f"Popularidad de los productos para la categoria {cat} ")
        df2 = pd.DataFrame(result2, columns=["ASIN", "Count"])
        st.write(df2)

        # Gráfico de línea para Consulta 2
        plt.figure(figsize=(10, 6))
        plt.plot(
            range(len(df2)), df2["Count"], marker="o", color="green", linestyle="-"
        )
        plt.xlabel("Número de artículo")
        plt.ylabel("Número de revisiones")
        plt.title("Popularidad de los productos (reviews por producto)")
        st.pyplot(plt)

        st.write("---")
        # Consulta 3
        if not query_by_asin:
            query3 = f"""
                    SELECT overall, count(*) AS Count
                    FROM Reviews
                    WHERE asin IN (SELECT asin 
                                    FROM Products
                                    WHERE category = '{cat}'
                                    )
                    GROUP BY overall
                    ORDER BY count(*) DESC
                    """
        else:
            query3 = f"""
                    SELECT overall, count(*) AS Count
                    FROM Reviews
                    WHERE asin = '{asin}'
                    GROUP BY overall
                    ORDER BY count(*) DESC
                    """
        result3 = run_query_sql(query3)
        st.write(f"Histograma por Nota para la categoria {cat}")
        df3 = pd.DataFrame(result3, columns=["Nota", "Count"])
        st.write(df3)

        # Gráfico de barras para Consulta 3
        plt.figure(figsize=(10, 6))
        plt.bar(df3["Nota"], df3["Count"], color="orange")
        plt.xlabel("Nota")
        plt.ylabel("Número de revisiones")
        plt.title("Histograma por Nota")
        st.pyplot(plt)

        # Consulta 4
        if not query_by_asin:
            query4 = f"""
                    SELECT reviewTime, COUNT(*)
                    FROM Reviews
                    WHERE asin IN (SELECT asin 
                                    FROM Products
                                    WHERE category = '{cat}'
                                    )
                    GROUP BY reviewTime
                    ORDER BY reviewTime
                    """

        else:
            query4 = f"""
                    SELECT reviewTime, COUNT(*)
                    FROM Reviews
                    WHERE asin = '{asin}'
                    GROUP BY reviewTime
                    ORDER BY reviewTime
                    """

        result4 = run_query_sql(query4)
        st.write("Popularidad de los productos por año")
        df4 = pd.DataFrame(result4, columns=["reviewTime", "Count"])
        st.write(df4)

        # Calcular la suma acumulativa
        df4["Cumulative Count"] = df4["Count"].cumsum()

        # Gráfico de línea para Consulta 4
        plt.figure(figsize=(10, 6))
        plt.plot(range(len(df4)), df4["Cumulative Count"], color="green", linestyle="-")
        plt.xlabel("Número de artículo")
        plt.ylabel("Número de revisiones")
        plt.title("Popularidad de los productos por año")
        st.pyplot(plt)
        st.write("---")

        # Consulta 5
        query5 = f"""
                SELECT reviewerID, count(*)
                FROM Reviews
                GROUP BY reviewerID
                """
        result5 = run_query_sql(query5)
        st.write(f"Histograma de reviews por usuario")
        df5 = pd.DataFrame(result5, columns=["Usuario", "Count"])
        st.write(df5)

        # Gráfico de barras para Consulta 5
        plt.figure(figsize=(10, 6))
        plt.hist(df5["Count"], bins=200, color="green")
        plt.xlabel("usuarios")
        plt.ylabel("Número de reviews")
        plt.title("Histograma de reviews por usuario")
        st.pyplot(plt)

        # Consulta 6 (wordlcloud)
        if wordcloud:
            summaries = run_query_mongo(cat)

            # Concatenar todos los resúmenes en un solo texto
            text = " ".join(summaries)
            filtered_text = " ".join(re.findall(r"\b\w{4,}\b", text))

            # Generar y mostrar la nube de palabras
            masks = {
                "musical_instruments": "mascaras/guitarra.jpeg",
                "video_game": "mascaras/consola.jpeg",
                "toys_and_games": "mascaras/chess.jpeg",
                "digital_music": "mascaras/mp3.jpg",
            }

            mask_path = masks.get(cat)  # Obtener la ruta de la máscara si existe

            if mask_path:
                mask = np.array(Image.open(mask_path))
                st.write(f"Nube de palabras para la categoría: {cat}")
                wordcloud = WordCloud(
                    width=800,
                    height=400,
                    background_color="white",
                    mask=mask,
                    contour_color="blue",
                    contour_width=2,
                ).generate(filtered_text)
            else:
                st.write(f"Nube de palabras para la categoría: {cat}")
                wordcloud = WordCloud(
                    width=800,
                    height=400,
                    background_color="white",
                ).generate(filtered_text)

            plt.figure(figsize=(10, 6))
            plt.imshow(wordcloud, interpolation="bilinear")
            plt.axis("off")
            st.pyplot(plt)


if __name__ == "__main__":
    main()
