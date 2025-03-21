// Databricks notebook source
// MAGIC %md
// MAGIC # Databricks Certified: Associate data engineer
// MAGIC
// MAGIC ## Semana 2
// MAGIC
// MAGIC ### Nota: Este cuaderno es parte de una hoja de ruta para prepararme la certificacion de databricks "Databricks certified associate data engineer"

// COMMAND ----------

// MAGIC %md
// MAGIC Runtime: 12.2 LTS (Scala 2.12, Spark 3.3.2)

// COMMAND ----------

// Librerias necesarias 

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// Cargamos los datos desde la tabla airports_w2 en un dataframe

val df = spark.read.table("airports_w2")
df.show()




// COMMAND ----------

// Filtramos algunas columnas de interes

df.select("Airport ID", "Name", "City").show()

// COMMAND ----------

// Agrupamos por pais y contamos cuantos aeropuertos hay en cada pais

df.groupBy("country").count().show()

// COMMAND ----------

// Creamos una nueva columna con la longitud de los nombres

df.withColumn("airport_name_length", length(col("Name"))).show(10)

// COMMAND ----------

// Cambiamos la columna Name para que todos los nombres sean en mayusculas

val df_1 = df.withColumn("airport_name_length", length(col("Name")))

df_1.withColumn("Name", upper(col("Name"))).show()

// COMMAND ----------

// Concatenamos nombre y pais con un guion

df.withColumn("name_country_combines", concat_ws("-", col("Name"), col("Country"))).show()

// COMMAND ----------

// Clasificar los aeropuertos segun su elevacion


df.withColumn("Elevation", when(col("Altitude") > 2000, "Elevado").otherwise("Bajo")).show()

// COMMAND ----------

// Contamos aeropuertos por pais usando spark.sql esta vez

val result = spark.sql("""
  SELECT country, COUNT(*) AS airport_count
  FROM airports_w2
  GROUP BY country
  ORDER BY airport_count DESC
""")
result.show()


// COMMAND ----------

// Filtrar los aeropuertos que se encuentran en paises con mas de 50 aeropuertos

val df_1 = df.groupBy("Country").count()

val df_2 = df_1.filter(col("count") > 50)

val result = df.join(df_2,("Country"))

result.show(10)

// COMMAND ----------

// Hacemos lo mismo usando spark SQL

df.createOrReplaceTempView("airports")

val result = spark.sql("""
  SELECT a.*
  FROM airports a
  JOIN (
    SELECT Country
    FROM airports
    GROUP BY Country
    HAVING COUNT(*) > 50
  ) AS filtered_countries
  ON a.Country = filtered_countries.Country
""")

result.show()


// COMMAND ----------

// Calcular porcentaje de aeropuertos en cada pais respecto al total de aeropuertos


val result = spark.sql("""
  SELECT Country, 
         COUNT(*) AS airport_count, 
         ROUND((COUNT(*) / (SELECT COUNT(*) FROM airports) * 100), 2) AS Porcentaje
  FROM airports
  GROUP BY Country
  ORDER BY Porcentaje DESC
""")

result.show()



