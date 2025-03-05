// Databricks notebook source
// MAGIC %md
// MAGIC # Databricks Certified: Associate data engineer
// MAGIC
// MAGIC ## Capitulo 1 - Semana 1 
// MAGIC
// MAGIC ### Nota: Este cuaderno es parte de una hoja de ruta para prepararme la certificacion de databricks "Databricks certified associate data engineer"

// COMMAND ----------

// MAGIC %md
// MAGIC Runtime: 12.2 LTS (Scala 2.12, Spark 3.3.2
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Preguntas teóricas
// MAGIC
// MAGIC 🔹 Preguntas teóricas (respuesta breve)
// MAGIC
// MAGIC 1️⃣ **¿Qué es Databricks y en qué se diferencia de Apache Spark?**
// MAGIC
// MAGIC   - Databricks es una plataforma en la nube pasada en Apache Spark, diseñada para facilitar el procesamiento de datos. Apache Spark es un motor de procesamiento distribuido. 
// MAGIC
// MAGIC 2️⃣ **¿Cuáles son los principales componentes del workspace de Databricks?**
// MAGIC
// MAGIC   - Los principales componentes del Databricks Workspace son:
// MAGIC
// MAGIC     - Workspace: Organiza notebooks, librerías y dashboards.
// MAGIC     - Clusters: Ejecutan código en Spark.
// MAGIC     - Jobs: Automatizan tareas y pipelines.
// MAGIC     - Databricks SQL: Ejecuta consultas y visualiza datos.
// MAGIC     - DBFS (Databricks File System): Almacenamiento de archivos.
// MAGIC     - Repos: Integración con Git para control de versiones.
// MAGIC
// MAGIC 3️⃣ **¿Qué es un Cluster en Databricks y para qué se usa?**
// MAGIC
// MAGIC - Un Cluster en Databricks es un grupo de máquinas virtuales que ejecutan Apache Spark. Se usa para procesar grandes volúmenes de datos, ejecutar notebooks, jobs y consultas en Databricks SQL.
// MAGIC
// MAGIC
// MAGIC 4️⃣ **¿Cuál es la diferencia entre un RDD, un DataFrame y un Dataset en Spark?**
// MAGIC
// MAGIC -  RDD (Resilient Distributed Dataset): Estructura básica de Spark, inmutable y distribuida, con bajo nivel de optimización.
// MAGIC -  DataFrame: Estructura tabular optimizada, similar a una tabla SQL, con mayor rendimiento y API más simple.
// MAGIC -  Dataset: Similar a un DataFrame pero con tipado fuerte y validación en tiempo de compilación (solo en Scala/Java).
// MAGIC
// MAGIC 5️⃣ **¿Qué ventajas ofrece Spark SQL frente a SQL tradicional?**
// MAGIC
// MAGIC - Procesamiento distribuido: Maneja grandes volúmenes de datos en clústeres.
// MAGIC - Optimización automática: Usa Catalyst Optimizer para mejorar el rendimiento.
// MAGIC - Compatibilidad: Soporta SQL estándar y se integra con DataFrames y Datasets.
// MAGIC - Escalabilidad: Funciona en memoria y en disco según la carga.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ejercicios prácticos
// MAGIC

// COMMAND ----------

// Importamos librerías necesarias
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// Definimos el esquema del DataFrame
val schema = StructType(Array(
    StructField("PassengerId", IntegerType, true),
    StructField("Survived", IntegerType, true),
    StructField("Pclass", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("Sex", StringType, true),
    StructField("Age", DoubleType, true),
    StructField("SibSp", IntegerType, true),
    StructField("Parch", IntegerType, true),
    StructField("Ticket", StringType, true),
    StructField("Fare", DoubleType, true),
    StructField("Cabin", StringType, true),
    StructField("Embarked", StringType, true)
))

// COMMAND ----------

// Importamos el csv a un dataframe usando el esquema anterior

val df = spark.read
  .option("header", "true")
  .schema(schema)
  .csv("/FileStore/tables/titanic.csv")
  df.show (5)

// COMMAND ----------

df.createOrReplaceTempView("titanic")

// COMMAND ----------

spark.sql("SELECT COUNT(*) AS total_pasajeros FROM titanic").show()

spark.sql("SELECT Pclass, COUNT(*) AS total_pasajeros FROM titanic GROUP BY Pclass ORDER BY Pclass").show()

spark.sql("SELECT Pclass, AVG(Age) as Edad_promedio FROM titanic WHERE Survived = 1 GROUP BY Pclass").show()

// COMMAND ----------

// Conteo de valores nulos en cada columna

df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show()

// COMMAND ----------

// Creamos una nueva columna para ver el tamaño de las familias, sumando padres/madres e hijos

val df2 = df.withColumn("FamilySize", col("SibSp") + col("Parch"))
df2.select("PassengerId", "SibSp", "Parch", "FamilySize").show(10)


// COMMAND ----------

// Con esta columna, podemos diferenciar los pasajeros que viajaban solos (FamilySize = 0) o los que viajaban con algun familiar (FamilySize > 1). Creamos una columna con valor true si viaja solo y false si viaje acompañado


val df3 = df2.withColumn("IsAlone", when(col("FamilySize") === 0, true).otherwise(false))
df3.select("PassengerId", "FamilySize", "IsAlone").show(10)

df3.groupBy("IsAlone").count().show()

// COMMAND ----------

// Contamos cuantos pasajeros sobrevivieron, y los agrupamos en muertos y supervivientes

df.groupBy("Survived").count().show()

// COMMAND ----------

// Aumentamos la restriccion y queremos ver SOLO las mujeres que sobrevivieron

df.filter(col("Survived") === 1 && col("Sex") === "female").show()

// COMMAND ----------

// Pasajeros que pagaron mas de 50 dolares y cuya edad era superior a los 30 años

df.filter(col("Fare") > 50 && col("Age") > 30).show()

// COMMAND ----------

// Mostramos a los 10 pasajeros que pagaron más por subir

df.select("PassengerId", "Fare").orderBy(col("Fare").desc).show(10)


// COMMAND ----------

// Ahora los 10 mas jovenes *Nota: Como en la columna hay nulos, tenemos que especificar que los ponga los ultimos, o solo veremos nulos

df.select("PassengerId", "Age").orderBy(col("Age").asc_nulls_last).show()

// COMMAND ----------

// Igual, pero solo los que sobrevivieron *Nota: Mientras que select() devuelve las columnas que especificamos (como en SQL), filter() devuelve todas las columnas, pero con los registros filtrados

df.filter(col("Survived") === 0).orderBy(col("Age").asc_nulls_last).show()

// COMMAND ----------

// Calculamos el precio promedio del pasaje

df.groupBy("Pclass").agg(avg("Fare").alias("avg_fare")).show()

// COMMAND ----------

//Calculamos la edad mas alta y la mas baja (Un poco raro alguien con 0.42 años no?)

df.agg(max("Age").alias("max_age"), min("Age").alias("min_age")).show()

// COMMAND ----------

// Contamos pasajeros por clase y sexo

df.groupBy("Pclass","Sex").count().orderBy("Pclass","Sex").show()

// COMMAND ----------

// Creamos una categoría de tarifa en una nueva columna, baja media y alta, para clasificar las tarifas

val df4 = df.withColumn("FareCategory", when(col("Fare") < 10, "Low")
  .when(col("Fare") >= 10 && col("Fare") <= 50, "Medium")
  .otherwise("High"))

// Mostrar resultado
df4.select("PassengerId", "Fare", "FareCategory").show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC ## 📝 Mini Test - Semana 1: Fundamentos de Spark y Databricks
// MAGIC

// COMMAND ----------

// Ej 1: Cuenta cuántos pasajeros tienen un nombre que comienza con la letra "M"

df.select(col("Name")).where(col("Name").rlike("^M")).count()

// COMMAND ----------

// Ej 2: Encuentra la edad promedio de los pasajeros que pagaron más de 100 en tarifa (Fare).

df.filter(col("Fare") > 100).agg(avg("Age").alias("Edad_promedio")).show()

// COMMAND ----------

/* Ej 3: Crea una nueva columna "AgeCategory" con valores:

"Young" (Age < 18)
"Adult" (18 ≤ Age < 60)
"Senior" (Age ≥ 60)

*/

df.withColumn("AgeCategory", when(col("Age") < 18, "Young").when(col("Age") > 60, "Senior").otherwise("Adult")).show()


// COMMAND ----------

// Ej 4: Muestra el top 5 de pasajeros con mayor tarifa (Fare) ordenados de mayor a menor.

df.select(col("Fare")).orderBy(desc("Fare")).show(5)

// COMMAND ----------

/* 📌 Bonus Challenge 🚀
💡 Encuentra la combinación de clase (Pclass) y género (Sex) con la mayor tasa de supervivencia (Survived).
*/

df.select("Pclass","Sex").where(col("Survived") === 1).groupBy("Pclass","Sex").count().orderBy(desc("count")).show()

// COMMAND ----------


// Solucion completa


val totalCount = df.groupBy("Pclass", "Sex")
  .count()
  .withColumnRenamed("count", "total")

val survivedCount = df.filter(col("Survived") === 1)
  .groupBy("Pclass", "Sex")
  .count()
  .withColumnRenamed("count", "survived")

val survivalRate = survivedCount
  .join(totalCount, Seq("Pclass", "Sex"))
  .withColumn("SurvivalRate", col("survived") / col("total")) // Calcula la tasa
  .orderBy(desc("SurvivalRate")) // Ordena de mayor a menor
  .select("Pclass", "Sex", "SurvivalRate")

survivalRate.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ## Ejercicios extra 
// MAGIC

// COMMAND ----------

// Calcular la edad promedio de los pasajeros en cada clase (Pclass) usando funciones de ventana

val windowSpec = Window.partitionBy("Pclass")

df.withColumn("Avg_Age_Pclass", avg("Age").over(windowSpec))
  .select("Pclass", "Age", "Avg_Age_Pclass")
  .show()

// COMMAND ----------

// Asignar un ranking a cada pasajero basado en la tarifa más alta dentro de su clase

val rankSpec = Window.partitionBy("Pclass").orderBy(desc("Fare"))

df.withColumn("Fare_Rank", rank().over(rankSpec))
  .select("Pclass", "Fare", "Fare_Rank")
  .show()

// COMMAND ----------

// Almacenar temporalmente el DataFrame en memoria 

val dfCached = df.cache() // También puedes usar persist()
dfCached.groupBy("Pclass").agg(avg("Fare")).show()
dfCached.groupBy("Sex").agg(avg("Age")).show()


// COMMAND ----------

// Calcula el procentaje de sobrevivientes de cada grupo en una sola consulta SQL

df.createOrReplaceTempView("titanic")

val result = spark.sql("""
  SELECT Pclass, Sex, 
         COUNT(CASE WHEN Survived = 1 THEN 1 END) * 100.0 / COUNT(*) AS SurvivalRate
  FROM titanic
  GROUP BY Pclass, Sex
  ORDER BY SurvivalRate DESC
""")

result.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ejercicios extra de "windowing"

// COMMAND ----------

// Dentro de cada clase (Pclass), clasificar a los pasajeros según su tarifa (Fare) y asignar un ranking basado en las tarifas más altas

val windowspec = Window.partitionBy("Pclass").orderBy(desc("Fare"))

df.withColumn("Fare_rank", rank().over(windowspec))
.select("Pclass","Fare","Fare_rank")
.show()


// COMMAND ----------

// Calcular la diferencia de edad entre el pasajero y el promedio de edad dentro de la misma clase (Pclass)

val windowspec = Window.partitionBy("Pclass")

val df_media = df.withColumn("Media_edad", avg("Age").over(windowspec))

df_media.withColumn("Diff", col("Age") - col("Media_edad"))
.select("Pclass", "Age", "Media_edad","Diff")
.show()

// COMMAND ----------

// Calcular el número de supervivientes dentro de cada grupo de clase (Pclass) y género (Sex), sin perder el detalle de cada fila

val windowspec = Window.partitionBy("Pclass", "Sex")

df.withColumn("Supervivientes", count(when(col("Survived") === 1,1)).over(windowspec))
.select("Pclass", "Sex", "Supervivientes")
.show()


// COMMAND ----------

// Para cada pasajero, calcula si su edad es superior o inferior al promedio de edad dentro de su clase (Pclass)


val windowspec = Window.partitionBy("Pclass")

val df_mean = df.withColumn("Media", avg("Age").over(windowspec))

df_mean.withColumn("Diff", when(col("Age") > col("Media"), "Superior").otherwise("Inferior"))
.select("Pclass", "Age", "Diff")
.show()

// COMMAND ----------

// Calcular las tarifas acumuladas de los pasajeros por cada clase (Pclass) y ordenarlos por la columna "Fare" en orden descendente 

val windowspec = Window.partitionBy("Pclass").orderBy(desc("Fare"))

df.withColumn("Acumulado", sum("Fare").over(windowspec))
  .select("Pclass", "Fare", "Acumulado")
  .show(10)

// COMMAND ----------

// Filtrar pasajeros mayores e la media de su clase

val windowspec = Window.partitionBy("Pclass")

val df_mean = df.withColumn("Media", avg("Age").over(windowspec))

df_mean.filter(col("Age") > col("Media"))
.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ## Ejercicios extra semana 1 sobre "Conceptos basicos"

// COMMAND ----------

// Ordenar a los pasajeros por la clase (Pclass) y el género (Sex) y asignarles un ranking utilizando una función de ventana

val windowspec = Window.partitionBy("Pclass", "Sex").orderBy(desc("Fare"))

df.withColumn("Fare_rank", rank().over(windowspec))
  .select("Pclass", "Sex", "Fare", "Fare_rank")
  .show(10)

// COMMAND ----------

// Calcula el total de supervivientes (Survived) por clase (Pclass) y género (Sex) y también la edad promedio por clase

df.groupBy("Pclass","Sex")
.agg(sum("Survived").alias("Supervivientes"), avg("Age").alias("Media_edad"))
.show()


// COMMAND ----------

// Calcular la tasa de mortalidad por Pclass y sex

df.createOrReplaceTempView("titanic")

val result = spark.sql("""
  SELECT Pclass, Sex, 
         ROUND(COUNT(CASE WHEN Survived = 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS DeathRate
  FROM titanic
  GROUP BY Pclass, Sex
  ORDER BY DeathRate DESC
""")

result.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ejercicios extra de Spark SQL

// COMMAND ----------

// Contar el numero de pasajeros que hay en cada clase (Pclass)

df.createOrReplaceTempView("titanic")

val result = spark.sql("""
  SELECT Pclass, COUNT(*) AS PassengerCount
  FROM titanic
  GROUP BY Pclass
""")

result.show()


// COMMAND ----------

// Calcular el promedio de edad de los pasajeros por clase y genero, excluyendo los valores nulos de la columna Age

val result = spark.sql("""
SELECT Pclass, Sex, AVG(CASE WHEN Age IS NOT NULL THEN Age END) AS Media_edad
FROM titanic
GROUP BY Pclass, Sex

""")

result.show()

// COMMAND ----------

// Mostrar las clases Pclass con la mayor cantidad de supervivientes

val result = spark.sql("""  
SELECT Pclass, COUNT(CASE WHEN Survived = 1 THEN 1 END) AS Supervivientes
FROM titanic
GROUP BY Pclass
ORDER BY Supervivientes DESC


""")

result.show()

// COMMAND ----------

// Calcular la tasa de sueprviviencia (en porcentaje) por clase y genero

val result = spark.sql("""
SELECT Pclass, Sex, (COUNT(CASE WHEN Survived = 1 THEN 1 END)*100)/ COUNT (*) as Porcentaje
FROM titanic
GROUP BY Pclass, Sex
ORDER BY Pclass, Sex

""")

result.show()

// COMMAND ----------

// Mostrar los 5 pasajeros con las tarifas mas altas

val result = spark.sql("""
SELECT Name, Fare
FROM titanic
ORDER BY Fare DESC
LIMIT 5

""")

result.show()

// COMMAND ----------

// Mostrar la edad maxima del os pasajeros por clase

val result = spark.sql("""
SELECT Pclass, MAX(Age) AS Maximo
FROM titanic
GROUP BY Pclass
ORDER BY Pclass

""")

result.show()

// COMMAND ----------

// Contar cuántos pasajeros sobrevivieron y cuantos no sobrevivieron en cada clase

val result = spark.sql("""
SELECT Pclass, COUNT(CASE WHEN Survived = 1 THEN 1 END) AS Vivos, COUNT(CASE WHEN Survived = 0 THEN 1 END) AS Muertos
FROM titanic
GROUP BY Pclass
ORDER BY Pclass

""")

result.show()
