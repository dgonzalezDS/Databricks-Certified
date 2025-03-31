// Databricks notebook source
// MAGIC %md
// MAGIC # Databricks Certified: Associate data engineer
// MAGIC
// MAGIC ## Semana 4
// MAGIC
// MAGIC ### Nota: Este cuaderno es parte de una hoja de ruta para prepararme la certificacion de databricks "Databricks certified associate data engineer"

// COMMAND ----------

// MAGIC %md
// MAGIC Runtime: 12.2 LTS (Scala 2.12, Spark 3.3.2)

// COMMAND ----------

// MAGIC %md
// MAGIC Semana 3 – Transformaciones avanzadas y manejo de datos complejos

// COMMAND ----------


// Librerias necesarias 

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions._
import scala.util._
import io.delta.tables._

// COMMAND ----------

//Creamos un nuevo data set para trabajar con el

val empleados = Seq("Ana", "Luis", "Carlos", "Sofía", "Elena", "Pedro")
val productos = Seq("Laptop", "Teclado", "Ratón", "Monitor", "Impresora", "Tablet")

val random = new Random()

val datos = (1 to 60).map { i =>
  val empleado = empleados(random.nextInt(empleados.length))
  val producto = productos(random.nextInt(productos.length))
  val unidades = random.nextInt(80) + 10
  val mes = f"${random.nextInt(12) + 1}%02d"
  val dia = f"${random.nextInt(28) + 1}%02d"
  val fecha = java.sql.Date.valueOf(s"2024-$mes-$dia")
  (empleado, producto, unidades, fecha)
}

val df = datos.toDF("Empleado", "Producto", "UnidadesVendidas", "Fecha")
df.show(10, truncate = false)

// COMMAND ----------

// Guardamos archivos parquet particionado por Producto

df.write.format("parquet")
.partitionBy("Producto")
.mode("overwrite")
.save("dbfs:/mnt/datos_parquet")

// COMMAND ----------

// Cargamos los datos que guardamos previamente

val df_read = spark.read
.format("parquet")
.option("basePath", "dbfs:/mnt/datos_parquet")
.load("dbfs:/mnt/datos_parquet")

df_read.show()
df_read.printSchema()

// COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/datos_parquet").foreach(println)


// COMMAND ----------

// Ahora guardaremos en formato ORC en lugar de parquet

df.write.format("orc")
.partitionBy("Producto")
.mode("overwrite")
.save("dbfs:/mnt/datos_orc")


// COMMAND ----------

// Podemos visualizar las particiones de esta manera

dbutils.fs.ls("dbfs:/mnt/datos_orc").foreach(println)


// COMMAND ----------

// Cargamos los datos en formato ORC

val df_orc = spark.read
.format("orc")
.option("basePath", "dbfs:/mnt/datos_orc")
.load("dbfs:/mnt/datos_orc")

df_orc.show()
df_orc.printSchema()

// COMMAND ----------

// Cargamos y filtramos particiones específicas

val df_prun = spark.read
.format("orc")
.option("basePath", "dbfs:/mnt/datos_orc")
.load("dbfs:/mnt/datos_orc")
.filter(col("Producto") === "Laptop")

val df_filt = df_prun.groupBy("Empleado")
.agg(sum(col("UnidadesVendidas")).alias("VentasTotales"))

df_prun.show()
df_filt.show()

// COMMAND ----------

// Haremos lo mismo con formato Delta, lo guardamos y luego lo cargaremos 

df.write
.format("delta")
.mode("overwrite")
.save("dbfs:/mnt/datos_delta")

// COMMAND ----------

// Cargamos los datos

val df_delta = spark.read
.format("delta")
.load("dbfs:/mnt/datos_delta")

df_delta.show()
df_delta.printSchema()

// COMMAND ----------

// Cargar la tabla Delta
val deltaTable = DeltaTable.forPath(spark, "dbfs:/mnt/datos_delta")

// Actualizar las filas
deltaTable.update(
  condition = col("Empleado") === "Carlos", 
  set = Map("UnidadesVendidas" -> lit(100))
)


// COMMAND ----------

// Vemos los datos actualizados

val df_updated = spark.read.format("delta").load("dbfs:/mnt/datos_delta")
df_updated.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Uso de particiones 
// MAGIC
// MAGIC Las particiones son una parte fundamental de cómo Spark maneja la paralelización. Cuantas más particiones tenga un DataFrame o RDD, más trabajo se puede hacer en paralelo, pero si se tiene demasiadas particiones pequeñas, puede haber un overhead de gestión de tareas. De manera similar, si hay muy pocas particiones, algunas tareas podrían quedar sobrecargadas.
// MAGIC
// MAGIC Guía para optimización con particiones
// MAGIC  
// MAGIC Repartir las particiones: El primer paso es entender cómo repartir las particiones para lograr un procesamiento más eficiente. Podemos usar el método repartition() o coalesce() para ajustar el número de particiones.
// MAGIC
// MAGIC repartition(n): Reparticiona el DataFrame a n particiones. Este método puede ser costoso porque implica un shuffling de los datos.
// MAGIC
// MAGIC coalesce(n): Reduce el número de particiones, pero solo se usa para reducir el número de particiones sin causar un shuffling completo. Ideal para cuando se reduce el tamaño del DataFrame después de un filtrado.
// MAGIC
// MAGIC Repartir y realizar un shuffling eficiente: Usar repartition es útil cuando queremos aumentar el número de particiones (por ejemplo, al trabajar con grandes cantidades de datos), pero es más costoso que coalesce, que no involucra shuffling completo.

// COMMAND ----------

// Creamos 8 particiones en el dataframe original

val df_rep = df.repartition(8)

df_rep.show()

// Nota: Es interesante ver que a efectos de visualizacion, la reparticion no hace nada.

// COMMAND ----------

// Ahora reduciremos las particiones

val df_coalesce = df_rep.coalesce(3)

df_coalesce.show()

// Coalesce tampoco implicada un cambio en lo visual

// COMMAND ----------

// Ahora trabajaremos con broadcast para optimizar las uniones entre un dataset mas pequeño y otro mas grande
// Creamos un df mas pequeño que sera el que use con el que ya tengo

val smallData = Seq(
  ("Ana", "Monitor", 100),
  ("Carlos", "Laptop", 150),
  ("Luis", "Teclado", 50)
)

val smallDF = spark.createDataFrame(smallData).toDF("Empleado", "Producto", "Precio")

smallDF.show()


// COMMAND ----------

val df_joined = df.join(broadcast(smallDF), Seq("Producto","Empleado"))
df_joined.show()

// COMMAND ----------

// Practicaremos ahora con Cache () y Persist ()

// Medir el tiempo sin caché
val startTimeWithoutCache = System.nanoTime()
df.show()  // Realizar una acción para calcular el tiempo
val endTimeWithoutCache = System.nanoTime()

val durationWithoutCache = (endTimeWithoutCache - startTimeWithoutCache) / 1e9d
println(s"Tiempo sin caché: $durationWithoutCache segundos")

// Aplicar caché
df.cache()

// Medir el tiempo con caché
val startTimeWithCache = System.nanoTime()
df.show()  // Realizar una acción para calcular el tiempo
val endTimeWithCache = System.nanoTime()

val durationWithCache = (endTimeWithCache - startTimeWithCache) / 1e9d
println(s"Tiempo con caché: $durationWithCache segundos")



// COMMAND ----------

// Podemos observar en la ejecucion anterior como el uso de cache reduce a un tercio el tiempo de ejecucion en nuestro caso

// COMMAND ----------

// Trabajaremos ahora con el Z ordering. Para ello necesitamos nuestro datos en formato Delta. Usaremos los guardados con anterioridad

spark.sql("OPTIMIZE delta.`/mnt/datos_delta` ZORDER BY (Producto)")


// COMMAND ----------

// Probamos la optimizacion recargando estos datos, los cuales deberiamos obtener de manera mas rapida

val result = spark.read.format("delta").load("/mnt/datos_delta")
  .filter(col("Producto") === "Laptop")

result.show()


// COMMAND ----------

// Trabajaremos el time travel del formato delta, para lo cual haremos algunas transformaciones y luego revisaremos el historial de versiones

// Realizar un update
spark.sql("UPDATE delta.`/mnt/datos_delta` SET UnidadesVendidas = 100 WHERE Empleado = 'Carlos'")

// Insertar algunos nuevos registros
val df_new = Seq(
  ("Luis", "Tablet", 80, "2024-03-05")
).toDF("Empleado", "Producto", "UnidadesVendidas", "Fecha")
  .withColumn("Fecha", to_date(col("Fecha"), "yyyy-MM-dd"))

df_new.write.format("delta").mode("append").save("/mnt/datos_delta")


// COMMAND ----------

// Consultamos la version anterior

val df_version_0 = spark.read.format("delta").option("versionAsOf", 0).load("/mnt/datos_delta")
df_version_0.show()
