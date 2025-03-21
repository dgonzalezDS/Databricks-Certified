// Databricks notebook source
// MAGIC %md
// MAGIC # Databricks Certified: Associate data engineer
// MAGIC
// MAGIC ## Semana 3
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

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/

// COMMAND ----------

// Cargamos los datos desde el archivo titanic.csv en un dataframe

val df = spark.read
.option("header", "true")
.option("inferSchema","true")
.csv("/FileStore/tables/titanic.csv")

df.show(10)


// COMMAND ----------

val datos = Seq(
  (1,"Mapfre","UK"),
  (2,"AXA","ES"),
  (3,"Allianz","FR"),
  (12,"Ocaso","DE"),
  (13,"Ocaso","ES"),
  (14,"Mapfre","UK"),
  (45,"AXA","UK"),
  (76,"AXA", "FR")
)

// COMMAND ----------

val df_2 = datos.toDF("PassengerId","Insurance","Location")

df_2.show(10)

// COMMAND ----------

//Primero haremos la unión usando los metodos nativos de spark

val df_unido2 = df.join(df_2,Seq("PassengerId"),"inner")
df_unido2.show()

// COMMAND ----------

// Para usar SQL hay que crear las vistas temporales para trabajar con tablas

df.createOrReplaceTempView("df")
df_2.createOrReplaceTempView("df_2")


val df_unido = spark.sql("""
SELECT * 
FROM df AS A
INNER JOIN df_2 AS B
ON A.PassengerId = B.PassengerId
""")

df_unido.show(10)

// COMMAND ----------

// Probemos con un left join

val df_izquierdo = df.join(df_2, "PassengerId", "left")
df_izquierdo.show()

// COMMAND ----------

//Ahora con el derecho 

val df_derecho = df.join(df_2, "PassengerId", "right")
df_derecho.show()

// COMMAND ----------

//Ahora con el full

val df_full = df.join(df_2, "PassengerId", "full")
df_full.show()

// COMMAND ----------

// Semi Left

val df_semi_left = df.join(df_2, "PassengerId", "left_semi")
df_semi_left.show()

// COMMAND ----------

// Anti Left

val df_anti_left = df.join(df_2, "PassengerId", "left_anti")
df_anti_left.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Struct y Arrays, estructuras anidadas

// COMMAND ----------

//Datos de muestra


val datos = Seq(
  (1,"David","Malaga","España"),
  (2,"Jose","Malaga","España"),
  (3,"Pedro","Sevilla","España"),
  (4,"John","Houston","Estados Unidos"),
  (5,"Peter","Bristol","Reino Unido"),
)

val df = datos.toDF("ID","Nombre","Ciudad","Pais")

df.show()

// COMMAND ----------

//Columna combinada con struct

val df_struct = df.withColumn("Direccion",struct(col("Ciudad"),col("Pais")))

df_struct.show()

// COMMAND ----------

// Acceder a la info anidada

val df_sec = df_struct.select(col("Direccion.Ciudad")).show()

// COMMAND ----------

// Crear nueva columna con los campos anidados

val df_concar = df_struct.withColumn("Ciudad(Pais)",concat(col("Direccion.Ciudad"), lit(" ("), col("Direccion.Pais"), lit(")")))

df_concar.show()

// COMMAND ----------

// Filtramos por un campo dentro del struct

val df_filter = df_struct.filter(col("Direccion.Ciudad") === "Malaga")

df_filter.show()

// COMMAND ----------

// Trabajaremos ahora con arrays

val personas = Seq(
  (1, "David", Array("Español", "Inglés")),
  (2, "Jose", Array("Frances", "Inglés")),
  (3, "John", Array("Aleman", "Frances")),
  (4, "James", Array("Español", "Aleman")),  
)


// COMMAND ----------

val df_personas = personas.toDF("ID", "Nombre","Idiomas")

df_personas.show()

// COMMAND ----------

// Acceder al primer idioma de cada registro

val df_primer = df_personas.withColumn("PrimerIdioma",col("Idiomas")(0))

df_primer.show()

// COMMAND ----------

// Contar elementos en el array

val df_count = df_personas.withColumn("NumIdiomas",size(col("Idiomas")))

df_count.show()

// COMMAND ----------

// Filtrado usando el array

val df_filtrado = df_personas.filter(size(col("Idiomas")) >= 2)

df_filtrado.show()

// COMMAND ----------

// Uso del explode para expandir columnas
val df_count = df_personas.withColumn("Idioma",explode(col("Idiomas")))

df_count.show()


// COMMAND ----------

// Agrupar por idioma y contar cuantas personas lo hablan

val df_contar = df_count.groupBy("Idioma").agg(count(col("Nombre")).alias("Numero"))

df_contar.show()


// COMMAND ----------

// Usamos la nueva columna para filtrar de nuevo

val df_filtrado = df_contar.filter(col("Numero") >= 2)

df_filtrado.show()

// COMMAND ----------

// MAGIC %md
// MAGIC MapType

// COMMAND ----------

//Creamos unos datos de muestra con un mapa en uno de los campos

val datos = Seq(
  (1,"David",Map("Scala" -> "Avanzado", "Python" -> " Avanzado")),
  (2,"Juan",Map("SQL" -> "Avanzado", "Python" -> " Avanzado")),
  (3,"Pedro",Map("Scala" -> "Avanzado", "SQL" -> " Avanzado")),
  (4,"John",Map("Java" -> "Avanzado", "Python" -> " Avanzado")),
  (5,"James",Map("Scala" -> "Avanzado", "JavaScript" -> " Avanzado")),
)

// COMMAND ----------

// Convertimos a DF

val df = datos.toDF("ID","Nombre","Lenguajes -> Nivel")

// COMMAND ----------

df.show()

// COMMAND ----------

// Nueva columna, acciendiendo a los valores el map

val df_scala = df.withColumn("Nivel_Scala", col("Lenguajes -> Nivel")("Scala"))
df_scala.show()

// COMMAND ----------

// Contamos cuantas claves hay en el map

val df_con_count = df.withColumn("NumLenguajes", size(col("Lenguajes -> Nivel")))
df_con_count.show()


// COMMAND ----------

// Comprobar si una clave especifica esta incluida en el mapa. OJO: lo que devuelve el anidado array_contains, map keys, es un valor booleano por fila, que hay que usar como condicion de filtro en nuestro data frame original

val df_filtrar = df.filter(array_contains(map_keys(col("Lenguajes -> Nivel")), "Python"))

df_filtrar.show()

// COMMAND ----------

// Añadir una nueva pareja clave-valor

val df_nuevo = df.withColumn("Lenguajes -> Nivel", map_concat(col("Lenguajes -> Nivel"), map(lit("Git"), lit("Básico"))))


// COMMAND ----------

display(df_nuevo)

// COMMAND ----------

// Filtramos quien tiene Git ahora en el mapa

val df_git = df_nuevo.filter(array_contains(map_keys(col("Lenguajes -> Nivel")), "Git"))

df_git.show()

// COMMAND ----------

// Crear una nueva columna con el nivel de Git

val df_git_level = df_nuevo.withColumn("Nivel Git",col("Lenguajes -> Nivel")("Git"))

df_git_level.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Columnas tipo fecha

// COMMAND ----------

// Datos de muestra 

val datos = Seq(
  (1,"David","1993-01-06"),
  (2,"Fran","1992-06-04"),
  (3,"Rafa","1987-09-12"),
  (4,"John","1951-01-30"),
  (5,"Stuart","2010-07-14")
)

val df = datos.toDF("ID","Nombre","Cumpleaños")
df.show()

// COMMAND ----------

//Cambiamos el tipo de la columna cumpleaños a tipo fecha y printamos el esquema para visualizarlo

val df_fecha = df.withColumn("Cumpleaños",to_date(col("Cumpleaños")))

df_fecha.printSchema()

// COMMAND ----------

// Extraemos el año, mes y dia en columnas nuevas, desde la columna tipo fecha (Cumpleaños)

val df_año = df_fecha.withColumn("Año", year(col("Cumpleaños")))

df_año.show()

val df_mes = df_fecha.withColumn("Mes", month(col("Cumpleaños")))

df_mes.show()

val df_dia = df_fecha.withColumn("Día", dayofmonth(col("Cumpleaños")))

df_dia.show()

// COMMAND ----------

// Los dataset anteriores estan separados a drede, ahora practicaremos los join
// NOTA: Esta solucion es poco intuitiva, ilegible y el resultado no es el esperado. Esta bien como practica y aproximacion, pero no es una buena manera de resolverlo

val df_completo = df_año.join(df_mes.join(df_dia,"ID","left"),"ID", "left")

df_completo.show()

// COMMAND ----------

/* La solucion mas limpia sería unir las columnas nuevas directamente al dataframe objetico con .withColumn
 O en su defecto, hacer los join especificando columnas, y no hacer joins en cascada ( df3 con df2 y df2 con df1) sino en estrella (df3 con df1 y df2 con df1)
 Nota: Esta segunda aproximacion sigue siendo poco intuitiva y requiere más codigo
*/


// Aproximación 1 (la más limpia)

val df_completo = df_fecha
  .withColumn("Año", year(col("Cumpleaños")))
  .withColumn("Mes", month(col("Cumpleaños")))
  .withColumn("Día", dayofmonth(col("Cumpleaños")))

df_completo.show()

// COMMAND ----------

// Aproximacion 2 (menos limpia)

val df_año = df_fecha.withColumn("Año", year(col("Cumpleaños"))) 
val df_mes = df_fecha.withColumn("Mes", month(col("Cumpleaños"))).select("ID", "Mes")
val df_dia = df_fecha.withColumn("Día", dayofmonth(col("Cumpleaños"))).select("ID", "Día")

val df_join1 = df_año.join(df_mes, "ID")
val df_completo = df_join1.join(df_dia, "ID")

df_completo.show()


// COMMAND ----------

// Aproximacion 3 (alternativa tambien interesante pero menos intuitiva)

val df_completo = df_año
  .join(df_mes.select("ID", "Mes"), "ID", "left")
  .join(df_dia.select("ID", "Día"), "ID", "left")


df_completo.show()

// COMMAND ----------

// Calcular una nueva columna Edad que calcule los años de la persona

val df_completo = df_fecha.withColumn("Edad", floor(months_between(current_date(), col("Cumpleaños")) / 12))
df_completo.show()



// COMMAND ----------

// Añadir 6 meses a una columna tipo fecha con add_months()

val df_seis = df_completo.withColumn("Más 6 meses", add_months(col("Cumpleaños"), 6))

df_seis.show()

// COMMAND ----------

// Truncar fechas a un rango concreto (mes o año)

val df_truncado_mes = df_completo.withColumn("InicioMes", trunc(col("Cumpleaños"),"MM"))
val df_truncado_año = df_completo.withColumn("InicioAño", trunc(col("Cumpleaños"),"YYYY"))

df_truncado_mes.show()
df_truncado_año.show()

// COMMAND ----------

// Truncar fechas a registros concretos

val df_trunc = df_completo.withColumn("Cumpleaños_truncado", when(year(col("Cumpleaños")) < 2000, trunc(col("Cumpleaños"), "YYYY")).otherwise(col("Cumpleaños")))


df_trunc.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Funciones agregadas y ventanas

// COMMAND ----------

// Creamos un nuevo dataframe con el que trabajar

val ventas = Seq(
  ("2024-01-01", "España", "ProductoA", 100),
  ("2024-01-01", "España", "ProductoB", 80),
  ("2024-01-02", "España", "ProductoA", 120),
  ("2024-01-02", "México", "ProductoA", 90),
  ("2024-01-03", "México", "ProductoB", 70),
  ("2024-01-03", "España", "ProductoA", 110),
  ("2024-01-03", "México", "ProductoA", 100)
)

val df_ventas = ventas.toDF("Fecha", "Pais", "Producto", "UnidadesVendidas")
df_ventas.show()


// COMMAND ----------

// Aplicar funciones de agregacion basicas 

val df_ventas_agg = df_ventas.groupBy(col("Producto")).agg(sum(col("UnidadesVendidas")).alias("Total"),avg(col("UnidadesVendidas")).alias("MediaVentas"),max(col("UnidadesVendidas")).alias("VentaMaxima"),min(col("UnidadesVendidas")).alias("VentaMinima"))

df_ventas_agg.show()

// COMMAND ----------

// Ordenar productos por ventas totales (de mayor a menor)

val df_ordenado = df_ventas.groupBy("Producto").agg(sum(col("UnidadesVendidas")).alias("Ventas")).orderBy(desc("Ventas"))

df_ordenado.show()



// COMMAND ----------

// Encontrar el producto con mas unidades vendidas en total

val df_primero= df_ventas.groupBy("Producto").agg(sum(col("UnidadesVendidas")).alias("Ventas")).orderBy(desc("Ventas")).limit(1)

df_primero.show()

// COMMAND ----------

// Calcular porcentajes de cada producto


val df_ventas_prod = df_ventas.groupBy("Producto")
  .agg(sum(col("UnidadesVendidas")).alias("Ventas"))


val total_general = df_ventas_prod.agg(sum("Ventas")).first().getLong(0)


val df_con_porcentaje = df_ventas_prod.withColumn("Porcentaje", round(col("Ventas") * 100 / total_general, 2))

df_con_porcentaje.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Funciones de ventana (windowing)

// COMMAND ----------

// Creamos un nuevo dataframe mas grande con el que trabajar

val datos = Seq(
  ("2024-03-01", "Juan", "ProductoA", 10),
  ("2024-03-02", "Juan", "ProductoA", 15),
  ("2024-03-03", "Juan", "ProductoB", 5),
  ("2024-03-04", "Juan", "ProductoB", 8),
  ("2024-03-05", "Juan", "ProductoC", 12),
  ("2024-03-06", "Juan", "ProductoA", 14),
  ("2024-03-07", "Juan", "ProductoC", 9),
  ("2024-03-01", "Ana", "ProductoA", 20),
  ("2024-03-02", "Ana", "ProductoA", 25),
  ("2024-03-03", "Ana", "ProductoB", 30),
  ("2024-03-04", "Ana", "ProductoB", 15),
  ("2024-03-05", "Ana", "ProductoC", 22),
  ("2024-03-06", "Ana", "ProductoC", 27),
  ("2024-03-07", "Ana", "ProductoA", 18),
  ("2024-03-01", "Luis", "ProductoA", 8),
  ("2024-03-02", "Luis", "ProductoA", 12),
  ("2024-03-03", "Luis", "ProductoB", 18),
  ("2024-03-04", "Luis", "ProductoB", 10),
  ("2024-03-05", "Luis", "ProductoC", 14),
  ("2024-03-06", "Luis", "ProductoC", 11),
  ("2024-03-07", "Luis", "ProductoA", 13),
  ("2024-03-01", "Marta", "ProductoA", 9),
  ("2024-03-02", "Marta", "ProductoB", 14),
  ("2024-03-03", "Marta", "ProductoB", 17),
  ("2024-03-04", "Marta", "ProductoC", 20),
  ("2024-03-05", "Marta", "ProductoA", 11),
  ("2024-03-06", "Marta", "ProductoC", 16),
  ("2024-03-07", "Marta", "ProductoB", 13),
  ("2024-03-01", "Pedro", "ProductoA", 10),
  ("2024-03-02", "Pedro", "ProductoA", 14),
  ("2024-03-03", "Pedro", "ProductoC", 21),
  ("2024-03-04", "Pedro", "ProductoC", 19),
  ("2024-03-05", "Pedro", "ProductoB", 15),
  ("2024-03-06", "Pedro", "ProductoA", 12),
  ("2024-03-07", "Pedro", "ProductoB", 16),
  ("2024-03-01", "Laura", "ProductoB", 18),
  ("2024-03-02", "Laura", "ProductoC", 20),
  ("2024-03-03", "Laura", "ProductoC", 25),
  ("2024-03-04", "Laura", "ProductoA", 19),
  ("2024-03-05", "Laura", "ProductoB", 14),
  ("2024-03-06", "Laura", "ProductoA", 17),
  ("2024-03-07", "Laura", "ProductoC", 22),
  ("2024-03-01", "Sofía", "ProductoA", 11),
  ("2024-03-02", "Sofía", "ProductoB", 17),
  ("2024-03-03", "Sofía", "ProductoC", 13),
  ("2024-03-04", "Sofía", "ProductoA", 12),
  ("2024-03-05", "Sofía", "ProductoC", 16),
  ("2024-03-06", "Sofía", "ProductoB", 19),
  ("2024-03-07", "Sofía", "ProductoA", 14),
  ("2024-03-01", "David", "ProductoB", 15),
  ("2024-03-02", "David", "ProductoB", 10),
  ("2024-03-03", "David", "ProductoA", 13),
  ("2024-03-04", "David", "ProductoC", 18),
  ("2024-03-05", "David", "ProductoA", 9),
  ("2024-03-06", "David", "ProductoB", 20),
  ("2024-03-07", "David", "ProductoC", 17),
  ("2024-03-01", "Carlos", "ProductoC", 21),
  ("2024-03-02", "Carlos", "ProductoA", 19),
  ("2024-03-03", "Carlos", "ProductoB", 11),
  ("2024-03-04", "Carlos", "ProductoA", 14),
  ("2024-03-05", "Carlos", "ProductoC", 16),
  ("2024-03-06", "Carlos", "ProductoB", 15)
)

val df = datos.toDF("Fecha", "Empleado", "Producto", "UnidadesVendidas")
  .withColumn("Fecha", to_date(col("Fecha")))

df.show()

// COMMAND ----------

// Ranking de ventas por empleado

val windowspec = Window.partitionBy("Empleado").orderBy(desc("UnidadesVendidas"))

val df_ranking = df.withColumn("Ranking", rank().over(windowspec))

df_ranking.show()

// COMMAND ----------

// Ranking(denso) de ventas por empleado

val windowspec = Window.partitionBy("Empleado").orderBy(desc("UnidadesVendidas"))

val df_ranking_denso = df.withColumn("Ranking", dense_rank().over(windowspec))

df_ranking_denso.show()


// COMMAND ----------

// Row number para cada empleado

val windowspec = Window.partitionBy("Empleado").orderBy(desc("UnidadesVendidas"))

val df_rownumber = df.withColumn("Fila", row_number().over(windowspec))

df_rownumber.show()

// COMMAND ----------

// Usaremos el lag y lead para ver las ventas de la fila anterior y la fila siguiente, respectivamente

val windowspec = Window.partitionBy("Empleado").orderBy("Fecha")

val df_lag = df.withColumn("VentasPrevias", lag(col("UnidadesVendidas"),1).over(windowspec))

val df_lead = df.withColumn("VentasSiguiente", lead(col("UnidadesVendidas"),1).over(windowspec))

df_lag.show()
df_lead.show()

// COMMAND ----------

// Ahora haremos algo similar, pero con first y last

val windowspec = Window.partitionBy("Empleado").orderBy("Fecha")

val df_first = df.withColumn("PrimeraVenta", first(col("UnidadesVendidas")).over(windowspec))

val df_last = df.withColumn("UltimaVenta", last(col("UnidadesVendidas")).over(windowspec))

df_first.show()
df_last.show()

// COMMAND ----------

// Usar funciones de agregacion en ventanas

val windowspec = Window.partitionBy("Empleado").orderBy("Fecha").rowsBetween(Window.unboundedPreceding, Window.currentRow)

val df_agg = df
  .withColumn("TotalVentas", sum(col("UnidadesVendidas")).over(windowspec))
  .withColumn("MediaVentas", avg(col("UnidadesVendidas")).over(windowspec))

df_agg.show()

// COMMAND ----------

// Agregación condicional WHEN

val df_when = df.groupBy("Empleado").agg(sum(when(col("UnidadesVendidas") > 50, col("UnidadesVendidas"))).alias("VentasAltas"),
sum(when(col("UnidadesVendidas") <= 50, col("UnidadesVendidas"))).alias("VentasBajas")
)

df_when.show()

// COMMAND ----------

// Calcular el porcentaje de ventas altas frente a las ventas bajas

val df_ventas = df.groupBy("Empleado").agg(
sum(col("UnidadesVendidas")).alias("VentasTotal"),
sum(when(col("UnidadesVendidas") > 50, col("UnidadesVendidas"))).alias("VentasAltas"),
sum(when(col("UnidadesVendidas") <= 50, col("UnidadesVendidas"))).alias("VentasBajas")
 )
 .withColumn("Porcentaje", (col("VentasBajas")/ col("VentasTotal"))*100)

 df_ventas.show()

// COMMAND ----------

// Agrupaciones con múltiples condiciones

val df_category = df.withColumn("Category",when(col("UnidadesVendidas") > 70, "Alta").when(col("UnidadesVendidas") < 40, "Baja").otherwise("Media"))

val df_agrupaciones = df_category.groupBy("Empleado").agg(
  count(when(col("Category") === "Alta",1)).alias("VentasAltas"),
  count(when(col("Category") === "Media", 1)).alias("VentasMedias"),
  count(when(col("Category") === "Baja", 1)).alias("VentasBajas")
  )

df_agrupaciones.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Pivot Tables en Spark

// COMMAND ----------

// Crear una tabla dinámica de las ventas por categoría y empleado

val df_pivot = df_category.groupBy("Empleado").pivot("Category").agg(sum("UnidadesVendidas"))

df_pivot.show()

// COMMAND ----------

// Crear una tabla dinámica de las ventas por categoría y empleado mostrando mas de una columna

val df_pivot2 = df_category
.groupBy("Empleado")
.pivot("Category")
.agg(
  sum("UnidadesVendidas").alias("TotalVentas"),
  avg("UnidadesVendidas").alias("MediaVentas")
)
.orderBy(desc("Baja_TotalVentas"))

df_pivot2.show()
