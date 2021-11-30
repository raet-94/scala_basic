import org.spark.sql.SparkSession
// se busca una tabla de parquet
val ruta = "path/tabla/parquet/"
// se crea el dataframe
val df = spark.read.parquet(ruta)

// Se crea el temp view

df.createOrReplaceTempView("temp")

// llamar al esquema

df.printSchema()
// spark sql acepta queries de sql 
val consulta = spark.sql("Select * from temp")

counting.show(100, false)


