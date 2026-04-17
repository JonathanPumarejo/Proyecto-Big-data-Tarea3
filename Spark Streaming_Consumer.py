# 1. Configuración de Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
import matplotlib.pyplot as plt
import pandas as pd

# Crear sesión de Spark
spark = SparkSession.builder.appName("LibroOficialVentas").getOrCreate()

# 2. Convertir Excel a CSV 
excel_file = "libro_oficial_ventas.xlsx"
csv_file = "libro_oficial_ventas.csv"

# Leer Excel con Pandas y guardarlo como CSV
df_excel = pd.read_excel(excel_file)
df_excel.to_csv(csv_file, index=False)

# 3. Cargar dataset con Spark
df = spark.read.csv(csv_file, header=True, inferSchema=True)

# Mostrar primeras filas
df.show(5)

# 4. Limpieza y transformación
df_clean = df.dropna(subset=["Comprobante", "Fecha elaboracion", "Total"])
df_clean = df_clean.withColumn("Fecha", to_date(col("Fecha elaboracion"), "yyyy-MM-dd"))
df_clean = df_clean.withColumn("Total", col("Total").cast("double"))

# 5. Análisis exploratorio
# Ventas totales
ventas_totales = df_clean.groupBy().sum("Total")
ventas_totales.show()

# Ventas por sucursal
ventas_sucursal = df_clean.groupBy("Sucursal").sum("Total")
ventas_sucursal.show()

# Top 5 clientes con mayor total
top_clientes = df_clean.groupBy("Nombre tercero").sum("Total") \
    .orderBy(col("sum(Total)").desc()) \
    .limit(5)
top_clientes.show()

# 6. Visualización
ventas_sucursal_pd = ventas_sucursal.toPandas()
plt.bar(ventas_sucursal_pd["Sucursal"], ventas_sucursal_pd["sum(Total)"])
plt.xlabel("Sucursal")
plt.ylabel("Ventas Totales")
plt.title("Ventas por Sucursal")
plt.xticks(rotation=45)
plt.show()
