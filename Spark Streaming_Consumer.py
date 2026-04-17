from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg, count, round as _round
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# ── 1. Inicialización de Spark ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("LoteriaVentasStreaming") \
    .getOrCreate()

# Reducir logs innecesarios
spark.sparkContext.setLogLevel("WARN")

# ── 2. Esquema idéntico al JSON del Productor ─────────────────────────────────
# IMPORTANTE: Los nombres deben estar en MAYÚSCULAS
schema = StructType([
    StructField("ANIO",      IntegerType(),   True),
    StructField("MES",       IntegerType(),   True),
    StructField("LOTERIA",   StringType(),    True),
    StructField("VENTAS",    DoubleType(),    True),
    StructField("TIMESTAMP", TimestampType(), True),
])

# ── 3. Conexión a Kafka ───────────────────────────────────────────────────────
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loteria_ventas") \
    .option("startingOffsets", "earliest") \
    .load()

# ── 4. Transformación de Datos ────────────────────────────────────────────────
# Convertimos el binario de Kafka a texto, aplicamos el JSON y expandimos
loteria_df = (
    raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# ── 5. Análisis 1: Ventas Totales por Lotería ─────────────────────────────────
ventas_por_loteria = (
    loteria_df
    .filter(col("LOTERIA").isNotNull())
    .groupBy("LOTERIA")
    .agg(
        _round(_sum("VENTAS"), 0).alias("TOTAL_VENTAS"),
        _round(avg("VENTAS"), 0).alias("PROMEDIO_VENTA"),
        count("LOTERIA").alias("NUM_REGISTROS")
    )
)

# ── 6. Análisis 2: Ventas Totales por Año ─────────────────────────────────────
ventas_por_anio = (
    loteria_df
    .filter(col("ANIO").isNotNull())
    .groupBy("ANIO")
    .agg(
        _round(_sum("VENTAS"), 0).alias("VENTAS_POR_ANIO"),
        count("LOTERIA").alias("CONTEO_LOTERIAS")
    )
)

# ── 7. Salida a Consola ───────────────────────────────────────────────────────
# Query para ver el ranking por Lotería
q1 = (
    ventas_por_loteria.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .start()
)

# Query para ver el resumen por Año
q2 = (
    ventas_por_anio.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

# Mantener la aplicación activa
spark.streams.awaitAnyTermination()
