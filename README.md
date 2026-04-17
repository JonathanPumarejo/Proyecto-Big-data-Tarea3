1. Descripción Detallada de la Solución
Esta solución implementa una arquitectura Lambda simplificada, diseñada para el procesamiento híbrido de datos de ventas de loterías en Colombia. Combina la capacidad de analizar datos históricos masivos con la monitorización de eventos en tiempo real utilizando el ecosistema de Big Data (Apache Spark y Apache Kafka).

A. Componente de Ingesta (Productor Kafka)
El productor es un script de Python diseñado para transformar un dataset estático en un flujo dinámico.

Normalización de Datos: El archivo ventas_loteria.csv presenta desafíos de formato (años con puntos como 2.012 y ventas con comas como 3,486,162,000). El productor limpia estos campos en tiempo real mediante expresiones regulares antes de enviarlos.

Publicación Distribuida: Los datos se serializan en formato JSON y se publican en el bróker de Kafka bajo el tópico loteria_ventas. Esto permite que múltiples sistemas consuman la misma información sin interferir entre sí.

B. Componente de Streaming (Spark Structured Streaming)
Este módulo se encarga del procesamiento de los datos "en movimiento".

Consumo de Micro-batches: Spark lee los mensajes del tópico de Kafka, aplicando un esquema de datos rígido (StructType) para asegurar que no existan valores nulos por errores de escritura.

Agregaciones en Vivo: Realiza cálculos de suma total, promedio y conteo de registros por cada lotería y por año.

Modo Complete: El sistema utiliza el modo de salida completo, lo que significa que la tabla en consola se actualiza y se vuelve a mostrar íntegramente cada vez que llega un nuevo dato, permitiendo ver el ranking de ventas en tiempo real.

C. Componente de Procesamiento Batch (Análisis Histórico)
Es una aplicación de Spark independiente que procesa el dataset completo de una sola vez para generar inteligencia de negocios.

EDA (Análisis Exploratorio de Datos): Ejecuta funciones estadísticas para obtener la descripción general del dataset (mínimos, máximos y promedios históricos).

Visualización de Datos: Utiliza la librería matplotlib para traducir los resultados de las consultas de Spark en un gráfico de barras. Esto permite identificar visualmente las 10 loterías con mayor éxito comercial.

2. Instrucciones para la Ejecución
Para que el proyecto funcione correctamente, debes seguir este orden exacto en terminales o pestañas diferentes de VS Code:

Fase I: Infraestructura de Kafka
Iniciar Zookeeper: Es el coordinador de Kafka.
bin/zookeeper-server-start.sh config/zookeeper.properties

Iniciar Kafka Server: El servidor que gestiona los mensajes.
bin/kafka-server-start.sh config/server.properties

Crear el Tópico: (Solo es necesario la primera vez)
kafka-topics.sh --create --topic loteria_ventas --bootstrap-server localhost:9092

Fase II: Análisis de Datos Históricos (Batch)
Este paso cumple con la parte de análisis exploratorio y visualización de la guía.

Abre una terminal y ejecuta:
python loteria_batch.py

Resultado: Se mostrarán estadísticas en la consola y se generará el archivo de imagen visualizacion_batch_loteria.png.

Fase III: Sistema de Streaming en Tiempo Real
Encender el Consumidor: Debe estar listo antes de que el productor envíe datos.
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 loteria_consumer.py
(Nota: Espera a que aparezca el cuadro vacío de "Batch: 0").

Encender el Productor: Es el encargado de alimentar el sistema.
python loteria_producer.py

Monitorización: Regresa a la terminal del Consumidor. Verás cómo las filas empiezan a aparecer con los nombres de las loterías y los valores de venta calculados al instante.
