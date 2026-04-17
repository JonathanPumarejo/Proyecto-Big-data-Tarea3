import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

# --- CONFIGURACIÓN ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'loteria_ventas'
CSV_FILE = 'ventas_loteria.csv'

def limpiar_valor(valor):
    if pd.isna(valor): return 0.0
    # Quitamos puntos (miles), comas (decimales), comillas y espacios
    s = str(valor).replace('"', '').replace('.', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return 0.0

def run_producer():
    if not os.path.exists(CSV_FILE):
        print("Error: No se encuentra el archivo CSV.")
        return

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        # Cargamos el CSV ignorando los nombres de las columnas problemáticos
        # Usamos header=0 para saltar la primera fila y nombres manuales
        df = pd.read_csv(CSV_FILE, encoding='latin-1', header=0, names=['COL0', 'COL1', 'COL2', 'COL3'])

        print("Iniciando envío de datos...")

        for _, row in df.iterrows():
            # Accedemos por posición para evitar errores de caracteres especiales
            anio_raw = str(row['COL0']).replace('.', '') # De "2.012" a "2012"
            
            evento = {
                "ANIO": int(float(anio_raw)),
                "MES": int(str(row['COL1']).replace('"', '')),
                "LOTERIA": str(row['COL2']).replace('"', '').strip(),
                "VENTAS": limpiar_valor(row['COL3']),
                "TIMESTAMP": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            producer.send(KAFKA_TOPIC, value=evento)
            print(f"Enviado: {evento['LOTERIA']} | Año: {evento['ANIO']} | Ventas: ${evento['VENTAS']:,.0f}")
            time.sleep(0.5)

    except Exception as e:
        print(f"Ocurrió un error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
