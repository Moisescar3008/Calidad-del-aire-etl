from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import logging
import os

# Configuración de logging
logger = logging.getLogger(__name__)

# Argumentos por defecto del DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'air_quality_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para análisis de calidad del aire',
    schedule_interval='0 8 * * *',  # Ejecutar diariamente a las 8 AM
    catchup=False,
    tags=['air_quality', 'environmental', 'etl'],
)

# ============================================================================
# FASE EXTRACT: Generación de datos sintéticos de calidad del aire
# ============================================================================

def extract_air_quality_data(**context):
    """
    Extrae datos de calidad del aire.
    En producción, esto se conectaría a APIs como OpenAQ, IQAir, etc.
    Para este proyecto, generamos datos sintéticos realistas.
    """
    try:
        logger.info("Iniciando extracción de datos de calidad del aire...")
        
        # Configuración
        num_days = 90  # 3 meses de datos
        stations = ['Estación_Centro', 'Estación_Norte', 'Estación_Sur', 
                    'Estación_Este', 'Estación_Oeste']
        
        # Generar fechas
        date_range = pd.date_range(
            end=datetime.now(), 
            periods=num_days * 24,  # Datos por hora
            freq='H'
        )
        
        data = []
        np.random.seed(42)
        
        for station in stations:
            # Parámetros base por estación (algunas más contaminadas)
            base_pm25 = np.random.uniform(15, 40)
            base_pm10 = np.random.uniform(25, 60)
            base_no2 = np.random.uniform(20, 50)
            base_o3 = np.random.uniform(30, 60)
            base_co = np.random.uniform(0.3, 1.2)
            
            for timestamp in date_range:
                # Variación por hora del día (más contaminación en horas pico)
                hour = timestamp.hour
                hour_factor = 1.0
                if 7 <= hour <= 9 or 17 <= hour <= 19:  # Horas pico
                    hour_factor = 1.5
                elif 0 <= hour <= 5:  # Madrugada
                    hour_factor = 0.7
                
                # Variación por día de la semana
                weekday = timestamp.weekday()
                weekday_factor = 0.8 if weekday >= 5 else 1.0  # Menos tráfico fin de semana
                
                # Generar valores con ruido realista
                pm25 = max(0, base_pm25 * hour_factor * weekday_factor + 
                          np.random.normal(0, 5))
                pm10 = max(0, base_pm10 * hour_factor * weekday_factor + 
                          np.random.normal(0, 10))
                no2 = max(0, base_no2 * hour_factor * weekday_factor + 
                         np.random.normal(0, 8))
                o3 = max(0, base_o3 * (2 - hour_factor) + np.random.normal(0, 10))
                co = max(0, base_co * hour_factor * weekday_factor + 
                        np.random.normal(0, 0.2))
                
                # Simular algunos valores perdidos (2% de probabilidad)
                if np.random.random() < 0.02:
                    pm25 = None
                if np.random.random() < 0.02:
                    pm10 = None
                
                data.append({
                    'timestamp': timestamp,
                    'station': station,
                    'pm25': pm25,
                    'pm10': pm10,
                    'no2': no2,
                    'o3': o3,
                    'co': co,
                    'temperature': np.random.uniform(10, 30),
                    'humidity': np.random.uniform(30, 80)
                })
        
        # Crear DataFrame
        df = pd.DataFrame(data)
        
        # Guardar datos crudos
        raw_data_path = '/tmp/air_quality_raw.csv'
        df.to_csv(raw_data_path, index=False)
        
        logger.info(f"Datos extraídos exitosamente: {len(df)} registros")
        logger.info(f"Estaciones: {len(stations)}, Período: {num_days} días")
        
        # Push del path a XCom para siguientes tareas
        context['ti'].xcom_push(key='raw_data_path', value=raw_data_path)
        context['ti'].xcom_push(key='total_records', value=len(df))
        
        return raw_data_path
        
    except Exception as e:
        logger.error(f"Error en extracción de datos: {str(e)}")
        raise

# ============================================================================
# FASE TRANSFORM: Limpieza y transformación de datos
# ============================================================================

def transform_air_quality_data(**context):
    """
    Transforma y limpia los datos de calidad del aire.
    Incluye: limpieza de nulos, creación de features, agregaciones.
    """
    try:
        logger.info("Iniciando transformación de datos...")
        
        # Recuperar path de XCom
        raw_data_path = context['ti'].xcom_pull(
            task_ids='extract_data', 
            key='raw_data_path'
        )
        
        # Procesar en chunks para escalabilidad
        chunk_size = 10000
        transformed_chunks = []
        
        for chunk in pd.read_csv(raw_data_path, chunksize=chunk_size):
            # 1. LIMPIEZA DE DATOS
            
            # Convertir timestamp a datetime
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'])
            
            # Manejo de valores perdidos - interpolación lineal
            numeric_cols = ['pm25', 'pm10', 'no2', 'o3', 'co', 
                           'temperature', 'humidity']
            for col in numeric_cols:
                chunk[col] = chunk.groupby('station')[col].transform(
                    lambda x: x.interpolate(method='linear', limit=3)
                )
            
            # Eliminar valores extremos (outliers) usando IQR
            for col in ['pm25', 'pm10', 'no2']:
                Q1 = chunk[col].quantile(0.25)
                Q3 = chunk[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR
                chunk[col] = chunk[col].clip(lower=lower_bound, upper=upper_bound)
            
            # Eliminar duplicados
            chunk = chunk.drop_duplicates(subset=['timestamp', 'station'])
            
            # 2. CREACIÓN DE FEATURES (FEATURE ENGINEERING)
            
            # Calcular AQI (Air Quality Index) simplificado para PM2.5
            def calculate_aqi_pm25(pm25):
                if pd.isna(pm25):
                    return None
                elif pm25 <= 12:
                    return (50 / 12) * pm25
                elif pm25 <= 35.4:
                    return 50 + ((100 - 50) / (35.4 - 12.1)) * (pm25 - 12.1)
                elif pm25 <= 55.4:
                    return 100 + ((150 - 100) / (55.4 - 35.5)) * (pm25 - 35.5)
                elif pm25 <= 150.4:
                    return 150 + ((200 - 150) / (150.4 - 55.5)) * (pm25 - 55.5)
                else:
                    return 200 + ((300 - 200) / (250.4 - 150.5)) * (pm25 - 150.5)
            
            chunk['aqi_pm25'] = chunk['pm25'].apply(calculate_aqi_pm25)
            
            # Categoría de calidad del aire
            def get_air_quality_category(aqi):
                if pd.isna(aqi):
                    return 'Unknown'
                elif aqi <= 50:
                    return 'Good'
                elif aqi <= 100:
                    return 'Moderate'
                elif aqi <= 150:
                    return 'Unhealthy for Sensitive'
                elif aqi <= 200:
                    return 'Unhealthy'
                else:
                    return 'Very Unhealthy'
            
            chunk['air_quality_category'] = chunk['aqi_pm25'].apply(
                get_air_quality_category
            )
            
            # Extraer características temporales
            chunk['hour'] = chunk['timestamp'].dt.hour
            chunk['day_of_week'] = chunk['timestamp'].dt.dayofweek
            chunk['is_weekend'] = chunk['day_of_week'].isin([5, 6]).astype(int)
            chunk['is_rush_hour'] = chunk['hour'].isin([7, 8, 9, 17, 18, 19]).astype(int)
            chunk['month'] = chunk['timestamp'].dt.month
            chunk['date'] = chunk['timestamp'].dt.date
            
            # Índice de contaminación combinado (normalizado)
            chunk['pollution_index'] = (
                (chunk['pm25'] / 50) * 0.3 +
                (chunk['pm10'] / 100) * 0.3 +
                (chunk['no2'] / 100) * 0.2 +
                (chunk['co'] / 2) * 0.2
            )
            
            transformed_chunks.append(chunk)
        
        # Combinar todos los chunks
        df_transformed = pd.concat(transformed_chunks, ignore_index=True)
        
        # 3. AGREGACIONES
        
        # Resumen diario por estación
        daily_summary = df_transformed.groupby(['date', 'station']).agg({
            'pm25': ['mean', 'max', 'min'],
            'pm10': ['mean', 'max'],
            'no2': 'mean',
            'aqi_pm25': 'mean',
            'pollution_index': 'mean',
            'is_rush_hour': 'sum'
        }).reset_index()
        
        daily_summary.columns = ['_'.join(col).strip('_') for col in daily_summary.columns]
        
        # Guardar datos transformados
        transformed_path = '/tmp/air_quality_transformed.csv'
        df_transformed.to_csv(transformed_path, index=False)
        
        # Guardar agregaciones diarias
        daily_summary_path = '/tmp/air_quality_daily_summary.csv'
        daily_summary.to_csv(daily_summary_path, index=False)
        
        # Guardar en formato Parquet para mejor eficiencia
        parquet_path = '/tmp/air_quality_transformed.parquet'
        df_transformed.to_parquet(parquet_path, index=False, compression='snappy')
        
        logger.info(f"Datos transformados: {len(df_transformed)} registros")
        logger.info(f"Valores nulos eliminados/imputados")
        logger.info(f"Features creados: AQI, categorías, índices temporales")
        logger.info(f"Formato Parquet generado para escalabilidad")
        
        # Push paths a XCom
        context['ti'].xcom_push(key='transformed_path', value=transformed_path)
        context['ti'].xcom_push(key='daily_summary_path', value=daily_summary_path)
        context['ti'].xcom_push(key='parquet_path', value=parquet_path)
        
        return transformed_path
        
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}")
        raise

# ============================================================================
# FASE LOAD: Carga de datos a destino final
# ============================================================================

def load_to_postgres(**context):
    """
    Carga los datos transformados a PostgreSQL.
    Alternativa: también guardamos en archivos locales.
    """
    try:
        logger.info("Iniciando carga de datos...")
        
        # Recuperar paths
        transformed_path = context['ti'].xcom_pull(
            task_ids='transform_data',
            key='transformed_path'
        )
        daily_summary_path = context['ti'].xcom_pull(
            task_ids='transform_data',
            key='daily_summary_path'
        )
        
        # Leer datos transformados
        df_transformed = pd.read_csv(transformed_path)
        df_daily = pd.read_csv(daily_summary_path)
        
        # OPCIÓN 1: Guardar en carpeta local (para el proyecto)
        output_dir = '/tmp/air_quality_output'
        os.makedirs(output_dir, exist_ok=True)
        
        final_path = os.path.join(output_dir, 'air_quality_final.csv')
        daily_path = os.path.join(output_dir, 'air_quality_daily.csv')
        
        df_transformed.to_csv(final_path, index=False)
        df_daily.to_csv(daily_path, index=False)
        
        logger.info(f"Datos guardados en: {output_dir}")
        logger.info(f"Archivo principal: {final_path}")
        logger.info(f"Resumen diario: {daily_path}")
        
        # OPCIÓN 2: Cargar a PostgreSQL (comentado - descomentar si tienes Postgres)
        """
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Cargar datos a tablas
        df_transformed.to_sql(
            'air_quality_hourly',
            engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        df_daily.to_sql(
            'air_quality_daily',
            engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info("Datos cargados exitosamente a PostgreSQL")
        """
        
        # Push información final
        context['ti'].xcom_push(key='final_path', value=final_path)
        context['ti'].xcom_push(key='daily_path', value=daily_path)
        context['ti'].xcom_push(key='records_loaded', value=len(df_transformed))
        
        return final_path
        
    except Exception as e:
        logger.error(f"Error en carga de datos: {str(e)}")
        raise

# ============================================================================
# VALIDACIÓN Y CALIDAD DE DATOS
# ============================================================================

def validate_data_quality(**context):
    """
    Valida la calidad de los datos cargados.
    """
    try:
        logger.info("Validando calidad de datos...")
        
        final_path = context['ti'].xcom_pull(
            task_ids='load_data',
            key='final_path'
        )
        
        df = pd.read_csv(final_path)
        
        # Validaciones
        validations = {
            'total_records': len(df),
            'null_percentage': (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100,
            'unique_stations': df['station'].nunique(),
            'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
            'avg_aqi': df['aqi_pm25'].mean(),
            'records_good_quality': len(df[df['air_quality_category'] == 'Good'])
        }
        
        logger.info("=" * 50)
        logger.info("REPORTE DE CALIDAD DE DATOS")
        logger.info("=" * 50)
        for key, value in validations.items():
            logger.info(f"{key}: {value}")
        logger.info("=" * 50)
        
        # Verificar que hay suficientes datos
        if validations['total_records'] < 1000:
            raise ValueError("Datos insuficientes para análisis")
        
        if validations['null_percentage'] > 5:
            logger.warning(f"Alto porcentaje de valores nulos: {validations['null_percentage']:.2f}%")
        
        return validations
        
    except Exception as e:
        logger.error(f"Error en validación: {str(e)}")
        raise

# ============================================================================
# DEFINICIÓN DE TAREAS
# ============================================================================

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_air_quality_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_air_quality_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgres,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# ============================================================================
# DEFINICIÓN DE DEPENDENCIAS (DAG Flow)
# ============================================================================

extract_task >> transform_task >> load_task >> validate_task