/* ============================================================
   CONSULTAS SEMI2 - PROYECTO G12
   Proyecto: semi2-proyecto-g12
   Dataset:  fase1
   ============================================================ */


/* ============================================================
   0. CONSULTAS EXPLORATORIAS INICIALES
   ============================================================ */

-- 1. Contar todas las filas de la tabla pública (dataset original de NYC taxis 2022)
SELECT COUNT(*) AS total_filas
FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022;

-- 2. Revisar nulos en la columna fare_amount (tarifa del viaje)
SELECT
  COUNTIF(fare_amount IS NULL) AS nulos_tarifa,
  COUNTIF(fare_amount IS NOT NULL) AS no_nulos_tarifa
FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022;

-- 3. Revisar nulos en columnas clave (fechas, pasajeros, distancia, montos, forma de pago)
SELECT
  COUNTIF(pickup_datetime IS NULL) AS nulos_pickup,
  COUNTIF(dropoff_datetime IS NULL) AS nulos_dropoff,
  COUNTIF(passenger_count IS NULL) AS nulos_pasajeros,
  COUNTIF(trip_distance IS NULL) AS nulos_distancia,
  COUNTIF(total_amount IS NULL) AS nulos_total,
  COUNTIF(payment_type IS NULL) AS nulos_pago
FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022;



/* ============================================================
   1. TABLA BASE LIMPIA (Q1) → taxi_trips_copia
   ============================================================ */

-- Se crea una copia filtrada: solo enero-marzo, con datos válidos
CREATE OR REPLACE TABLE semi2-proyecto-g12.fase1.taxi_trips_copia
PARTITION BY DATE(pickup_datetime)                         -- partición por fecha del viaje
CLUSTER BY pickup_location_id, dropoff_location_id, payment_type AS   -- clustering por zonas y tipo de pago
SELECT *
FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022
WHERE data_file_month BETWEEN 1 AND 3      -- solo Q1 (enero a marzo)
  AND trip_distance > 0                    -- viajes válidos (> 0 km)
  AND total_amount >= 0                    -- montos válidos
  AND fare_amount >= 0
  AND passenger_count BETWEEN 1 AND 6;     -- pasajeros válidos (descarta outliers)

-- Chequeo del esquema de la tabla creada
SELECT column_name, data_type
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'taxi_trips_copia'
ORDER BY column_name;

-- Chequeo de las particiones de la tabla
SELECT partition_id, total_rows
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'taxi_trips_copia'
ORDER BY partition_id
LIMIT 20;



/* ============================================================
   2. KPIs MENSUALES (Q1) → metrica_mensual_q1
   ============================================================ */

-- Se genera tabla con métricas por mes y tipo de pago
CREATE OR REPLACE TABLE semi2-proyecto-g12.fase1.metrica_mensual_q1
PARTITION BY month_date                 -- partición por mes
CLUSTER BY payment_type AS              -- clustering por tipo de pago
SELECT
  DATE_TRUNC(DATE(pickup_datetime), MONTH) AS month_date,  -- mes del viaje
  payment_type,                                        -- tipo de pago
  COUNT(*) AS trips,                                   -- número de viajes
  ROUND(AVG(trip_distance), 2) AS avg_distance,        -- distancia promedio
  ROUND(AVG(total_amount), 2) AS avg_total,            -- monto promedio
  ROUND(AVG(tip_amount), 2) AS avg_tip                 -- propina promedio
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY month_date, payment_type;

-- Chequeo esquema de la tabla de KPIs
SELECT column_name, data_type
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'metrica_mensual_q1';

-- Chequeo de particiones de la tabla
SELECT partition_id, total_rows
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'metrica_mensual_q1';



/* ============================================================
   3. DISTRIBUCIÓN DE PROPINAS (Q1) → propinas_q1
   ============================================================ */

-- Se clasifica la propina en categorías
CREATE OR REPLACE TABLE semi2-proyecto-g12.fase1.propinas_q1
PARTITION BY mes                -- partición por mes
CLUSTER BY propinas AS          -- clustering por categoría de propina
WITH datos AS (
  SELECT 
    DATE_TRUNC(DATE(pickup_datetime), MONTH) AS mes,
    CASE
      WHEN tip_amount = 0 THEN 'Sin propina'
      WHEN tip_amount <= 2 THEN 'Hasta 2 USD'
      WHEN tip_amount <= 5 THEN '2–5 USD'
      WHEN tip_amount <= 10 THEN '5–10 USD'
      ELSE 'Más de 10 USD'
    END AS propinas
  FROM semi2-proyecto-g12.fase1.taxi_trips_copia
)
SELECT mes, propinas, COUNT(*) AS viajes
FROM datos
GROUP BY mes, propinas;

-- Chequeo esquema de la tabla de propinas
SELECT column_name, data_type
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'propinas_q1';

-- Chequeo particiones de la tabla
SELECT partition_id, total_rows
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'propinas_q1';



/* ============================================================
   4. DEMANDA POR HORA (Q1) → demanda_horas_zona_q1
   ============================================================ */

-- Se genera tabla con cantidad de viajes por día, hora y zona
CREATE OR REPLACE TABLE semi2-proyecto-g12.fase1.demanda_horas_zona_q1
PARTITION BY DATE(pickup_datetime)              -- partición por fecha
CLUSTER BY pickup_location_id, hora_dia AS      -- clustering por zona y hora
SELECT
  DATE(pickup_datetime) AS fecha_viaje,         -- fecha del viaje
  EXTRACT(HOUR FROM pickup_datetime) AS hora_dia,  -- hora del viaje
  pickup_location_id,                           -- zona de recogida
  COUNT(*) AS viajes,                           -- número de viajes
  ANY_VALUE (pickup_datetime) AS pickup_datetime -- muestra un valor de ejemplo
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY fecha_viaje, hora_dia, pickup_location_id;

-- Chequeo esquema de la tabla
SELECT column_name, data_type
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'demanda_horas_zona_q1';

-- Chequeo particiones de la tabla
SELECT partition_id, total_rows
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'demanda_horas_zona_q1';



/* ============================================================
   5. VERIFICACIONES DE PARTICIONES Y CLUSTERING
   ============================================================ */

-- Verificación: la tabla solo tiene datos enero-marzo
SELECT COUNT(trip_distance) AS filas_q1
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-03-31';

-- Verificación del clustering: viajes desde la zona 237 en febrero
SELECT COUNT(trip_distance) AS viajes_zona237_febrero
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-02-01' AND '2022-02-28'
  AND pickup_location_id = '237';

-- Verificación del clustering: viajes pagados en efectivo (2) en febrero
SELECT COUNT(trip_distance) AS viajes_cash_febrero
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-02-01' AND '2022-02-28'
  AND payment_type = '2';



/* ============================================================
   6. CONSULTAS PARA DASHBOARD
   ============================================================ */

-- Top 10 zonas de recogida con más viajes (Q1)
SELECT pickup_location_id, COUNT(*) AS viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY pickup_location_id
ORDER BY viajes DESC
LIMIT 10;

-- Evolución mensual de viajes totales
SELECT month_date, SUM(trips) AS total_viajes
FROM semi2-proyecto-g12.fase1.metrica_mensual_q1
GROUP BY month_date
ORDER BY month_date;

-- Distribución de viajes por hora del día (ejemplo: febrero)
SELECT hora_dia, SUM(viajes) AS total_viajes
FROM semi2-proyecto-g12.fase1.demanda_horas_zona_q1
WHERE fecha_viaje BETWEEN '2022-02-01' AND '2022-02-28'
GROUP BY hora_dia
ORDER BY hora_dia;

-- Distribución de viajes por tipo de pago
SELECT payment_type, SUM(trips) AS total_viajes
FROM semi2-proyecto-g12.fase1.metrica_mensual_q1
GROUP BY payment_type
ORDER BY total_viajes DESC;

-- Distribución de viajes por categorías de propinas
SELECT propinas, SUM(viajes) AS total_viajes
FROM semi2-proyecto-g12.fase1.propinas_q1
GROUP BY propinas
ORDER BY total_viajes DESC;

-- Top 10 zonas de recogida en Q1
SELECT pickup_location_id, COUNT(*) AS total_viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-03-31'
GROUP BY pickup_location_id
ORDER BY total_viajes DESC
LIMIT 10;

/* ============================================================
   7. CONSULTAS ADICIONALES REQUERIDAS SEGÚN LA GUÍA DEL PROYECTO
   ============================================================ */

-- 7.1 Duración promedio de viajes (en minutos)
-- Permite conocer el tiempo promedio que duran los viajes en Q1.
SELECT 
  ROUND(AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)), 2) 
    AS duracion_promedio_minutos
FROM semi2-proyecto-g12.fase1.taxi_trips_copia;

-- 7.2 Distribución de pasajeros
-- Analiza cuántos viajes se realizan con 1, 2, 3… pasajeros.
SELECT 
  passenger_count, 
  COUNT(*) AS total_viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY passenger_count
ORDER BY passenger_count;

-- 7.3 Viajes por día de la semana
-- Permite observar patrones de demanda según el día.
-- Opción A: devuelve número del día (1=Lunes ... 7=Domingo)
SELECT 
  EXTRACT(DAYOFWEEK FROM pickup_datetime) AS dia_semana,
  COUNT(*) AS total_viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY dia_semana
ORDER BY dia_semana;

-- Opción B: devuelve nombre del día (Lunes, Martes, etc.)
SELECT 
  FORMAT_DATE('%A', DATE(pickup_datetime)) AS dia_semana,
  COUNT(*) AS total_viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY dia_semana
ORDER BY total_viajes DESC;

-- 7.4 Comparación de costos antes y después de la optimización
-- Ejemplo de consultas para demostrar reducción de bytes procesados en BigQuery.
-- IMPORTANTE: capturar pantalla de los bytes procesados en cada ejecución.

-- Consulta sin optimización (dataset original)
SELECT COUNT(*) 
FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022
WHERE EXTRACT(MONTH FROM pickup_datetime) = 2;

-- Consulta optimizada (tabla con partición/clustering en Q1)
SELECT COUNT(*) 
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE EXTRACT(MONTH FROM pickup_datetime) = 2;

/* ============================================================
   DISTRIBUCIÓN DE PASAJEROS
   ============================================================ */

-- Número de viajes por cantidad de pasajeros
SELECT 
  passenger_count, 
  COUNT(*) AS total_viajes,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS porcentaje
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY passenger_count
ORDER BY passenger_count;

/* ============================================================
   RELACIÓN DISTANCIA / TARIFA / PROPINA
   ============================================================ */

-- Agrupación de viajes por rangos de distancia
SELECT
  CASE
    WHEN trip_distance <= 2 THEN '0-2 km'
    WHEN trip_distance <= 5 THEN '2-5 km'
    WHEN trip_distance <= 10 THEN '5-10 km'
    WHEN trip_distance <= 20 THEN '10-20 km'
    ELSE '20+ km'
  END AS rango_distancia,
  ROUND(AVG(total_amount), 2) AS tarifa_promedio,
  ROUND(AVG(tip_amount), 2) AS propina_promedio,
  COUNT(*) AS total_viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY rango_distancia
ORDER BY total_viajes DESC;
