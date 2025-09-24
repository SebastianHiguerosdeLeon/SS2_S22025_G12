/* ============================================================
   CONSULTAS SEMI2 - PROYECTO G12
   Proyecto: semi2-proyecto-g12
   Dataset:  fase1
   ============================================================ */


-- ============================================================
-- 0. CONSULTAS EXPLORATORIAS INICIALES
-- ============================================================

-- 1. Contar todas las filas de la tabla pública
SELECT COUNT(*) AS total_filas
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`;

-- 2. Revisar nulos en la columna fare_amount
SELECT
  COUNTIF(fare_amount IS NULL) AS nulos_tarifa,
  COUNTIF(fare_amount IS NOT NULL) AS no_nulos_tarifa
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`;

-- 3. Revisar nulos en columnas clave
SELECT
  COUNTIF(pickup_datetime IS NULL) AS nulos_pickup,
  COUNTIF(dropoff_datetime IS NULL) AS nulos_dropoff,
  COUNTIF(passenger_count IS NULL) AS nulos_pasajeros,
  COUNTIF(trip_distance IS NULL) AS nulos_distancia,
  COUNTIF(total_amount IS NULL) AS nulos_total,
  COUNTIF(payment_type IS NULL) AS nulos_pago
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`;



/* ============================================================
   1. TABLA BASE LIMPIA (Q1) → taxi_trips_copia
   ============================================================ */
CREATE OR REPLACE TABLE `semi2-proyecto-g12.fase1.taxi_trips_copia`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY pickup_location_id, dropoff_location_id, payment_type AS
SELECT *
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
WHERE data_file_month BETWEEN 1 AND 3
  AND trip_distance > 0
  AND total_amount >= 0
  AND fare_amount >= 0
  AND passenger_count BETWEEN 1 AND 6;

-- Chequeo esquema
SELECT column_name, data_type
FROM `semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'taxi_trips_copia'
ORDER BY  column_name;

-- Chequeo particiones
SELECT partition_id, total_rows
FROM semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'taxi_trips_copia'
ORDER BY partition_id
LIMIT 20;



/* ============================================================
   2. KPIs MENSUALES (Q1) → metrica_mensual_q1
   ============================================================ */
CREATE OR REPLACE TABLE `semi2-proyecto-g12.fase1.metrica_mensual_q1`
PARTITION BY month_date
CLUSTER BY payment_type AS
SELECT
  DATE_TRUNC(DATE(pickup_datetime), MONTH) AS month_date,  -- columna de partición
  payment_type,
  COUNT(*) AS trips,
  ROUND(AVG(trip_distance), 2) AS avg_distance,
  ROUND(AVG(total_amount), 2) AS avg_total,
  ROUND(AVG(tip_amount), 2) AS avg_tip
FROM `semi2-proyecto-g12.fase1.taxi_trips_copia`
GROUP BY month_date, payment_type;

-- Chequeo esquema
SELECT column_name, data_type
FROM `semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'metrica_mensual_q1';

-- Chequeo particiones
SELECT partition_id, total_rows
FROM `semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'metrica_mensual_q1';



/* ============================================================
   3. DISTRIBUCIÓN DE PROPINAS (Q1) → propinas_q1
   ============================================================ */
CREATE OR REPLACE TABLE `semi2-proyecto-g12.fase1.propinas_q1`
PARTITION BY mes
CLUSTER BY propinas AS 
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
  FROM `semi2-proyecto-g12.fase1.taxi_trips_copia`
)
SELECT mes, propinas, COUNT(*) AS viajes
FROM datos
GROUP BY mes, propinas;

-- Chequeo esquema
SELECT column_name, data_type
FROM `semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'propinas_q1';

-- Chequeo particiones
SELECT partition_id, total_rows
FROM `semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'propinas_q1';



/* ============================================================
   4. DEMANDA POR HORA (Q1) → demanda_horas_zona_q1
   ============================================================ */
CREATE OR REPLACE TABLE `semi2-proyecto-g12.fase1.demanda_horas_zona_q1`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY pickup_location_id, hora_dia AS
SELECT
  DATE(pickup_datetime) AS fecha_viaje,
  EXTRACT(HOUR FROM pickup_datetime) AS hora_dia,
  pickup_location_id,
  COUNT(*) AS viajes,
  ANY_VALUE (pickup_datetime) AS pickup_datetime
FROM `semi2-proyecto-g12.fase1.taxi_trips_copia`
GROUP BY fecha_viaje, hora_dia, pickup_location_id;

-- Chequeo esquema
SELECT column_name, data_type
FROM `semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'demanda_horas_zona_q1';

-- Chequeo particiones
SELECT partition_id, total_rows
FROM `semi2-proyecto-g12.fase1.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'demanda_horas_zona_q1';



-- Verificacion de las particiones
SELECT COUNT(trip_distance) AS filas_q1
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-03-31';


-- Verificacion del clustering
SELECT COUNT(trip_distance) AS viajes_zona237_febrero
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-02-01' AND '2022-02-28'
  AND pickup_location_id = '237';

SELECT COUNT(trip_distance) AS viajes_cash_febrero
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-02-01' AND '2022-02-28'
  AND payment_type = '2';


-- Consultas para generar dashboard
SELECT pickup_location_id, COUNT(*) AS viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
GROUP BY pickup_location_id
ORDER BY viajes DESC
LIMIT 10;

SELECT month_date, SUM(trips) AS total_viajes
FROM semi2-proyecto-g12.fase1.metrica_mensual_q1
GROUP BY month_date
ORDER BY month_date;

SELECT hora_dia, SUM(viajes) AS total_viajes
FROM semi2-proyecto-g12.fase1.demanda_horas_zona_q1
WHERE fecha_viaje BETWEEN '2022-02-01' AND '2022-02-28'
GROUP BY hora_dia
ORDER BY hora_dia;

SELECT payment_type, SUM(trips) AS total_viajes
FROM semi2-proyecto-g12.fase1.metrica_mensual_q1
GROUP BY payment_type
ORDER BY total_viajes DESC;

SELECT propinas, SUM(viajes) AS total_viajes
FROM semi2-proyecto-g12.fase1.propinas_q1
GROUP BY propinas
ORDER BY total_viajes DESC;

SELECT pickup_location_id, COUNT(*) AS total_viajes
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-03-31'
GROUP BY pickup_location_id
ORDER BY total_viajes DESC
LIMIT 10;