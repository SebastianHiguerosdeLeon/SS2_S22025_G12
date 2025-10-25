CREATE SCHEMA IF NOT EXISTS `semi2-proyecto-g12.fase2`
OPTIONS (location = "US");

-- tabla de features

CREATE OR REPLACE TABLE semi2-proyecto-g12.fase2.taxi_features AS
SELECT
fare_amount,
tip_amount,
trip_distance,
passenger_count,
payment_type,
EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_day,
pickup_datetime
FROM semi2-proyecto-g12.fase1.taxi_trips_copia
WHERE trip_distance > 0
AND fare_amount > 0
AND total_amount >= 0
AND passenger_count BETWEEN 1 AND 6;


-- tabla de datos de entrenamiento

CREATE OR REPLACE TABLE semi2-proyecto-g12.fase2.train_data AS
SELECT * FROM semi2-proyecto-g12.fase2.taxi_features
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(pickup_datetime AS STRING))), 10) < 8;


--- tabla de datos de prueba

CREATE OR REPLACE TABLE semi2-proyecto-g12.fase2.test_data AS
SELECT * FROM semi2-proyecto-g12.fase2.taxi_features
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(pickup_datetime AS STRING))), 10) >= 8;


-- modelo lineal

CREATE OR REPLACE MODEL semi2-proyecto-g12.fase2.model_linear_tip
OPTIONS(
model_type = 'LINEAR_REG',
input_label_cols = ['tip_amount']
) AS
SELECT
fare_amount,
trip_distance,
passenger_count,
payment_type,
pickup_hour,
pickup_day,
tip_amount
FROM semi2-proyecto-g12.fase2.train_data;


-- modelo boosted tree

CREATE OR REPLACE MODEL semi2-proyecto-g12.fase2.model_boosted_tip
OPTIONS(
model_type = 'BOOSTED_TREE_REGRESSOR',
input_label_cols = ['tip_amount'],
max_iterations = 30,
learn_rate = 0.1
) AS
SELECT
fare_amount,
trip_distance,
passenger_count,
payment_type,
pickup_hour,
pickup_day,
tip_amount
FROM semi2-proyecto-g12.fase2.train_data;


-- Evaluación del modelo lineal
CREATE OR REPLACE TABLE semi2-proyecto-g12.fase2.eval_linear AS
SELECT * FROM ML.EVALUATE(
MODEL semi2-proyecto-g12.fase2.model_linear_tip,
(SELECT * FROM semi2-proyecto-g12.fase2.test_data)
);


-- Evaluación del modelo Boosted Tree
CREATE OR REPLACE TABLE semi2-proyecto-g12.fase2.eval_boosted AS
SELECT * FROM ML.EVALUATE(
MODEL semi2-proyecto-g12.fase2.model_boosted_tip,
(SELECT * FROM semi2-proyecto-g12.fase2.test_data)
);


-- Comparación de resultados
SELECT 'LINEAR' AS modelo, * FROM semi2-proyecto-g12.fase2.eval_linear
UNION ALL
SELECT 'BOOSTED_TREE' AS modelo, * FROM semi2-proyecto-g12.fase2.eval_boosted;


-- tabla de predicciones con el modelo seleccionado (BOOSTED TREE)

CREATE OR REPLACE TABLE semi2-proyecto-g12.fase2.predicciones_tip AS
SELECT
trip_distance,
fare_amount,
passenger_count,
payment_type,
predicted_tip_amount,
tip_amount AS real_tip,
ROUND(ABS(tip_amount - predicted_tip_amount), 2) AS error_absoluto
FROM ML.PREDICT(
MODEL semi2-proyecto-g12.fase2.model_boosted_tip,
(SELECT * FROM semi2-proyecto-g12.fase2.test_data)
);