# 📊 Proyecto Fase 1 – Análisis Exploratorio de Datos Masivos en BigQuery
**Curso:** Seminario de Sistemas 2  
**Universidad:** USAC – Facultad de Ingeniería  
**Grupo:** G12  
**Integrantes:** [Nombres del equipo]

---

## 📌 Introducción
Este proyecto analiza los viajes en taxi de Nueva York durante el año 2022 usando el dataset público de BigQuery:  
`bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`.

Se aplicaron técnicas de validación, limpieza, creación de tablas optimizadas, consultas SQL y visualización de resultados en Google Data Studio / Looker Studio.

---

## 🔎 Validaciones del dataset

📷 **Comprobación dataset público**  
![Validación dataset](imagenes/comprobacion-dataset-publico.png)

📷 **Validación de columnas y nulos en dataset público**  
![Validación columnas](imagenes/validacion-datos-dataset-publico.png)

📷 **Consulta validación de nulos en dataset trabajado**  
![Validación nulos dataset](imagenes/Validacion-datos-nulos-dataset.png)

📷 **Resultado validación de nulos en dataset trabajado**  
![Resultado nulos](imagenes/resultado-datos-nulos-dataset.png)

---

## 🛠️ Creación de tablas

📷 **Creación tabla base (taxi_trips_copia)**  
![Creación tabla base](imagenes/creacion_tabla_base.jpg)

📷 **Información de tabla creada**  
![Info tabla](imagenes/informacion-creacion-tabla.jpg)

📷 **Creación tabla de métricas mensuales**  
![Métricas mensuales](imagenes/creacion-tabla-metrica-mensual.jpg)

📷 **Creación tabla de propinas**  
![Tabla propinas](imagenes/creacion-tabla-propinas.png)

📷 **Creación tabla demanda por hora y zona**  
![Tabla demanda](imagenes/creacion-tabla-demanda-hora-zona.png)

📷 **Exploración de columnas – Parte 1**  
![Datos parte 1](imagenes/datos-1.png)

📷 **Exploración de columnas – Parte 2**  
![Datos parte 2](imagenes/datos-2.png)

---

## ✅ Validación de tablas

📷 **Validación de tabla base**  
![Validación tabla base](imagenes/validacion-tabla-base.jpg)

📷 **Resultado validación tabla base (bytes y estructura)**  
![Resultado validación](imagenes/resultado-validacion-tabla-base.jpg)

📷 **Validación de tabla demanda**  
![Validación demanda](imagenes/validacion-tabla-demanda.png)

📷 **Validación de tabla propinas**  
![Validación propinas](imagenes/validacion-tabla-propinas.png)

---

## 📈 Consultas y métricas

📷 **Consulta total de viajes Q1 (enero–marzo)**  
![Consulta Q1](imagenes/2.png)

📷 **Consulta viajes en febrero**  
![Consulta febrero](imagenes/3.png)

📷 **Consulta viajes en efectivo en febrero**  
![Consulta efectivo febrero](imagenes/4.png)

📷 **Consulta viajes desde zona específica (ejemplo zona 237)**  
![Consulta zona 237](imagenes/5.png)

📷 **Consulta distribución de pasajeros**  
![Distribución pasajeros](imagenes/11.png)

📷 **Consulta relación distancia – tarifa – propina**  
![Relación distancia tarifa propina](imagenes/12.png)

📷 **Consulta duracion promedio de los viajes**

![Duracion Promedio de Viaje](imagenes/13.png)

📷 **Consulta distribucion cantidad de pasajeros por viajes**

![Distribucion cantidad pasajeros por viaje](imagenes/15.png)

![Resultados cantidad pasajeros por viaje](imagenes/16.png)

📷 **Consulta viajes por dia de la semana**

![Viajes por dia de la semana](imagenes/17.png)

📷 **Consulta Reduccion de bytes**

![Sin optimizar](imagenes/18.png)

![Optimizada](imagenes/23.png)

📷 **Consulta Numero de viajes por cantidad de pasajeros**

![Resultados numero viajes por cantidad pasajeros](imagenes/19.png)

![numero viajes por cantidad pasajeros](imagenes/20.png)

📷 **Consulta relacion distancia-tarifa-propina**

![Resultados relacion distancia-tarfia-propina](imagenes/21.png)

![relacion distancia-tarifa-propina](imagenes/22.png)

---

## 📊 Visualizaciones / Dashboard

📷 **Visualización viajes por mes**  
![Viajes por mes](imagenes/1.png)

📷 **Visualización viajes por hora del día**  
![Viajes por hora](imagenes/7.png)

📷 **Visualización viajes por método de pago**  
![Viajes por método de pago](imagenes/8.png)

📷 **Visualización viajes por propinas**  
![Viajes por propinas](imagenes/9.png)

📷 **Visualización top 10 zonas**  
![Top zonas](imagenes/6.png)

👉 **[Enlace al Dashboard](imagenes/URL_DEL_DASHBOARD)**

---

## ⚡ Optimización y bytes procesados

Se compararon consultas en el dataset original y en la tabla optimizada (`taxi_trips_copia`) con **particiones y clustering**, mostrando la reducción en bytes procesados.

📷 **Evidencia reducción de bytes procesados**  
![Optimización bytes](imagenes/resultado-validacion-tabla-base.jpg)

---

## 📜 Conclusiones
- Se comprobó la importancia de usar **particiones y clustering** para reducir costos y tiempos de consulta.  
- Se identificaron **patrones temporales** (por mes, hora, día de la semana).  
- Se analizaron **patrones categóricos** (métodos de pago, propinas, número de pasajeros).  
- Se generaron métricas clave: distancia promedio, duración, montos y propinas.  
- Se construyó un dashboard interactivo que facilita la interpretación visual.  

---
