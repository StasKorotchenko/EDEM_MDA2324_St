# CASO DE USO: Visualización de Datos de Near Earth Asteroids and Comets  en Tiempo Real con Kafka

Описание

### 1. Adquisición de Datos:

- [producer.py](producer.py) 
- JSON archivo de https://data.nasa.gov/resource/2vr3-k9wn.json
- Transmisión de datos al topic de Kafka llamado 'Near_Earth_Asteroids_and_Comets'.

```json
{
"designation": "419880 (2011 AH37)",
"discovery_date": "2011-01-07T00:00:00.000",
"h_mag": "19.7",
"moid_au": "0.035",
"q_au_1": "0.84",
"q_au_2": "4.26",
"period_yr": "4.06",
"i_deg": "9.65",
"pha": "Y",
"orbit_class": "Apollo"
}
```
### 2. Filtrado y Enrutamiento:
- [consumer.py](consumer.py)
- Consumo de datos del topic 'Near_Earth_Asteroids_and_Comets'.
- Filtrado para seleccionar información de orbitas seleccionadas: 

  - Apollo
  - Amor

```json
вставить файл
```
### 3. Procesamiento con Ksql
- Filtrado de datos por año 2011.
- Selección de variables: Designation, Discovery_date, period.
- Envío de datos procesados al topic 'Asteroids_and_Comets'.

Queries en Ksql
 ```sql
-- Crea un STREAM para consumir datos del topic Near_Earth_Asteroids_and_Comets_Orbits_Apollo_Amor
CREATE STREAM Asteroids_and_Comets_stream (designation STRING, discovery_date STRING, h_mag FLOAT, moid_au FLOAT, q_au_1 FLOAT, q_au_2 FLOAT, period_yr FLOAT,i_deg FLOAT, pha STRING, orbit_class STRING) WITH (KAFKA_TOPIC='Near_Earth_Asteroids_and_Comets_Orbits_Apollo_Amor', VALUE_FORMAT='JSON');

-- Crea una tabla por el año 2011
CREATE TABLE data_2011 AS
SELECT designation, discovery_date, period_yr, orbit_class
FROM Asteroids_and_Comets_stream
WHERE YEAR(discovery_date) = 2011;

-- Envia datos transformados al topic Asteroids_and_Comets_stream_2011
CREATE STREAM  Asteroids_and_Comets_stream_2011 WITH (VALUE_FORMAT='JSON') AS
SELECT * FROM data_2011;

```
