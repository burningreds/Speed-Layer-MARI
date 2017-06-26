# Speed-Layer-MARI
Tarea 3 Big Data

Se tienen datos de boletas del retail MARI, los cuales se almacenan a una cola local en Kafka.

Se responden las siguientes consultas haciendo procesamiento en tiempo real:
- ¿Existe alguna categoría que venda más?
- ¿Cuál es el promedio por boleta de cada cliente?
- ¿Cuales son los 10 productos más vendidos?
- ¿En qué sucursal se hacen más ventas?

Además se genera un archivo json con las ubicaciones de sucursales para visualizaciones con D3.

Para esto se hace procesamiento en stream usando Apache Storm y se almacenan las consultas procesadas en Apache Cassandra.

Se utiliza el keyspace:
```
CREATE KEYSPACE tarea3 WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor' : 1};
```

Y se necesitan las siguientes tablas:
```
CREATE TABLE top10categories (category text, count bigint, update_datetime timestamp, PRIMARY KEY (category));
CREATE TABLE client_total (client text, total double, count bigint, update_date date, PRIMARY KEY (client));
CREATE TABLE top10products (item_id int, count bigint, name text, update_datetime timestamp, PRIMARY KEY (item_id));
CREATE TABLE topsucursal (sucursal_id int, total_sales double, address text, update_datetime timestamp, PRIMARY KEY (sucursal_id));
CREATE TABLE sucursal_location (sucursal_id int, lng double, lat double, PRIMARY KEY (sucursal_id));
```
