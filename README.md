# Speed-Layer-MARI
Tarea 3 Big Data

Grupo 7 (Gabriel Dintrans, Paula Ríos, Carlos Vega)

### Descripción
Se tienen datos de boletas del retail MARI, los cuales se almacenan a una cola local en Kafka.

Se responden las siguientes consultas haciendo procesamiento en tiempo real:
- ¿Existe alguna categoría que venda más?
- ¿Cuál es el promedio por boleta de cada cliente?
- ¿Cuales son los 10 productos más vendidos?
- ¿En qué sucursal se hacen más ventas?

Además se genera un archivo json con las ubicaciones de sucursales para visualizaciones con D3.

Para esto se hace procesamiento en stream usando Apache Storm y se almacenan las consultas procesadas en Apache Cassandra.

### Tablas y keyspace necesarios en Cassandra

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

### Instrucciones para correr consultas:

1. Tener corriendo Zookeeper, Kafka y Cassandra.
2. Correr cola Kafka
3. Correr la topología correspondiente a la consulta que queremos responder (ver sección siguiente). Las tablas se actualizan cada 60 segundos, por lo que se recomienda correrla por al menos esa cantidad de tiempo para obtener resultados.
4. Correr la consulta a la tabla generada por la topología.

### Topologías y consultas

Para cada consulta definida anteriormente se tiene una topología y una clase que realiza la consulta a la tabla generada y obtiene las respuestas:

- ¿Existe alguna categoría que venda más?: Se genera un top 10 con TopCategoriesTopology y se consulta con TopCategories.
- ¿Cuál es el promedio por boleta de cada cliente?: Se genera una tbla con total compra y cantidad de compras por cliente con ClientTotalAvgTopology y se consulta obteniendo promedio con ClientTotalAvg.
- ¿Cuales son los 10 productos más vendidos?: Se genera un top 10 con TopProductsTopology y se consulta con TopProducts.
- ¿En qué sucursal se hacen más ventas?: Se genera una tabla con la sucursal con más ventas y sus datos y se consulta con TopSucursal

Además para generar el json con las ubicaciones de las sucursales: Se genera una tabla para las sucursales, su latitud y longitud y se consulta con SucursalLocations, la cual además genera el archivo json.

### Bonus

En la carpeta Bonus se encuentra el código para la visualización de las sucursales en el mapa de Chile (Visualizacion_comunas.html) usando la librería D3.js. Se utiliza un archivo .json con 1000 sucursales el cual se encuentra en la misma carpeta. Se puede hacer zoom con el navegador para ver más en detalle.
