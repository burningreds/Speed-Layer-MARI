# Speed-Layer-MARI
Tarea 3 Big Data

Se tienen datos de boletas del retail MARI, los cuales se almacenan a una cola local en Kafka.

Se responden las siguientes consultas haciendo procesamiento en tiempo real:
- ¿Existe alguna categoría que venda más?
- ¿Cuál es el promedio por boleta de cada cliente?
- ¿Cuales son los 10 productos más vendidos?
- ¿En qué sucursal se hacen más ventas?

Para esto se hace procesamiento en stream usando Apache Storm y se almacenan las consultas procesadas en Apache Cassandra.
