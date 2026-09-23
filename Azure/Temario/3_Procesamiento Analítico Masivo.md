### 3.1 Arquitectura de Cómputo Analítico

* **Dedicated SQL Pool:** Un almacén de datos relacional de procesamiento masivamente paralelo (MPP) estructurado específicamente para ejecutar analítica sobre grandes volúmenes de datos. Exige el aprovisionamiento previo y el pago por capacidad de cómputo reservada.


* **Serverless SQL Pool:** Un modelo de ejecución sin servidor donde no se aprovisiona infraestructura reservada. El cobro se realiza de forma estricta por consulta ejecutada (pago por uso).



### 3.2 Estrategias de Distribución Física de Datos

* **Distribución Hash (Hash Distribution):** Método que reparte las filas de una tabla entre los nodos de cómputo aplicando una función matemática (*hash*) sobre una columna específica. Es la arquitectura recomendada para tablas de hechos masivas, ya que su objetivo estructural es minimizar el movimiento de datos (*data shuffling*) durante la resolución de *joins*.


* **Distribución Replicada (Replicated Distribution):** Estrategia que almacena una copia íntegra de la tabla en cada nodo de cómputo del clúster. Se emplea en tablas de dimensión relativamente pequeñas que participan constantemente en *joins*, evadiendo por completo la transferencia de datos por la red.



### 3.3 Particionamiento Lógico e Indexación Columnar

* **Clustered Columnstore Index:** Es la estructura de indexación predeterminada y recomendada en Synapse Dedicated SQL Pools para cargas de trabajo analíticas. Físicamente almacena y comprime la información por columnas en lugar de por filas, lo que dispara el rendimiento y maximiza la compresión al agregar volúmenes masivos de datos.


* **Particionamiento de Tablas (Partitioning):** La división de una tabla grande en segmentos lógicos aislados basados en una columna directriz (típicamente la fecha). Acelera las consultas al forzar al motor a escanear exclusivamente las particiones necesarias y omitir la lectura de toda la tabla.



### 3.4 Optimización de Joins y Diagnóstico de Cuellos de Botella

* **Broadcast Join:** Estrategia de optimización distribuida empleada al cruzar una tabla masiva de hechos contra una tabla pequeña de dimensión. Consiste en transmitir una réplica completa de la tabla pequeña hacia todos los nodos de cómputo, evitando el altísimo costo de red (shuffle) que implicaría redistribuir ambas tablas.


* **Sesgo de Datos (Data Skew):** Un escenario de degradación del rendimiento que ocurre cuando las filas se distribuyen de manera desigual entre los nodos de cómputo, comúnmente debido a la selección de una clave de distribución defectuosa. Provoca que uno o varios nodos ejecuten mucho más trabajo que el resto, convirtiéndose en el ancla térmica del clúster.



### 3.5 Interoperabilidad con el Data Lake

* **PolyBase y Tablas Externas (External Tables):** Mecanismo de virtualización que permite consultar y extraer datos directamente desde archivos físicos almacenados en ADLS Gen2 o Blob Storage. Se opera mediante sintaxis T-SQL estándar sin la necesidad de ingerir o mover previamente la data hacia una tabla relacional interna.