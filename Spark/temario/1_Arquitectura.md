### 1. Arquitectura Topológica y Computacional del Clúster

* **Modelo Master-Worker de Spark**
* El Driver Program: SparkContext/SparkSession y el DAGScheduler.
* El Cluster Manager: YARN, Kubernetes, Mesos y Standalone.
* Los Executors: JVMs de trabajo, hilos (cores), ranuras (slots) y gestión de memoria local.


* **La Unificación del Punto de Entrada**
* Transición histórica: De `SparkContext` + `SQLContext` a `SparkSession`.


* Anatomía de una Aplicación: Jobs, Stages y Tasks.


* **Particionamiento Físico**
* Bloques HDFS/Cloud Storage vs. Particiones en Memoria.
* Mapeo de tareas a cores físicos.



### 2. Estructuras de Datos Computacionales: De RDDs a DataFrames

* **Resilient Distributed Datasets (RDDs)**
* Características fundacionales: Inmutabilidad, particionamiento y tolerancia a fallos.
* Concepto de Linaje (Lineage) y grafos de dependencia.
* Opacidad semántica: Por qué el optimizador no entiende los tipos de datos ni el código interno de un RDD.




* **Evolución hacia la Abstracción Estructurada**
* DataFrames y Datasets: Esquemas explícitos (StructType/StructField) y tipado columnar.




* **El Motor Tungsten**
* Gestión de memoria explícita (*off-heap memory management*) para evadir el *Garbage Collector* de Java.
* Formatos de datos binarios y empaquetado en memoria.



### 3. Evaluación Perezosa (Lazy Evaluation) y el Grafo de Ejecución

* **Taxonomía de las Operaciones**
* Transformaciones Estrechas (*Narrow Transformations*): Dependencia 1:1, ejecución *in-memory* (`map`, `filter`, `select`).


* Transformaciones Anchas (*Wide Transformations*): Dependencias N:N y fronteras de etapa (*Stage boundaries*) (`groupBy`, `join`, `repartition`).


* Acciones (*Actions*): Gatillos de materialización física (`collect`, `show`, `count`, `write`).




* **El Grafo Acíclico Dirigido (DAG)**
* Construcción lógica vs. ejecución física secuencial.
* Tolerancia a fallos: Reconstrucción determinista de particiones perdidas desde el linaje en lugar de replicación de datos en memoria.



### 4. El Optimizador Catalyst: Ciclo de Vida de una Consulta

* **Fase 1: Análisis (Analysis)**
* Conversión de código (SQL/DataFrame) a Plan Lógico Sin Resolver (*Unresolved Logical Plan*).
* Resolución contra el Catálogo (*Catalog/Metastore*) para validar columnas y tablas.


* **Fase 2: Optimización Lógica (*Logical Optimization*)**
* Aplicación heurística basada en reglas.
* *Predicate Pushdown*: Bajar filtros a la capa de almacenamiento.


* *Column Pruning*: Descartar columnas no utilizadas tempranamente.


* *Constant Folding*: Evaluación anticipada de expresiones estáticas.


* **Fase 3: Planificación Física (*Physical Planning*)**
* Traducción a múltiples planes físicos posibles.
* Optimización basada en costos (Cost-Based Optimizer - CBO): Elección del plan óptimo (ej. evaluar tamaños de tablas para elegir estrategias de Join).




* **Fase 4: Generación de Código (Whole-Stage Code Generation)**
* Fusión de operadores (*operator fusion*) y compilación del plan físico en *bytecode* de Java optimizado en tiempo de ejecución.



### 5. Extensibilidad, Serialización y Cuellos de Botella

* **Expresiones Nativas vs. Código de Usuario**
* Por qué usar `pyspark.sql.functions`: Optimización de Catalyst y ejecución directa en la JVM sin sobrecarga.




* **El Problema Estructural de las UDFs (User Defined Functions) en PySpark**
* Caja negra para Catalyst: Imposibilidad de optimización lógica.


* Overhead de serialización: Empaquetado (Pickle), transferencia de datos vía *sockets* entre la JVM y el proceso Python, y vuelta a la JVM.




* **Mitigación: Pandas UDFs (Vectorized UDFs)**
* Uso de Apache Arrow para transferencia de datos tabulares (*columnar memory format*) sin costo de serialización fila por fila.



### 6. Ejecución Adaptativa de Consultas (Adaptive Query Execution - AQE)

* **Límites de la Planificación Estática**
* El problema de las estadísticas desactualizadas o inexistentes previas a la ejecución física.




* **Re-optimización en Tiempo de Ejecución (*Runtime*)**
* Recolección de métricas reales al finalizar etapas de *Shuffle* (*Stage boundaries*).


* **Mecanismos Clave de AQE**
* *Dynamically Coalescing Shuffle Partitions*: Fusión automática de particiones pequeñas resultantes de un *shuffle* excesivo.


* *Dynamically Switching Join Strategies*: Cambio de un lento *Sort-Merge Join* a un *Broadcast Join* si una tabla se redujo masivamente tras un filtro aplicado en memoria.


* *Dynamically Optimizing Skew Joins*: Detección y subdivisión automática de particiones sesgadas (*data skew*) durante cruces de datos.

