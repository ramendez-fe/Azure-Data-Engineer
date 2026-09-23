### 1. Mecánica y Costo del Movimiento de Datos (The Shuffle)

* **Anatomía Física del Shuffle:** El movimiento masivo de datos entre particiones y *executors* que requiere serialización, transferencia a través de la red y lectura/escritura intensiva en disco.


* **Operaciones Desencadenantes:** Identificación de las *wide transformations* que obligan a reconstruir las particiones, típicamente funciones como `groupBy()`, `repartition()` y uniones (`join`) no optimizadas.


* **Fases del Proceso:** La división entre *Shuffle Write* (donde las tareas de mapeo agrupan y escriben resultados intermedios en discos locales) y *Shuffle Read* (donde las tareas de reducción solicitan esos bloques por la red).

### 2. Gestión de Particiones: Repartition vs. Coalesce

* **`repartition(n)` y la Redistribución Forzada:** Operación que ejecuta un *shuffle* completo en la red, útil para aumentar el número de particiones o para lograr una distribución de registros matemáticamente uniforme.


* **`coalesce(n)` y el Colapso Local:** Operación optimizada para disminuir el número de particiones fusionándolas dentro del mismo *executor*, evitando por completo el costo de red del *shuffle* masivo.


* **El Problema de los Archivos Pequeños (Small Files Problem):** La necesidad de usar estas operaciones antes de las escrituras en disco para evitar generar miles de archivos de unos pocos kilobytes, lo cual degrada la lectura posterior.

### 3. Estrategias de Joins y Evasión de Shuffles

* **Broadcast Hash Join (BHJ):** Eliminación total del *shuffle* de la tabla de hechos; la estrategia consiste en enviar (mediante la función explícita `broadcast()`) una copia completa de la tabla de dimensión pequeña a todos los *executors*.


* **Sort-Merge Join (SMJ):** El motor por defecto de Spark para tablas masivas. Requiere que los datos de ambas tablas experimenten un *shuffle* para alinearse por la misma llave y luego sean ordenados localmente antes del cruce.
* **Shuffle Hash Join (SHJ):** Estrategia alternativa donde los datos sufren *shuffle* por la llave de join, pero en lugar de ordenarlos, se construye una tabla hash en memoria para la partición más pequeña.

### 4. Identificación y Mitigación del Sesgo de Datos (Data Skew)

* **Diagnóstico del Cuello de Botella:** Ocurre cuando la elección de una llave de distribución agrupa una cantidad desproporcionada de registros en un solo *executor*, ahogando al clúster entero a la espera de esa única partición (el *straggler task*).


* **Técnica de Salting (Inyección de Entropía):** Modificación del diseño lógico agregando una llave aleatoria (un *salt*) a la columna sesgada para fragmentar esa megapartición en trozos más pequeños, forzando una distribución de trabajo equitativa.


* **Lectura del Spark UI:** Técnicas para auditar la varianza extrema en los tiempos de ejecución (*Duration*) y lectura de datos (*Shuffle Read Size*) de los *tasks* individuales dentro del *Stage* de ejecución.

### 5. Persistencia, Caching y el Ciclo de Vida de la Memoria

* **Ruptura del Linaje (Evaluación Perezosa):** El uso de la memorización para obligar a Spark a guardar los resultados parciales tras la primera acción, evadiendo la penalización de recalcular toda la cadena de transformaciones desde el origen en cada nueva consulta.


* **Uso de `cache()`:** Directiva de alto nivel que materializa el DataFrame utilizando invariablemente el nivel de almacenamiento por defecto (típicamente `MEMORY_AND_DISK`).


* **Control Granular con `persist()`:** Directiva de ingeniería que permite seleccionar niveles de almacenamiento específicos a través del objeto `StorageLevel` (ej. forzar solo memoria, forzar solo disco, o almacenar en formato serializado para ahorrar RAM).


* **Liberación de Recursos (`unpersist()`):** La importancia de desalojar explícitamente los datos de la memoria cuando ya no son útiles en el pipeline, antes de que el motor de Evicción (LRU - *Least Recently Used*) de Spark tenga que actuar bajo presión.