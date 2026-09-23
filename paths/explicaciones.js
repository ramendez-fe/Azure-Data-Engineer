// Explicaciones teóricas del Balotario Databricks & PySpark
// Cada clave corresponde al "id" de la pregunta en el archivo principal.
// Este archivo se carga como <script> ANTES del script principal del quiz.

const EXPLANATIONS = {

  // ===================== NIVEL BÁSICO =====================
  "b1": "Desde Spark 2.0, SparkSession unifica lo que antes eran objetos separados (SparkContext, SQLContext, HiveContext) en un único punto de entrada. A través de él se crean DataFrames, se ejecutan consultas SQL, se configura el runtime y se accede a los catálogos. En Databricks normalmente ya viene creado como la variable `spark`.",

  "b2": "Spark distingue entre transformaciones (lazy: select, filter, withColumn, drop, etc., que solo construyen un plan lógico) y acciones (que disparan la ejecución real, como collect, count, show o write). Nada se calcula hasta que se invoca una acción; esto permite que el optimizador Catalyst vea el plan completo antes de ejecutarlo.",

  "b3": "El patrón general para leer datos en PySpark es `spark.read.<formato>(ruta)`, ya sea con el método específico (`.csv()`, `.json()`, `.parquet()`) o de forma genérica con `.format(\"csv\").load(ruta)`. `spark.read` devuelve un DataFrameReader que permite encadenar opciones como `header`, `inferSchema` o `sep` antes de materializar el DataFrame.",

  "b4": "`printSchema()` imprime el árbol de columnas con su tipo de dato y si admiten nulos (nullable). A diferencia de `count()` o `show()`, normalmente no necesita ejecutar un job sobre los datos completos, porque el esquema suele obtenerse de los metadatos del archivo (o de una muestra, si se usa inferSchema en CSV).",

  "b5": "`select()` es una operación de proyección: elige o transforma columnas, pero conserva todas las filas. `filter()` (equivalente a `where()`) es una operación de selección: conserva todas las columnas pero descarta filas según una condición booleana. Ambas son transformaciones lazy y suelen combinarse en la misma consulta.",

  "b6": "`show()` es una acción: fuerza la ejecución del plan acumulado hasta ese punto y muestra por consola un número limitado de filas (20 por defecto), truncando strings largos a 20 caracteres salvo que se indique lo contrario. No devuelve un DataFrame nuevo ni es apta para pipelines, solo para inspección.",

  "b7": "Un objeto `Column` (como el que devuelve `col(\"monto\")` o `df.monto`) no contiene datos: es una representación simbólica de una expresión dentro del plan lógico. Sirve para construir condiciones y transformaciones (`col(\"a\") + col(\"b\")`, `col(\"x\") > 10`) que Catalyst analiza y optimiza antes de tocar los datos reales.",

  "b8": "`withColumn(nombre, expresión)` agrega una columna nueva o, si el nombre ya existe, la reemplaza aplicando la expresión dada. Como los DataFrames son inmutables, el método no modifica el original: devuelve un DataFrame nuevo con la columna añadida/actualizada, y hay que reasignarlo (`df = df.withColumn(...)`).",

  "b9": "Los notebooks de Databricks son multilenguaje: cada celda puede tener un lenguaje distinto usando un 'magic command' al inicio (`%python`, `%sql`, `%scala`, `%r`, `%md` para markdown). `%sql` permite escribir consultas SQL directas contra las tablas/vistas registradas en el metastore o Unity Catalog sin salir del notebook.",

  "b10": "DBFS es una capa de abstracción que monta almacenamiento en la nube (S3, ADLS, GCS) como si fuera un sistema de archivos accesible desde el workspace con rutas tipo `/dbfs/...` o `dbfs:/...`. Facilita leer/escribir archivos sin gestionar credenciales de la nube en cada operación, aunque hoy Databricks recomienda Unity Catalog Volumes para nuevos desarrollos.",

  "b11": "Databricks ofrece All-Purpose clusters (interactivos, compartidos), Job clusters (efímeros, creados solo para ejecutar un job), SQL Warehouses (cómputo optimizado para SQL/BI) y la opción de habilitar Photon sobre cualquiera de ellos. 'Streaming-Only Cluster' no es un tipo de cómputo que exista: el streaming corre sobre cualquiera de los tipos anteriores.",

  "b12": "`join()` combina dos DataFrames relacionando filas según una condición (usualmente igualdad de una o más columnas), replicando el álgebra relacional de un JOIN en SQL. `union()`/`unionByName()` en cambio apilan filas de DataFrames con esquemas compatibles, sin relacionar columnas entre sí.",

  "b13": "El parámetro `how` define la estrategia del join: `inner` (solo coincidencias), `left`/`left_outer` (todo lo de la izquierda + coincidencias), `right_outer`, `outer`/`full` (todo de ambos lados), `left_semi` (filas de la izquierda que tienen coincidencia, sin traer columnas de la derecha) y `left_anti` (filas de la izquierda sin coincidencia).",

  "b14": "`count()` es una acción que recorre el DataFrame y devuelve el número total de filas como un entero de Python. Al ser una acción, dispara un job de Spark; sobre datasets grandes puede ser costosa porque requiere materializar/contar todas las particiones.",

  "b15": "Un notebook es la unidad de trabajo interactiva en Databricks: combina celdas de código (en distintos lenguajes), texto en markdown y las salidas/visualizaciones generadas, todo ejecutado contra un cluster o SQL warehouse asociado. Es el equivalente evolucionado de un script, pero con ejecución incremental celda por celda.",

  "b16": "Si no se especifica `.format(...)`, `DataFrameWriter` usa el valor de la configuración `spark.sql.sources.default`, que en Spark (y por defecto en Databricks) es Parquet. Parquet es un formato columnar comprimido, eficiente para lectura analítica, y es además la base sobre la que se construye Delta Lake.",

  "b17": "`drop(\"col1\", \"col2\", ...)` devuelve un nuevo DataFrame sin las columnas indicadas. Es una transformación lazy; si la columna no existe, Spark simplemente la ignora (no lanza error), a diferencia de intentar seleccionar una columna inexistente con `select()`.",

  "b18": "`groupBy(\"col\")` agrupa las filas por los valores distintos de esa columna, produciendo un `GroupedData` sobre el que se aplican funciones de agregación como `sum`, `avg`, `count`, `max`. `.agg(sum(\"monto\"))` calcula la suma de `monto` dentro de cada grupo, devolviendo una fila por valor distinto de `col`.",

  "b19": "`dbutils` es un conjunto de utilidades exclusivas del entorno Databricks (no es parte de PySpark/Spark core). Incluye `dbutils.fs` (operaciones de archivos sobre DBFS/almacenamiento en la nube), `dbutils.secrets` (acceso a secretos gestionados), `dbutils.widgets` (parámetros interactivos en el notebook) y `dbutils.notebook` (encadenar/ejecutar otros notebooks).",

  "b20": "`%pip install paquete` instala una librería Python a nivel de sesión del notebook (no afecta a todo el cluster de forma permanente ni a otros usuarios). Es la forma recomendada para dependencias ad-hoc, en contraste con instalar librerías a nivel de cluster desde la UI, que sí persisten entre reinicios y afectan a todos los notebooks adjuntos.",

  // ===================== NIVEL INTERMEDIO =====================
  "i1": "Una UDF (User Defined Function) permite ejecutar lógica Python arbitraria fila por fila. El problema es que Catalyst no puede 'ver dentro' de esa función: la trata como una caja negra que no puede reordenar, empujar (pushdown) ni optimizar junto al resto del plan. Además, en UDFs Python clásicas los datos cruzan la frontera JVM↔Python (vía Py4J o Arrow), lo que añade overhead de serialización comparado con ejecutar todo dentro de la JVM.",

  "i2": "Las funciones de `pyspark.sql.functions` (como `when`, `concat`, `date_add`) son expresiones nativas de Catalyst que se ejecutan directamente en la JVM, sin serializar datos hacia Python. Esto permite que el optimizador las combine, reordene y empuje hacia el origen de datos (pushdown), logrando planes de ejecución mucho más eficientes que una UDF equivalente.",

  "i3": "`cache()`/`persist()` no ejecutan nada por sí mismos (siguen siendo lazy): marcan el DataFrame para que, en la primera acción que lo materialice, el resultado se guarde en memoria y/o disco según el StorageLevel elegido. Las siguientes acciones reutilizan ese resultado en vez de recalcular toda la cadena de transformaciones previas (el lineage), lo cual es útil cuando un mismo DataFrame se reutiliza varias veces.",

  "i4": "`cache()` es en realidad un atajo equivalente a `persist(StorageLevel.MEMORY_AND_DISK)` (para DataFrames). `persist()` permite elegir explícitamente otros niveles: solo memoria, solo disco, con o sin serialización, con o sin replicación, según el balance de velocidad vs. uso de recursos que se necesite.",

  "i5": "En un broadcast join, la tabla más pequeña se copia completa (se 'broadcastea') a la memoria de cada executor, de modo que el join se resuelve localmente sin necesidad de mover ni reparticionar la tabla grande por la red (shuffle). Es ideal cuando una de las tablas cabe cómodamente en memoria; Spark lo aplica automáticamente si el tamaño está por debajo del umbral `spark.sql.autoBroadcastJoinThreshold`.",

  "i6": "La función `broadcast(df)` de `pyspark.sql.functions` marca explícitamente un DataFrame para que Spark intente un broadcast join, sin importar el umbral de autodetección por tamaño. Es útil cuando Spark no puede estimar bien el tamaño real de la tabla (por ejemplo, tras varias transformaciones) y no lo haría automáticamente.",

  "i7": "AQE (Adaptive Query Execution) re-optimiza el plan de ejecución en tiempo real usando estadísticas reales generadas después de un shuffle (no solo estimaciones previas). Con AQE, Spark puede cambiar dinámicamente la estrategia de join, coalescer particiones post-shuffle que quedaron muy pequeñas, y dividir particiones con 'data skew' automáticamente.",

  "i8": "`CONVERT TO DELTA parquet.\`ruta\`` toma una tabla Parquet existente y la convierte en Delta 'in place': genera el `_delta_log` inicial describiendo los archivos Parquet ya existentes como la primera versión de la tabla, sin necesidad de reescribir los datos.",

  "i9": "La carpeta `_delta_log/` contiene el registro transaccional (transaction log) de la tabla: una secuencia de archivos JSON (y checkpoints Parquet periódicos) que describen cada commit (qué archivos se agregaron o removieron). Este log es la fuente de verdad que permite las garantías ACID, el time travel y la reconstrucción del estado de la tabla en cualquier versión.",

  "i10": "`SELECT * FROM tabla VERSION AS OF 5` (o su equivalente `TIMESTAMP AS OF`) es la sintaxis de 'time travel' de Delta Lake: le pide al motor que reconstruya el estado de la tabla usando el `_delta_log` hasta esa versión específica, en lugar de la versión más reciente.",

  "i11": "`OPTIMIZE` reescribe (compacta) muchos archivos pequeños en archivos más grandes y de tamaño más uniforme (bin-packing), lo que reduce la sobrecarga de abrir/leer miles de archivos pequeños en consultas posteriores. Opcionalmente puede combinarse con `ZORDER BY` para además co-localizar valores relacionados.",

  "i12": "`VACUUM` elimina físicamente del almacenamiento los archivos de datos que ya no forman parte de ninguna versión activa de la tabla dentro de la ventana de retención (por defecto 7 días). Esto libera espacio, pero también rompe el time travel hacia versiones cuyos archivos ya fueron eliminados.",

  "i13": "Delta Lake no inventa un formato de datos nuevo: almacena los datos como archivos Parquet estándar, y añade por encima el `_delta_log` (transacciones) para aportar ACID, versionado, schema enforcement/evolution y otras garantías que Parquet por sí solo no ofrece.",

  "i14": "`partitionBy(\"col\")` hace que, al escribir, Spark cree una jerarquía de subcarpetas físicas según los valores distintos de esa columna (por ejemplo `fecha=2024-01-01/`). Esto permite que consultas que filtran por esa columna evitar leer particiones irrelevantes (partition pruning), aunque un exceso de particiones muy pequeñas puede generar el problema de 'small files'.",

  "i15": "Structured Streaming reutiliza el mismo motor y la misma API de DataFrames de Spark SQL, pero interpretando el stream de entrada como una tabla que crece de forma indefinida (unbounded table). Cada nuevo dato que llega se procesa como si fueran filas nuevas insertadas en esa tabla, y las mismas transformaciones (select, groupBy, join) usadas en batch aplican también en streaming.",

  "i16": "`checkpointLocation` le indica a Structured Streaming dónde guardar los offsets procesados y el estado del stream. Es lo que permite que, si el job falla y se reinicia, retome exactamente donde se quedó en vez de reprocesar todo desde el inicio o perder datos; sin checkpoint no hay garantías reales de tolerancia a fallos.",

  "i17": "Auto Loader (`cloudFiles`) está diseñado para ingerir de forma incremental y eficiente archivos nuevos que van llegando a un directorio en la nube, sin tener que listar todo el directorio en cada ejecución (usa notificaciones de eventos o listado incremental optimizado). Además soporta inferencia de esquema y evolución automática cuando aparecen columnas nuevas.",

  "i18": "Para usar Auto Loader en Structured Streaming se especifica `.format(\"cloudFiles\")` junto con la opción `cloudFiles.format` indicando el formato real de los archivos de origen (csv, json, parquet, etc.). 'cloudFiles' es el nombre de la fuente de streaming, no del formato del archivo en sí.",

  "i19": "Unity Catalog añade un nivel de gobierno centralizado a nivel de cuenta (no solo de workspace), organizando los objetos en una jerarquía de tres niveles: catálogo → esquema → tabla. Desde ahí se gestionan permisos granulares, linaje de datos (qué tabla alimenta a cuál), auditoría de accesos y descubrimiento de datos de forma unificada entre todos los workspaces de la cuenta.",

  "i20": "`GRANT SELECT ON TABLE tabla TO grupo` sigue la sintaxis estándar de control de acceso (DCL) de SQL, adaptada por Unity Catalog para sus objetos gobernados (catálogos, esquemas, tablas, vistas, funciones). Existen privilegios análogos para otras acciones: `MODIFY`, `CREATE`, `USE CATALOG`, etc.",

  // ===================== NIVEL AVANZADO =====================
  "a1": "El shuffle ocurre cuando Spark necesita reorganizar los datos de forma que filas relacionadas terminen en la misma partición/executor, lo cual implica escribir a disco y transferir por red. Sucede en transformaciones 'anchas' (wide): `groupBy`/`agg`, `join` sin broadcast, `repartition`, `distinct` y `orderBy`, a diferencia de transformaciones 'angostas' (narrow) como `select` o `filter` que no requieren mover datos entre particiones.",

  "a2": "`repartition(n)` siempre hace un shuffle completo (todos los datos se redistribuyen), lo que permite tanto aumentar como disminuir el número de particiones y equilibrar mejor su tamaño. `coalesce(n)` solo puede reducir particiones y lo hace fusionando particiones existentes en el mismo nodo cuando es posible, evitando un shuffle completo — por eso es más barato, pero puede generar particiones desbalanceadas.",

  "a3": "El 'salting' combate el data skew agregando un componente aleatorio (una 'sal', por ejemplo un número del 0 al N) a la llave sesgada antes de agrupar/unir, de modo que los valores que antes caían todos en una sola partición se reparten en varias. Luego suele requerirse un segundo paso de agregación para combinar los resultados parciales por la llave original.",

  "a4": "`df.explain()` muestra el plan de ejecución que Catalyst generó: por defecto el plan físico, y con `explain(True)` o `explain(\"extended\")` también los planes lógico (parseado, analizado, optimizado) y físico. Es la herramienta principal para entender si Spark hará broadcast o shuffle join, si aplicará pushdown de filtros, etc., antes de ejecutar el job.",

  "a5": "Los join hints (`/*+ BROADCAST(t) */`, `/*+ MERGE(t) */`, `/*+ SHUFFLE_HASH(t) */`, etc.) son directivas dentro de la consulta SQL que sugieren al optimizador qué estrategia física de join usar. Catalyst y AQE normalmente respetan el hint si es viable, pero conservan la decisión final del motor cuando la estrategia sugerida no es aplicable (por ejemplo, si la tabla es demasiado grande para broadcast).",

  "a6": "Z-Ordering reorganiza físicamente los archivos de una tabla Delta de forma que los valores de una o más columnas queden agrupados dentro de los mismos archivos (usando una curva de orden multidimensional). Esto mejora el 'data skipping': al filtrar por esas columnas, Spark puede descartar archivos completos basándose en sus estadísticas min/max sin ni siquiera abrirlos.",

  "a7": "Delta Live Tables es un framework declarativo: en vez de escribir la orquestación paso a paso, se definen las tablas de destino (con SQL o PySpark) y sus dependencias, y DLT se encarga de calcular el orden de ejecución, gestionar el pipeline (batch o streaming), monitorear la calidad de datos y manejar reintentos/errores automáticamente.",

  "a8": "Los decoradores `@dlt.expect(\"nombre\", \"condición\")`, `@dlt.expect_or_drop(...)` y `@dlt.expect_or_fail(...)` definen expectativas de calidad de datos sobre una tabla DLT. Difieren en qué pasa cuando una fila no cumple la condición: solo se registra la métrica, se descarta la fila, o se detiene el pipeline con error, respectivamente.",

  "a9": "Para CDC hacia Delta se usa `MERGE INTO destino USING origen ON destino.id = origen.id WHEN MATCHED ... WHEN NOT MATCHED ...`, comparando por una llave primaria y normalmente un indicador de operación (insert/update/delete). Al basarse en la llave, reprocesar el mismo lote de cambios no duplica filas (operación idempotente), lo cual es clave para pipelines de CDC confiables.",

  "a10": "El checkpoint de Structured Streaming garantiza que la lectura no pierda ni reprocese offsets 'a ciegas', pero si el mismo micro-batch se reintenta tras un fallo, el sink podría recibir esos datos dos veces. Para lograr exactly-once de extremo a extremo se necesita además que el sink sea idempotente ante reprocesamiento — por ejemplo, un MERGE en Delta usando el ID del batch o una llave de negocio para no insertar duplicados.",

  "a11": "Photon es un motor de ejecución reescrito en C++ (no Python ni Scala/JVM puro) que reemplaza partes del motor de ejecución de Spark con operadores vectorizados de bajo nivel, manteniendo compatibilidad total con las APIs de DataFrame y SQL existentes. Acelera especialmente consultas de agregación, joins y escaneos intensivos en I/O.",

  "a12": "Las cluster policies son plantillas de configuración que un administrador define para limitar qué tipos de instancia, tamaños de cluster, tiempos de auto-terminación, etc. pueden usar los usuarios al crear clusters. Sirven para evitar configuraciones costosas o inseguras y estandarizar el uso de cómputo en toda la organización.",

  "a13": "Un job cluster se crea automáticamente al lanzar un job programado y se termina en cuanto el job finaliza, por lo que solo se paga por el tiempo real de ejecución — es la opción recomendada para cargas de trabajo productivas y automatizadas. Un all-purpose cluster permanece activo para uso interactivo compartido entre varios notebooks/usuarios, y sigue facturando mientras esté encendido aunque nadie lo use en ese momento.",

  "a14": "Liquid clustering es una alternativa más moderna al particionamiento tradicional (carpetas fijas) y a Z-Order manual: permite definir columnas de clustering (`CLUSTER BY`) sin comprometerse a una estructura de carpetas rígida, y `OPTIMIZE` puede reclusterizar incrementalmente solo los datos nuevos/modificados, sin tener que reescribir toda la tabla cada vez.",

  "a15": "El column masking de Unity Catalog aplica una función definida por el administrador que transforma u oculta el valor real de una columna (por ejemplo, mostrando solo los últimos 4 dígitos de una tarjeta) dependiendo del rol o grupo del usuario que consulta, todo sobre la misma tabla física, sin necesidad de crear copias o vistas separadas por rol.",

  "a16": "Los row filters funcionan de forma similar al column masking pero a nivel de fila: se define una función que, evaluada en tiempo de consulta según el usuario/grupo, determina qué filas son visibles. Así, dos usuarios distintos pueden consultar la misma tabla física y ver subconjuntos diferentes de filas (por ejemplo, cada vendedor solo ve las filas de su propia región).",

  "a17": "El problema de 'small files' ocurre típicamente cuando un job escribe con demasiadas particiones de salida (cada partición genera al menos un archivo). La combinación recomendada es controlar el paralelismo de escritura con `repartition()`/`coalesce()` antes de guardar, y complementarlo con ejecuciones periódicas de `OPTIMIZE` que compactan lo que ya quedó fragmentado en la tabla Delta con el tiempo.",

  "a18": "Predicate pushdown consiste en trasladar los filtros de la consulta (los `WHERE`) hacia la capa de lectura de almacenamiento, de modo que se descarten bloques, row groups o archivos completos antes de traerlos a memoria, usando las estadísticas (min/max, por ejemplo) que Parquet y Delta guardan por archivo/columna. Esto reduce drásticamente el I/O comparado con leer todo y filtrar después en memoria.",

  "a19": "Databricks Workflows (Jobs) es el orquestador nativo de la plataforma: permite encadenar múltiples tareas (notebooks, scripts, pipelines DLT, SQL) definiendo dependencias entre ellas, políticas de reintento ante fallos, y programación por horario o disparadores (triggers), todo sin salir del ecosistema Databricks ni depender de un orquestador externo como Airflow.",

  "a20": "Cuando el particionamiento tradicional por una sola columna (por ejemplo fecha) ya no basta porque además se filtra frecuentemente por otra columna de alta cardinalidad, liquid clustering permite migrar con `ALTER TABLE ... CLUSTER BY (fecha, columna_cardinalidad)` y luego ejecutar `OPTIMIZE`, dejando que Delta reorganice los datos de forma incremental según los patrones reales de consulta, algo que el particionamiento rígido por carpetas no permite hacer sin reescribir toda la tabla."
};
