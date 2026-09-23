### 1. Arquitectura de Cómputo y Gobierno de Infraestructura

* **Topología de Clústeres (Costo vs. Propósito):**
* *All-Purpose Clusters:* Cómputo persistente para desarrollo interactivo, exploración y análisis *ad-hoc* (mayor costo por DBU).


* *Job Clusters:* Cómputo efímero aprovisionado exclusivamente para la vida útil de una tarea programada (costo optimizado).




* **Motor de Ejecución Vectorizado (Photon):**
* Aceleración a nivel de hardware escrita en C++ para consultas SQL y DataFrames, evadiendo la JVM sin romper la compatibilidad de las APIs de PySpark.




* **Control Administrativo con Cluster Policies:**
* Reglas y plantillas JSON impuestas por la organización para restringir la creación de clústeres, forzar auto-apagado, limitar tamaños de hardware y estandarizar etiquetas para *billing*.





### 2. Orquestación y Automatización (Databricks Workflows)

* **El Orquestador Nativo (Jobs):**
* Definición de *Pipelines* multi-tarea mediante un Grafo Acíclico Dirigido (DAG) interno que gestiona dependencias, paralelismo y políticas de reintento sin requerir herramientas externas como Airflow o Azure Data Factory.




* **Interacción y Contexto del Entorno (`dbutils` y DBFS):**
* Abstracción del almacenamiento subyacente mediante el *Databricks File System* (DBFS).


* Uso de *Databricks Utilities* (`dbutils.fs`, `dbutils.notebook`) para encadenar *notebooks*, pasar parámetros dinámicos (Widgets) e interactuar con el sistema de archivos de forma programática.





### 3. Ingesta Incremental y Procesamiento en Tiempo Real

* **El Motor de Structured Streaming:**
* Tratamiento de flujos de datos como tablas infinitas (*unbounded tables*) evaluadas incrementalmente usando el motor SQL de Spark.


* Implementación de *Checkpoints* para guardar el estado del procesamiento y garantizar tolerancia a fallos.


* Combinación de *Checkpoints* y sumideros (*sinks*) idempotentes (como `MERGE` en Delta) para asegurar una semántica de entrega estricta de "exactamente una vez" (*exactly-once*).




* **Auto Loader (`cloudFiles`):**
* Patrón optimizado para ingesta continua de archivos nuevos (CSV, JSON, Parquet) desde el almacenamiento en la nube, evadiendo el costoso proceso de listar directorios completos iterativamente.


* Inferencia automática y evolución dinámica del esquema ante cambios en la estructura de los datos entrantes (*Schema Evolution*).





### 4. Ingeniería Declarativa y Calidad de Datos (Delta Live Tables - DLT)

* **El Framework DLT:**
* Cambio de paradigma: de escribir flujos imperativos de PySpark a definir declarativamente el estado deseado de un *pipeline* de datos (Vistas Materializadas y Tablas Streaming).


* Gestión automática del linaje, la orquestación de la infraestructura subyacente y el re-procesamiento.


* **Control de Calidad Integrado (Expectations):**
* Definición de reglas de calidad directamente en el código del *pipeline* mediante decoradores (`@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`) para monitorear, purgar o detener el flujo ante registros corruptos.





### 5. Gobierno Global y Seguridad Estructural (Unity Catalog)

* **Metastore Centralizado:**
* Transición desde el esquema local (*Hive Metastore*) a una capa de gobierno unificada a nivel de cuenta, estructurada en 3 niveles lógicos: Catálogo > Esquema > Tabla/Volumen.




* **Control de Acceso (RBAC y ACLs):**
* Aprovisionamiento y revocación de permisos mediante sintaxis estándar ANSI SQL (`GRANT SELECT ON TABLE...`).




* **Seguridad de Grano Fino (Fine-Grained Access Control):**
* *Row filters:* Funciones SQL aplicadas dinámicamente en tiempo de ejecución para ocultar filas enteras según la identidad o el grupo del usuario que consulta.


* *Column masking:* Funciones de enmascaramiento para ofuscar (ej. redacción parcial de un DNI o tarjeta) el contenido de columnas sensibles sin alterar el dato físico subyacente.




* **Auditoría y Trazabilidad:**
* Captura y visualización automática del linaje de datos de extremo a extremo, mapeando dependencias tanto a nivel de tabla como de columna.



### 6. Gestión de Secretos e Integración Cloud

* **Arquitectura de Seguridad de Credenciales:**
* Integración nativa con Azure Key Vault mediante *Secret Scopes* para desterrar tokens, contraseñas o *Connection Strings* del código fuente.




* **Recuperación en Tiempo de Ejecución:**
* Extracción segura de secretos a través del plano de control de Databricks utilizando la librería utilitaria (`dbutils.secrets.get()`).