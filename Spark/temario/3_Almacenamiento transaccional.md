### 1. Arquitectura Core y el Registro de Transacciones (Transaction Log)

* **Paradigma del Almacenamiento Dual:** La separación física entre los archivos de datos (Parquet columnar inmutable) y el registro de metadatos (`_delta_log`).


* **Anatomía del `_delta_log`:**
* Archivos JSON secuenciales (`00000.json`, `00001.json`) como registro de *commits*.
* Archivos *Checkpoint* en formato Parquet (instantáneas del estado para evitar leer miles de JSONs).


* **Garantías ACID sobre Almacenamiento de Objetos (S3, ADLS Gen2):**
* Atomicidad y Durabilidad: Una escritura falla completamente o tiene éxito completamente.
* Control de Concurrencia Optimista (*Optimistic Concurrency Control - OCC*): Cómo Delta Lake resuelve colisiones cuando múltiples usuarios o *jobs* intentan escribir en la misma tabla simultáneamente.



### 2. Operaciones Mutables y Soporte DML (Data Manipulation Language)

* **Ruptura del Paradigma *Append-Only*:** Capacidad de ejecutar `UPDATE`, `DELETE` y `MERGE INTO` nativamente sobre un *Data Lake*.


* **El Patrón de Diseño MERGE (Upsert):**
* Lógica condicional transaccional: `WHEN MATCHED` (actualizar) vs. `WHEN NOT MATCHED` (insertar).


* Implementación de *Change Data Capture* (CDC) e idempotencia para reprocesamientos sin duplicidad de datos.


* Aplicación en dimensiones lentamente cambiantes (*Slowly Changing Dimensions - SCD Tipo 2*) preservando fechas de vigencia.





### 3. Versionado y Auditoría (Time Travel)

* **Lectura de Snapshots Históricos:**
* Sintaxis SQL (`VERSION AS OF`, `TIMESTAMP AS OF`) para acceder a estados pasados de la tabla exactamente como estaban en un momento dado.




* **Recuperación ante Desastres (Rollback):** Uso del comando `RESTORE` para revertir una tabla entera a una versión anterior tras una escritura errónea o corrupción lógica.
* **Auditoría Interna:** Inspección del historial de la tabla (`DESCRIBE HISTORY`) para rastrear qué usuario, *job* o servicio realizó cada operación y métricas de afectación de archivos.

### 4. Gobernanza del Esquema (Schema Management)

* **Schema Enforcement (Validación Estricta):** Rechazo automático de escrituras que no coinciden con la estructura de la tabla destino, previniendo la corrupción silenciosa del *Data Lake*.
* **Schema Evolution (Evolución Dinámica):** Mecanismo explícito (ej. `mergeSchema=true`) para permitir que operaciones de escritura añadan nuevas columnas de forma segura al metadato sin romper los pipelines existentes.

### 5. Optimización Física y Rendimiento de Lectura

* **Compactación de Archivos (El Comando `OPTIMIZE`):**
* Solución al problema crónico de los "archivos pequeños" (*small files problem*) generado por ingestas de *streaming* o *micro-batching*.


* Reescritura de múltiples archivos pequeños Parquet en archivos más grandes y eficientes para el *I/O*.




* **Estrategias de Co-localización de Datos:**
* *Particionamiento Tradicional:* División física por carpetas (útil solo para columnas de baja cardinalidad como fecha o país).
* *Z-Ordering (Curvas de Llenado del Espacio):* Técnica de clúster multidimensional que coloca valores relacionados físicamente juntos para potenciar el *Data Skipping* (descarte masivo de archivos antes de leerlos).


* *Liquid Clustering:* El reemplazo moderno a Z-Order y particionamiento que flexibiliza la reclusterización incremental sin requerir reescrituras masivas ni elecciones de partición rígidas.





### 6. Mantenimiento del Ciclo de Vida y Limpieza (Data Retention)

* **El Comando `VACUUM`:**
* Mecánica de recolección de basura (*Garbage Collection*) para purgar físicamente los archivos Parquet que ya no son referenciados por la última versión del log y que han superado el umbral de retención (*Retention Threshold*).




* **El Balance del Time Travel:** Cómo la ejecución de `VACUUM` rompe deliberadamente la capacidad de hacer *Time Travel* hacia versiones más antiguas que el umbral configurado (por defecto, 7 días) a cambio de ahorrar costos de almacenamiento.