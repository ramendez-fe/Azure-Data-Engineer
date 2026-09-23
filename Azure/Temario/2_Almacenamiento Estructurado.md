### 2.1 Almacenamiento Analítico Masivo (Data Lakes)

* **Azure Data Lake Storage Gen2 (ADLS Gen2):** La base del almacenamiento para *Big Data* en Azure. Construido sobre la infraestructura de *Blob Storage*, añade un espacio de nombres jerárquico (*hierarchical namespace*) que optimiza el acceso y las operaciones sobre directorios masivos.


* **Niveles de Acceso (Access Tiers) en Blob Storage:** Estrategia de optimización de costos. Los niveles *Cool* (Frecuencia de acceso baja) y *Archive* (Archivo a largo plazo) ofrecen un almacenamiento significativamente más económico para datos históricos o que rara vez se consultan, en contraposición al nivel *Hot*.



### 2.2 Formatos Físicos de Almacenamiento Analítico

* **Almacenamiento Columnar (Parquet vs. CSV):** A diferencia de formatos basados en filas como el CSV, Parquet almacena los datos por columnas, embebiendo metadatos de esquema y aplicando una compresión altamente eficiente.


* **Reducción de I/O (Input/Output):** Esta estructura columnar habilita el *column pruning* (leer exclusivamente las columnas solicitadas en la consulta) y el *predicate pushdown* (empujar los filtros hacia la capa de almacenamiento), reduciendo drásticamente la carga de lectura en operaciones analíticas.



### 2.3 Bases de Datos Relacionales Gestionadas (PaaS)

* **Azure SQL Database (Single Database):** Implementación pura de *Platform as a Service* (PaaS), donde Microsoft administra integralmente el motor de base de datos, la aplicación de parches de seguridad y la gestión de *backups*.


* **Azure SQL Managed Instance:** Diseñada para migraciones *lift-and-shift*. A diferencia de la base de datos única, ofrece una compatibilidad casi total con los motores SQL Server *on-premises*, soportando características *legacy* críticas como SQL Server Agent, consultas entre bases de datos (*cross-database queries*) y CLR.



### 2.4 Alta Disponibilidad y Replicación Relacional

* **Geo-replicación Activa (Active Geo-Replication):** Característica de Azure SQL Database que permite el despliegue de hasta cuatro réplicas legibles de una base de datos distribuidas en distintas regiones globales.


* **Casos de Uso Operativo:** Esta topología no solo actúa como mecanismo de *failover* ante desastres regionales severos, sino que permite enrutar geográficamente el tráfico de solo lectura para minimizar la latencia de los usuarios globales.



### 2.5 Bases de Datos NoSQL y Distribución Global (Cosmos DB)

* **Arquitectura Multi-modelo:** Azure Cosmos DB es un servicio de base de datos NoSQL distribuido globalmente, diseñado para garantizar latencias de lectura/escritura en el orden de los milisegundos de un solo dígito.


* **Espectro de Niveles de Consistencia:** Sustituye la elección binaria tradicional (fuerte vs. eventual) por cinco niveles granulares: *Strong, Bounded Staleness, Session, Consistent Prefix* y *Eventual*. Esto permite a los arquitectos balancear milimétricamente la consistencia de los datos, la disponibilidad y la latencia según el caso de uso.


* **Particionamiento Físico y Request Units (RU):** La elección de la clave de partición (*partition key*) dicta la distribución de los datos en los nodos físicos. Seleccionar una clave de baja cardinalidad o con un acceso muy asimétrico concentra excesivamente las operaciones de lectura/escritura en unas pocas particiones físicas, creando el temido cuello de botella conocido como *hot partition*, el cual degrada severamente el rendimiento general.