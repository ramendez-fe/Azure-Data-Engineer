### 4.1 Orquestación y Movimiento de Datos (ETL/ELT)

* **Azure Data Factory (ADF):** Es el servicio principal de orquestación e integración de datos en Azure, diseñado para extraer, transformar y cargar (ETL/ELT) información moviendo datos entre múltiples orígenes y destinos.


* **Pipelines:** Representan la unidad fundamental de ejecución dentro de ADF. Se definen como una agrupación lógica de actividades que interactúan secuencial o paralelamente para ejecutar una tarea integral, como la copia o transformación de datos.



### 4.2 Infraestructura de Cómputo para Integración

* **Integration Runtime (IR):** Es el motor o infraestructura de cómputo subyacente que Data Factory utiliza para ejecutar los pipelines y el movimiento de datos. Puede ejecutarse en infraestructura aprovisionada por Azure (*Auto-Resolve*).


* **Self-Hosted Integration Runtime:** Es una variante del IR que se instala en máquinas administradas por el cliente. Su función arquitectónica principal es permitir que ADF acceda a fuentes de datos ubicadas en redes privadas *on-premises* o en una Virtual Network (VNet), evadiendo la necesidad de exponer dichos recursos a internet público.



### 4.3 Patrones de Arquitectura, Carga y Modelado

* **Patrón Medallion:** Arquitectura de datos lógica que clasifica la madurez de la información en tres capas: *Bronze* para el almacenamiento de datos crudos, *Silver* para datos que han sido limpios y validados, y *Gold* para datos altamente agregados y listos para el consumo analítico del negocio.


* **Cargas Incrementales (Watermarking):** Estrategia de diseño en ADF para evitar el alto costo de recargar tablas completas. Consiste en almacenar una marca de tiempo (*watermark*, como `LastModifiedDate`) en una tabla de control para filtrar y extraer estrictamente los registros que han sido creados o modificados desde la última ejecución del pipeline.


* **Slowly Changing Dimension (SCD) Tipo 2:** Técnica de modelado en *Data Warehousing* orientada a la preservación del historial. En lugar de sobrescribir el valor de un registro cuando cambia un atributo, crea una nueva fila y gestiona la vigencia temporal de cada versión mediante columnas de rango de fechas (efectivo desde/hasta).



### 4.4 Ingesta en Tiempo Real y Mensajería Empresarial

* **Azure Event Hubs:** Es el motor de *streaming* de eventos de alto rendimiento de la plataforma, optimizado mecánicamente para la ingesta masiva y continua de enormes volúmenes de datos en tiempo real, operando de manera análoga a un clúster de Apache Kafka administrado.


* **Diferenciación con Azure Service Bus:** Mientras que *Event Hubs* absorbe caudales masivos de telemetría, *Service Bus* es un broker de mensajería empresarial transaccional basado en colas y tópicos. Se utiliza para procesar mensajes individuales de alto valor donde se exigen garantías estrictas de entrega, orden y estado.