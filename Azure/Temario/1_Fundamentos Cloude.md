### 1.1 Arquitectura Lógica y Organización de Recursos

* **Resource Groups (Grupos de Recursos):** El contenedor fundamental en Azure. Actúa como un límite lógico que agrupa recursos relacionados (bases de datos, VNets, Data Factories) para administrar su ciclo de vida, monitorearlos y facturarlos como una unidad indivisible.


* **Aislamiento y Gobernanza:** Es la capa base sobre la cual se aplican políticas (Azure Policy) y se otorgan permisos de acceso (RBAC) para limitar el alcance de las identidades.

### 1.2 Modelos de Servicio y Gobernanza en la Nube

* **Taxonomía IaaS, PaaS y SaaS:** El nivel de abstracción y control. IaaS entrega infraestructura donde el cliente administra el sistema operativo; PaaS abstrae la infraestructura delegando parches y backups a Azure (ej. Azure SQL Database); SaaS entrega el producto final listo para usar.


* **Modelo de Responsabilidad Compartida (Shared Responsibility Model):** El marco de cumplimiento que define qué gestiona Microsoft (seguridad física del centro de datos) y qué gestiona el cliente (datos, endpoints, identidades), variando estrictamente según el modelo de servicio contratado (IaaS vs PaaS vs SaaS).



### 1.3 Resiliencia Física y Escalabilidad

* **Availability Zones (Zonas de Disponibilidad):** La protección contra desastres a nivel de centro de datos. Son ubicaciones físicas separadas dentro de una misma región de Azure, equipadas con energía, refrigeración y red independientes para garantizar la tolerancia a fallos.


* **Dinámica de Escalado (Scale Up):** El escalado vertical. Consiste en aumentar la capacidad de cómputo (CPU, memoria) de una instancia de recurso existente, a diferencia del escalado horizontal (scale out) que añade más instancias idénticas.



### 1.4 Continuidad de Negocio (Disaster Recovery)

* **Recovery Time Objective (RTO):** El límite de tiempo máximo aceptable que un servicio puede estar inactivo antes de ser restaurado tras una falla. Mide la velocidad de recuperación.


* **Recovery Point Objective (RPO):** La cantidad máxima aceptable de datos que la organización está dispuesta a perder, medida en tiempo (ej. "los últimos 15 minutos"). Dicta la frecuencia de los backups y la estrategia de replicación.



### 1.5 Cómputo Serverless y Optimización de Ejecución

* **Azure Functions:** El modelo de ejecución orientada a eventos donde el desarrollador despliega pequeñas piezas de código sin aprovisionar ni administrar servidores subyacentes.


* **El problema del 'Cold Start' (Arranque en frío):** La latencia inicial que sufre una función bajo el plan de consumo (*Consumption Plan*) al despertarse tras un periodo de inactividad.


* **Mitigación del Cold Start:** Técnicas arquitectónicas para evadir esta latencia, optando por planes Premium o *App Service Plans* que mantienen instancias pre-calentadas (*always ready*), o implementando *pings* periódicos para mantener la función activa en memoria.