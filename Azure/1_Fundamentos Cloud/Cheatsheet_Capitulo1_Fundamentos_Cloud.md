# Cheatsheet — Capítulo 1: Fundamentos Cloud (Azure)

> Referencia rápida de repaso final — Balotario Azure Cloud (GDN-e Perú · Chapter Data).
> Resume 1.1 a 1.5. Úsalo el día del examen para repasar en minutos, no para aprender desde cero.

---

## 1.1 Arquitectura Lógica y Organización de Recursos

**Jerarquía:** `Management Group → Subscription → Resource Group → Resource` (permisos y políticas se heredan hacia abajo).

| Concepto | Definición en una línea |
|---|---|
| **Resource Group** | Contenedor lógico que agrupa recursos para administrarlos, monitorearlos y **facturarlos** juntos. Borrar el RG borra todo lo que contiene. |
| **RBAC** | **Quién** puede hacer **qué acción**, sobre un **scope**. = Principal + Role Definition + Scope (modelo aditivo). |
| **Roles built-in** | Owner (control total + otorga permisos) · Contributor (administra recursos, **no** otorga permisos) · Reader (solo lectura) · User Access Administrator (solo administra accesos). |
| **Service Principal** | Identidad para apps/automatización (CI/CD) — no es una persona. |
| **Managed Identity** | Identidad auto-administrada por Azure AD, ligada a un recurso, **sin secretos** en el código. |
| **Least Privilege** | Dar solo el permiso mínimo necesario, en el scope más reducido posible. |
| **Azure Policy** | **Qué configuración** es válida en un recurso (tags obligatorios, regiones/SKU permitidos) — sin importar quién lo creó. Efectos: Deny, Audit, Append, Modify, DeployIfNotExists. |
| **Autenticación vs. Autorización** | AuthN = quién eres (login). AuthZ = qué puedes hacer (RBAC). AuthN siempre ocurre antes. |

🔑 **RBAC = personas/identidades. Azure Policy = configuración de recursos.** (El distractor #1 del balotario)

---

## 1.2 Modelos de Servicio y Gobernanza en la Nube

| Modelo | Cliente administra | Azure administra | Ejemplo |
|---|---|---|---|
| **IaaS** | SO, parches, runtime, apps, datos | Virtualización, hardware físico | Azure VMs |
| **PaaS** | Apps, datos | **SO, runtime, parches, backups** | **Azure SQL Database** (ejemplo canónico) |
| **SaaS** | Solo uso/datos de negocio | Todo (incl. la app) | Microsoft 365 |

**Shared Responsibility Model:** marco de **seguridad y cumplimiento** — la línea se mueve según IaaS/PaaS/SaaS.

🔑 **Siempre son del cliente, en TODOS los modelos (incluso SaaS):** Datos, Endpoints, Identidades.
🔑 **Siempre es de Microsoft:** seguridad física del datacenter, virtualización, red física.

---

## 1.3 Resiliencia Física y Escalabilidad

**Jerarquía geográfica:** `Geography → Region → Availability Zone → Datacenter`

| Concepto | Definición en una línea |
|---|---|
| **Availability Zone** | Ubicación física **separada dentro de una región**, con energía/refrigeración/red **independientes** → tolerancia a fallos de datacenter. **No** protege ante pérdida de la región completa (para eso: multi-región). |
| **Scale Up (vertical)** | Aumentar CPU/memoria de **una instancia existente**. Tiene techo. |
| **Scale Out (horizontal)** | Agregar **más instancias idénticas**. Casi ilimitado, se apoya en autoscaling. |
| Scale Down / Scale In | Operaciones inversas (reducir capacidad / quitar instancias). |

🔑 Distractor clásico: describir "agregar más instancias" como si fuera Scale **Up** → es **Scale Out**.

---

## 1.4 Continuidad de Negocio (Disaster Recovery)

| Métrica | Mide | Impacta |
|---|---|---|
| **RTO** (Recovery **Time** Objective) | **Tiempo** máximo de inactividad aceptable | Estrategia de **failover** |
| **RPO** (Recovery **Point** Objective) | **Datos** máximos aceptables a perder (medido en tiempo) | **Frecuencia de backups** / replicación |

🔑 Mnemotecnia: **RTO = reloj (tiempo caído). RPO = punto de los datos (cuánto se pierde).**

- **Active Geo-Replication** (Azure SQL DB): hasta 4 réplicas legibles multi-región → mejora RTO (failover) y RPO (replicación casi continua) + reduce latencia de lectura.

---

## 1.5 Cómputo Serverless y Optimización de Ejecución

| Concepto | Definición en una línea |
|---|---|
| **Azure Functions** | Cómputo **serverless orientado a eventos**; código sin aprovisionar/administrar servidores. Triggers: HTTP, Timer, Blob, Queue, Event Hub. |
| **Consumption Plan** | Pago por ejecución, **escala a cero** → aquí ocurre el Cold Start. |
| **Cold Start** | Latencia al "despertar" una función tras inactividad (bajo Consumption Plan). |
| **Premium / App Service Plan** | Instancias **pre-calentadas ("always ready")** → elimina el Cold Start. |
| **Pings periódicos** | Mantienen la función activa dentro de Consumption Plan → **mitigan**, no eliminan del todo, el Cold Start. |

🔑 Sí existen mitigaciones para el Cold Start (nunca marques "no se puede reducir").

---

## Tabla maestra de respuestas rápidas (preguntas del balotario cubiertas)

| ID | Tema | Pregunta (resumen) | Respuesta clave |
|---|---|---|---|
| b1 | 1.1 | Resource Group | Contenedor lógico: admin + monitoreo + facturación conjunta |
| b6 | 1.1 | Azure AD / Entra ID | Servicio de identidad: autenticación y autorización |
| b7 | 1.1 | RBAC | Permisos por rol + principal + scope |
| i12 | 1.1 | Azure Policy | Gobernanza de configuración (tags, regiones, SKU) |
| i13 | 1.1 | AuthN vs AuthZ | AuthN = quién eres; AuthZ = qué puedes hacer |
| i14 | 1.1 | Service Principal | Identidad para apps/CI-CD, no persona |
| a19 | 1.1 | Least Privilege | Mínimo permiso necesario, mínimo scope |
| b2 | 1.2 | Azure SQL DB | Es PaaS: Azure administra motor, parches, backups |
| b3 | 1.2 | IaaS vs PaaS vs SaaS | Nivel de responsabilidad que administra el cliente |
| i18 | 1.2 | Shared Responsibility Model | Qué gestiona Microsoft vs. cliente, según modelo |
| b17 | 1.3 | Availability Zone | Ubicación física separada, energía/red independientes |
| b18 | 1.3 | Scale Up | Aumentar CPU/memoria de instancia existente (no agregar instancias) |
| a8 | 1.4 | RTO vs RPO | RTO = tiempo de restauración; RPO = datos perdidos |
| a9 | 1.4 | Active Geo-Replication | Hasta 4 réplicas legibles multi-región, failover + baja latencia |
| b12 | 1.5 | Azure Function | Serverless orientado a eventos, sin administrar servidores |
| a18 | 1.5 | Mitigar Cold Start | Premium/App Service Plan (always ready) o pings periódicos |

---

## Los 5 distractores más repetidos del balotario (repaso de 30 segundos)

1. **RBAC ≠ Azure Policy** — RBAC = quién/qué acción; Policy = qué configuración.
2. **Scale Up ≠ Scale Out** — Up = más grande una instancia; Out = más instancias.
3. **RTO ≠ RPO** — RTO = tiempo caído; RPO = datos perdidos.
4. **SaaS no libera al cliente de TODO** — datos, endpoints e identidades siguen siendo del cliente.
5. **El Cold Start SÍ se puede mitigar** — Premium/Dedicated (lo elimina) o pings (lo reduce).
