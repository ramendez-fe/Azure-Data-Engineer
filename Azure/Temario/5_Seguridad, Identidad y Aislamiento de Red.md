### 5.1 Gestión de Identidad y Accesos (IAM)

* **Azure Active Directory / Microsoft Entra ID:** Es el servicio central de identidad y control de acceso de Azure, utilizado para gestionar la autenticación y autorización tanto de usuarios humanos como de aplicaciones.


* **Autenticación vs. Autorización:** La frontera de seguridad en IAM. La autenticación se encarga de verificar la identidad del usuario (demostrar "quién eres"), mientras que la autorización determina a qué recursos puede acceder y qué acciones tiene permitidas ese usuario una vez validado (demostrar "qué puedes hacer").



### 5.2 Identidades de Aplicación (No Humanas)

* **Service Principal:** Es una identidad de seguridad aprovisionada específicamente para ser utilizada por aplicaciones, servicios o herramientas de automatización (como pipelines de CI/CD), permitiéndoles autenticarse y acceder a los recursos de Azure de forma programática.


* **Managed Identity (Identidad Administrada):** Es una evolución de seguridad sobre el Service Principal tradicional. Es administrada automáticamente por Azure AD para un recurso específico (como una máquina virtual o una Azure Function), eliminando la necesidad de almacenar contraseñas, secretos o credenciales embebidas en el código de la aplicación.



### 5.3 Control de Privilegios

* **RBAC (Role-Based Access Control):** Es el sistema de autorización fundamental de Azure. Otorga permisos a identidades (usuarios, grupos o aplicaciones) mediante la asignación de roles específicos sobre un ámbito o nivel jerárquico determinado (un recurso individual, un grupo de recursos o toda una suscripción).


* **Principio de Least Privilege (Mínimo Privilegio):** Es la directiva de diseño de roles empresariales que exige otorgar a cada identidad única y estrictamente los permisos necesarios para realizar su función, aplicados en el ámbito más reducido posible, con el fin de minimizar la superficie de riesgo ante brechas de seguridad.



### 5.4 Protección de Secretos y Cifrado

* **Azure Key Vault:** Es el servicio de bóveda diseñado para almacenar y gestionar de forma centralizada y segura contraseñas, secretos, certificados y claves de cifrado de la organización.


* **Customer-Managed Keys (CMK):** Para el cifrado de datos en reposo, utilizar claves administradas por el cliente en lugar de las que provee Microsoft permite a la organización controlar el ciclo de vida exacto de la clave (rotación o revocación) desde su propio Key Vault, lo cual es vital para cumplir con requisitos normativos y regulatorios estrictos.



### 5.5 Aislamiento Perimetral y Redes Privadas

* **Virtual Network (VNet):** Representa una red privada lógicamente aislada dentro de Azure, la cual proporciona el perímetro de red para que los recursos (como las máquinas virtuales) se comuniquen de forma segura entre sí y con el exterior.


* **Network Security Group (NSG):** Funciona como un firewall a nivel de red, utilizando un conjunto de reglas de seguridad para filtrar, permitir o denegar explícitamente el tráfico entrante y saliente hacia los recursos dentro de una VNet.


* **Private Endpoint:** Es una interfaz de red física que inyecta un recurso PaaS (como una base de datos SQL o una cuenta de Storage) directamente dentro de tu VNet, otorgándole una IP privada y evitando completamente que el tráfico hacia el servicio transite o se exponga al internet público.


* **VNet Service Endpoints:** Como mecanismo alternativo de aislamiento de red, permiten exponer un servicio PaaS únicamente a un conjunto autorizado de VNets o subredes extendiendo la identidad de la red a través de la red troncal o *backbone* privado de Azure, sin requerir la creación de un Private Endpoint.