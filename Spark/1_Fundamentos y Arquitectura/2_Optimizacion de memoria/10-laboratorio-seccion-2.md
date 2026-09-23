# Laboratorio de Pruebas: Estructuras de Datos Computacionales (De RDDs a DataFrames)

> Objetivo: usando el mismo clúster Spark Standalone del laboratorio de la Sección 1 (o `local[*]` si prefieres algo más ágil), comprobar **en vivo** las propiedades de los RDDs, la diferencia real de optimización entre RDDs y DataFrames, y el efecto del Motor Tungsten.

## Índice

1. [Requisitos previos](#1-requisitos-previos)
2. [Preparar el entorno de este laboratorio](#2-preparar-el-entorno-de-este-laboratorio)
3. [✅ Comprobación 1 — Inmutabilidad de los RDDs](#3--comprobación-1--inmutabilidad-de-los-rdds)
4. [✅ Comprobación 2 — Particionamiento de un RDD](#4--comprobación-2--particionamiento-de-un-rdd)
5. [✅ Comprobación 3 — Linaje y dependencias (Narrow vs Wide)](#5--comprobación-3--linaje-y-dependencias-narrow-vs-wide)
6. [✅ Comprobación 4 — Tolerancia a fallos: reconstrucción vía linaje](#6--comprobación-4--tolerancia-a-fallos-reconstrucción-vía-linaje)
7. [✅ Comprobación 5 — `cache()` vs `checkpoint()`](#7--comprobación-5--cache-vs-checkpoint)
8. [✅ Comprobación 6 — Opacidad semántica: RDD vs DataFrame optimizado](#8--comprobación-6--opacidad-semántica-rdd-vs-dataframe-optimizado)
9. [✅ Comprobación 7 — Esquemas explícitos vs inferidos](#9--comprobación-7--esquemas-explícitos-vs-inferidos)
10. [✅ Comprobación 8 — Motor Tungsten: UnsafeRow y Whole-Stage CodeGen](#10--comprobación-8--motor-tungsten-unsaferow-y-whole-stage-codegen)
11. [✅ Comprobación 9 — Tungsten en acción: GC y memoria, RDD vs DataFrame](#11--comprobación-9--tungsten-en-acción-gc-y-memoria-rdd-vs-dataframe)
12. [Troubleshooting](#12-troubleshooting)
13. [Checklist final de validación](#13-checklist-final-de-validación)

---

## 1. Requisitos previos

Este laboratorio asume que ya tienes montado el clúster Standalone del laboratorio de la Sección 1 (`docker-compose.yml` con `spark-master`, `spark-worker-1`, `spark-worker-2`). Si no lo tienes, puedes:

- **Opción A (recomendada, consistente con el laboratorio anterior):** reutilizar ese mismo `docker-compose.yml` y levantar el clúster con `docker compose up -d`.
- **Opción B (más simple, solo para este laboratorio):** usar `local[4]` directamente desde un contenedor `bitnami/spark` suelto o desde PySpark instalado en tu host (`pip install pyspark`). Todas las comprobaciones de este documento funcionan igual en `local[4]`, salvo donde se indique explícitamente lo contrario (ej. la comprobación de tolerancia a fallos, que requiere Executors reales y separados).

```bash
# Verifica que el clúster de la Sección 1 sigue disponible
docker compose ps
```

---

## 2. Preparar el entorno de este laboratorio

Dentro de la carpeta `apps/` de tu proyecto (`lab-spark-arquitectura/apps/`), crea una subcarpeta para mantener organizados los scripts de esta sección:

```bash
mkdir -p apps/seccion2
```

Todos los scripts de este laboratorio se guardarán ahí y se ejecutarán igual que en la Sección 1:

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/apps/seccion2/nombre_del_script.py
```

---

## 3. ✅ Comprobación 1 — Inmutabilidad de los RDDs

Crea `apps/seccion2/01_inmutabilidad.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Inmutabilidad").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext

rdd_original = sc.parallelize([1, 2, 3, 4, 5])
rdd_transformado = rdd_original.map(lambda x: x * 100)

print("RDD original:     ", rdd_original.collect())
print("RDD transformado: ", rdd_transformado.collect())
print("¿Son el mismo objeto?", rdd_original is rdd_transformado)

# Aplicamos VARIAS transformaciones adicionales sobre el original,
# para demostrar que sigue intacto sin importar cuántos "hijos" genere
rdd_hijo_2 = rdd_original.filter(lambda x: x > 2)
rdd_hijo_3 = rdd_original.map(lambda x: x + 1000)

print("RDD original después de crear más hijos:", rdd_original.collect())

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/01_inmutabilidad.py
```

**Comprobación esperada:**
- [ ] `rdd_original.collect()` imprime `[1, 2, 3, 4, 5]` **antes y después** de crear transformaciones derivadas.
- [ ] `rdd_original is rdd_transformado` → `False` (son objetos distintos).
- [ ] El último `print` confirma que ni `rdd_hijo_2` ni `rdd_hijo_3` alteraron `rdd_original`.

---

## 4. ✅ Comprobación 2 — Particionamiento de un RDD

Crea `apps/seccion2/02_particionamiento_rdd.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParticionamientoRDD").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(range(0, 1000), numSlices=8)
print("Número de particiones:", rdd.getNumPartitions())

def contar_por_particion(indice, iterador):
    yield (indice, sum(1 for _ in iterador))

distribucion = rdd.mapPartitionsWithIndex(contar_por_particion).collect()
print("Elementos por partición:", distribucion)

# Repartimos a un número distinto y confirmamos el cambio
rdd_repart = rdd.repartition(3)
print("Tras repartition(3):", rdd_repart.getNumPartitions())

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/02_particionamiento_rdd.py
```

**Comprobación esperada:**
- [ ] `getNumPartitions()` inicial = 8 (coincide con `numSlices=8`).
- [ ] `distribucion` muestra 8 tuplas `(índice, conteo)`, sumando en total 1000 elementos.
- [ ] Tras `.repartition(3)`, el número de particiones cambia exactamente a 3.
- [ ] En la UI (`:4040` → pestaña "Jobs" → "Stages"), confirma que el Job de `collect()` sobre `distribucion` generó **8 Tasks** en su Stage.

---

## 5. ✅ Comprobación 3 — Linaje y dependencias (Narrow vs Wide)

Crea `apps/seccion2/03_linaje_dependencias.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LinajeDependencias").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext

pares = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5)], numSlices=4)

# Dependencia ESTRECHA
mapeado = pares.map(lambda kv: (kv[0], kv[1] * 10))
print("=== Dependencias de 'mapeado' (esperado: Narrow/OneToOne) ===")
print(mapeado.dependencies())

# Dependencia ANCHA (requiere shuffle)
agrupado = pares.groupByKey()
print("\n=== Dependencias de 'agrupado' (esperado: Shuffle) ===")
print(agrupado.dependencies())

print("\n=== Linaje completo (toDebugString) de 'agrupado' ===")
print(agrupado.toDebugString().decode("utf-8"))

# Disparamos una Action para poder ver Jobs/Stages en la UI
resultado = agrupado.mapValues(list).collect()
print("\nResultado agrupado:", resultado)

input("Presiona Enter para terminar y revisar la UI en :4040...")
spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/03_linaje_dependencias.py
```

**Comprobación esperada:**
- [ ] `mapeado.dependencies()` imprime algo como `[<pyspark.rdd.OneToOneDependency object at ...>]` (dependencia estrecha).
- [ ] `agrupado.dependencies()` imprime algo como `[<pyspark.rdd.ShuffleDependency object at ...>]` (dependencia ancha).
- [ ] `toDebugString()` muestra el linaje completo, y **el símbolo `(N)`** al inicio de cada línea con un número mayor indica un punto de shuffle (verás algo como `(4)` vs `(N)` cambiando entre niveles).
- [ ] En la UI (`:4040` → "Jobs" → tu Job de `collect()`), la pestaña **"Stages"** debe mostrar **2 Stages**, confirmando que el `groupByKey()` cortó el DAG exactamente donde esperábamos.

---

## 6. ✅ Comprobación 4 — Tolerancia a fallos: reconstrucción vía linaje

> **Requiere el clúster Docker Standalone real** (no funciona igual de ilustrativo en `local[*]`, porque ahí no hay Executors como procesos separados que puedas matar).

Crea `apps/seccion2/04_tolerancia_fallos.py`:

```python
from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("ToleranciaFallos")
    .master("spark://spark-master:7077")
    .config("spark.executor.instances", "2")
    .getOrCreate()
)
sc = spark.sparkContext

rdd_base = sc.parallelize(range(0, 2_000_000), numSlices=8)
rdd_transformado = rdd_base.map(lambda x: x * 2).filter(lambda x: x % 3 == 0)
rdd_transformado.cache()

print("Primer count (calcula y cachea):", rdd_transformado.count())
print("Application ID:", sc.applicationId)
print("\nAHORA: en otra terminal, ejecuta:")
print("  docker restart spark-worker-1")
print("(esto mata los executors de ese worker, perdiendo las particiones cacheadas ahí)")
print("\nEsperando 30 segundos para que reinicies el worker...")
time.sleep(30)

print("\nSegundo count (debe recalcular las particiones perdidas usando el linaje):")
print(rdd_transformado.count())

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/04_tolerancia_fallos.py
```

**Mientras el script espera los 30 segundos**, en otra terminal:

```bash
docker restart spark-worker-1
```

**Comprobación esperada:**
- [ ] El **segundo `count()`** devuelve el **mismo resultado** que el primero, a pesar de haber matado un worker completo en medio del proceso.
- [ ] En la UI (`:4040` → pestaña "Executors"), verás que el Executor del worker reiniciado aparece como perdido/nuevo (dependiendo del timing), y Spark **reasignó las Tasks de las particiones perdidas** a los Executors sobrevivientes o al Executor recién levantado.
- [ ] En los logs del Driver (`docker logs spark-master` o la salida de consola), busca menciones a `Lost executor` seguido de una re-ejecución de Tasks — evidencia directa de la reconstrucción vía linaje.

> Si `spark-worker-1` tarda en volver a registrarse con el Master, el segundo `count()` puede tardar más de lo normal mientras Spark reintenta — esto es esperado y forma parte de la comprobación.

---

## 7. ✅ Comprobación 5 — `cache()` vs `checkpoint()`

Crea `apps/seccion2/05_cache_vs_checkpoint.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CacheVsCheckpoint").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext
sc.setCheckpointDir("/opt/output/checkpoints")

rdd = sc.parallelize(range(0, 500000), numSlices=6)
for i in range(10):  # linaje artificialmente largo
    rdd = rdd.map(lambda x, i=i: x + i)

print("=== ANTES del checkpoint ===")
print("Linaje (primeras líneas):")
print(rdd.toDebugString().decode("utf-8")[:500])

rdd.checkpoint()
rdd.count()  # materializa el checkpoint

print("\n=== DESPUÉS del checkpoint ===")
print("Linaje (debe verse truncado/simplificado):")
print(rdd.toDebugString().decode("utf-8")[:500])
print("\n¿Está checkpointed?", rdd.isCheckpointed())
print("Directorio del checkpoint:", rdd.getCheckpointFile())

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/05_cache_vs_checkpoint.py
```

**Comprobación esperada:**
- [ ] El primer `toDebugString()` muestra una cadena larga con ~10 niveles de `MapPartitionsRDD` encadenados.
- [ ] `rdd.isCheckpointed()` → `True` después de la Action.
- [ ] `rdd.getCheckpointFile()` apunta a una ruta dentro de `/opt/output/checkpoints/...` — verifica en tu host: `ls output/checkpoints/` debe mostrar archivos reales escritos a disco.
- [ ] El segundo `toDebugString()` refleja que el RDD ahora tiene un origen más corto (el checkpoint actúa como nuevo "punto de partida" del linaje).

---

## 8. ✅ Comprobación 6 — Opacidad semántica: RDD vs DataFrame optimizado

Primero, genera un archivo Parquet con una columna claramente descartable (para poder observar Column Pruning) y un filtro selectivo (para Predicate Pushdown):

```python
# apps/seccion2/06a_generar_datos_opacidad.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr

spark = SparkSession.builder.appName("GenerarOpacidad").master("spark://spark-master:7077").getOrCreate()

df = (
    spark.range(0, 2_000_000)
    .withColumn("pais", expr("element_at(array('PE','CO','CL','MX'), (rand()*4+1)::int)"))
    .withColumn("monto", rand() * 1000)
    .withColumn("columna_pesada_no_usada", expr("repeat('x', 200)"))  # simula una columna ancha/costosa
)
df.write.mode("overwrite").parquet("/opt/data/opacidad_demo/")
print("Datos generados en /opt/data/opacidad_demo/")
spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/06a_generar_datos_opacidad.py
```

Ahora, el script comparativo:

```python
# apps/seccion2/06b_rdd_vs_dataframe.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("RDDvsDataFrame").master("spark://spark-master:7077").getOrCreate()

print("\n========== CAMINO 1: vía RDD (opaco) ==========")
rdd = spark.read.parquet("/opt/data/opacidad_demo/").rdd
# Aquí Spark NO sabe que solo usamos 'pais' y 'monto': trae la fila COMPLETA
# (incluyendo columna_pesada_no_usada) a cada Task antes de que tu función decida qué usar
rdd_filtrado = rdd.filter(lambda row: row.pais == "PE" and row.monto > 500)
print("Cantidad (vía RDD):", rdd_filtrado.count())

print("\n========== CAMINO 2: vía DataFrame (Catalyst) ==========")
df = spark.read.parquet("/opt/data/opacidad_demo/")
df_filtrado = df.filter((col("pais") == "PE") & (col("monto") > 500)).select("pais", "monto")
print("Cantidad (vía DataFrame):", df_filtrado.count())

print("\n========== PLAN FÍSICO DEL DATAFRAME (revisa Column Pruning / PushedFilters) ==========")
df_filtrado.explain(True)

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/06b_rdd_vs_dataframe.py
```

**Comprobación esperada:**
- [ ] En el plan físico (`.explain(True)`) del camino DataFrame, busca la línea `PushedFilters: [...]` dentro del nodo de lectura Parquet — confirma que el filtro `pais = 'PE'` y `monto > 500` se empujó **hasta la fuente de datos** (Predicate Pushdown).
- [ ] En el mismo plan, la sección `ReadSchema` debe listar **solo** `pais` y `monto` — confirmando **Column Pruning** (la columna `columna_pesada_no_usada` nunca se lee).
- [ ] En la UI (`:4040` → pestaña "Stages"), compara las métricas de **"Input Size"** entre el Job del camino RDD y el del camino DataFrame: el camino DataFrame debería leer significativamente **menos bytes** desde disco, precisamente por el pruning.
- [ ] El camino RDD no tiene ningún plan físico optimizable que inspeccionar — no existe el concepto de `.explain()` con sentido equivalente para un RDD puro (puedes intentar `rdd_filtrado.toDebugString()` y comparar cuán poco te dice sobre optimización real, solo sobre el linaje de transformaciones).

---

## 9. ✅ Comprobación 7 — Esquemas explícitos vs inferidos

Crea `apps/seccion2/07_esquemas.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("Esquemas").master("spark://spark-master:7077").getOrCreate()

# Genera un CSV de prueba con un código postal que debería seguir siendo texto
datos = [("Ana", "00234", 1500.0), ("Luis", "00987", 890.5), ("Marta", "01023", 230.0)]
spark.createDataFrame(datos, ["nombre", "codigo_postal", "monto"]) \
    .write.mode("overwrite").option("header", "true").csv("/opt/data/prueba_esquema/")

print("=== SIN esquema explícito (inferSchema=True) ===")
df_inferido = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/data/prueba_esquema/")
df_inferido.printSchema()
df_inferido.show()

print("\n=== CON esquema explícito ===")
esquema = StructType([
    StructField("nombre", StringType(), True),
    StructField("codigo_postal", StringType(), True),  # forzamos String, NO Integer
    StructField("monto", DoubleType(), True),
])
df_explicito = spark.read.schema(esquema).option("header", "true").csv("/opt/data/prueba_esquema/")
df_explicito.printSchema()
df_explicito.show()

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/07_esquemas.py
```

**Comprobación esperada:**
- [ ] En el DataFrame **inferido**, `codigo_postal` aparece como `integer`, y al mostrarlo, `"00234"` se muestra como `234` — **se perdió el cero inicial**.
- [ ] En el DataFrame **con esquema explícito**, `codigo_postal` conserva el tipo `string` y el valor completo `00234` intacto.
- [ ] Compara los tiempos de los dos `spark.read` en la UI (`:4040` → "Jobs"): el camino con `inferSchema=True` debería generar un **Job adicional** dedicado exclusivamente a inferir el esquema (una pasada de lectura extra), ausente en el camino con esquema explícito.

---

## 10. ✅ Comprobación 8 — Motor Tungsten: UnsafeRow y Whole-Stage CodeGen

Crea `apps/seccion2/08_tungsten_codegen.py`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TungstenCodeGen")
    .master("spark://spark-master:7077")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "512m")
    .getOrCreate()
)

df = spark.range(0, 10_000_000).selectExpr("id", "id * 2 as doble", "cast(id as string) as texto")
resultado = df.filter("id % 2 = 0").selectExpr("id", "doble * 1.1 as ajustado")

print("=== Plan físico con Whole-Stage Code Generation ===")
resultado.explain(True)

print("\nBuscar en la salida de arriba: los operadores marcados con '*(n)' antes de su nombre")
print("(ej. '*(1) Project', '*(1) Filter') indican que fueron fusionados en un único bucle compilado.")

resultado.count()

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/08_tungsten_codegen.py
```

**Comprobación esperada:**
- [ ] En la sección `== Physical Plan ==` de la salida, localiza al menos dos operadores consecutivos (ej. `Project` y `Filter`) que compartan el **mismo número entre paréntesis**, como `*(1) Project` y `*(1) Filter` — esto confirma que Tungsten los fusionó en un solo bucle generado en tiempo de ejecución.
- [ ] Confirma que la configuración `spark.memory.offHeap.enabled=true` fue aceptada sin error revisando `spark.conf.get("spark.memory.offHeap.enabled")` (puedes añadir esa línea de `print` al script si quieres verificarlo explícitamente).

---

## 11. ✅ Comprobación 9 — Tungsten en acción: GC y memoria, RDD vs DataFrame

Crea `apps/seccion2/09_gc_rdd_vs_dataframe.py`:

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("GCComparativa").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext

N = 8_000_000

print("=== CAMINO RDD (sin Tungsten) ===")
inicio = time.time()
rdd = sc.parallelize(range(N), numSlices=8)
resultado_rdd = rdd.map(lambda x: (x, x * 2, str(x))).filter(lambda t: t[0] % 2 == 0).count()
duracion_rdd = time.time() - inicio
print(f"Resultado: {resultado_rdd} filas en {duracion_rdd:.2f}s")

time.sleep(3)

print("\n=== CAMINO DataFrame (con Tungsten) ===")
inicio = time.time()
df = spark.range(0, N).selectExpr("id", "id * 2 as doble", "cast(id as string) as texto")
resultado_df = df.filter("id % 2 = 0").count()
duracion_df = time.time() - inicio
print(f"Resultado: {resultado_df} filas en {duracion_df:.2f}s")

print(f"\nDiferencia de tiempo: RDD={duracion_rdd:.2f}s vs DataFrame={duracion_df:.2f}s")
print("Revisa la UI en :4040 -> pestaña Executors -> columna de GC Time por cada Job.")

input("Presiona Enter para terminar...")
spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion2/09_gc_rdd_vs_dataframe.py
```

**Comprobación esperada:**
- [ ] En la UI (`:4040` → pestaña "Jobs"), identifica el Job correspondiente al camino RDD y al camino DataFrame por separado (por su hora de inicio y descripción).
- [ ] Entra a la pestaña "Stages" de cada uno y revisa la columna **"GC Time"** en el detalle de Tasks — el camino RDD debería mostrar, proporcionalmente, **más tiempo de Garbage Collection** que el camino DataFrame equivalente (por el overhead de objetos Python/Java dispersos vs. el formato binario compacto de Tungsten).
- [ ] Compara `duracion_rdd` vs `duracion_df` impresos en consola — en datasets de este tamaño, el camino DataFrame normalmente termina siendo más rápido.

> **Nota honesta de laboratorio**: en máquinas locales con Docker y pocos recursos, la diferencia puede no ser dramática (el overhead de PySpark para RDDs con funciones Python añade su propio costo de serialización vía Pickle, independiente de Tungsten). Lo importante pedagógicamente es **identificar la métrica de GC Time en la UI** y entender qué la origina, más que memorizar un número exacto de mejora.

---

## 12. Troubleshooting

| Problema | Causa probable | Solución |
|---|---|---|
| `docker restart spark-worker-1` no genera ningún efecto visible | El worker se reinició demasiado rápido y ya estaba disponible de nuevo antes de que Spark necesitara reasignar Tasks | Aumenta el `time.sleep()` del script, o repite el reinicio justo cuando la Stage esté en progreso |
| `rdd.toDebugString()` lanza error de codificación | Falta decodificar el resultado (`bytes`) a texto | Usa `.decode("utf-8")` como en los ejemplos |
| `PushedFilters` no aparece en el plan físico | El formato de archivo no soporta pushdown, o el filtro no es "sargable" | Usa Parquet (como en el ejemplo) y filtros simples de igualdad/comparación directa sobre columnas |
| El checkpoint no se ve reflejado en `output/checkpoints/` en tu host | La ruta usada en `setCheckpointDir` no coincide con el volumen montado | Verifica que uses `/opt/output/...`, que corresponde a `./output:/opt/output` en `docker-compose.yml` |
| No ves diferencia de tiempo entre RDD y DataFrame | Dataset demasiado pequeño para que el overhead se note, o recursos del clúster limitados | Aumenta `N` en el script, o enfócate en la métrica de GC Time en vez del tiempo total |

---

## 13. Checklist final de validación

- [ ] Confirmaste que un RDD original permanece intacto tras generar múltiples RDDs derivados
- [ ] Verificaste el número de particiones de un RDD y su distribución real de elementos por partición
- [ ] Distinguiste una `OneToOneDependency` (Narrow) de una `ShuffleDependency` (Wide) con `.dependencies()`
- [ ] Viste el linaje completo con `.toDebugString()` y ubicaste dónde ocurre el shuffle
- [ ] Provocaste la pérdida de un Executor real y confirmaste que Spark reconstruyó los datos perdidos automáticamente vía linaje
- [ ] Confirmaste que `.checkpoint()` trunca el linaje y escribe a disco, mientras `.cache()` no
- [ ] Encontraste `PushedFilters` y una `ReadSchema` reducida en el plan físico de un DataFrame, algo imposible de lograr con un RDD equivalente
- [ ] Reprodujiste el error clásico de `inferSchema=True` perdiendo ceros a la izquierda, y lo corregiste con un esquema explícito
- [ ] Identificaste el símbolo `*(n)` de Whole-Stage Code Generation en un plan físico
- [ ] Comparaste el GC Time en la UI entre un Job basado en RDDs y uno equivalente basado en DataFrames

Si completaste todos estos puntos, tienes evidencia práctica y verificada — no solo teórica — de por qué DataFrames/Datasets superan a los RDDs en la gran mayoría de casos de uso modernos.
