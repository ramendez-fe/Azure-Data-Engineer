# Laboratorio de Pruebas: Evaluación Perezosa (Lazy Evaluation) y el Grafo de Ejecución

> Objetivo: usando el mismo clúster Spark Standalone de los laboratorios anteriores, comprobar en vivo la Evaluación Perezosa, la taxonomía de operaciones (Narrow/Wide/Actions), la construcción del DAG, la ejecución secuencial de Stages, y la reconstrucción determinista ante fallos.

## Índice

1. [Requisitos previos](#1-requisitos-previos)
2. [Preparar el entorno de este laboratorio](#2-preparar-el-entorno-de-este-laboratorio)
3. [✅ Comprobación 1 — Las transformaciones no ejecutan nada (Lazy Evaluation)](#3--comprobación-1--las-transformaciones-no-ejecutan-nada-lazy-evaluation)
4. [✅ Comprobación 2 — Identificar Narrow vs Wide por código](#4--comprobación-2--identificar-narrow-vs-wide-por-código)
5. [✅ Comprobación 3 — Las 4 representaciones del plan (Parsed → Analyzed → Optimized → Physical)](#5--comprobación-3--las-4-representaciones-del-plan-parsed--analyzed--optimized--physical)
6. [✅ Comprobación 4 — Visualizando el DAG y las Stages en la UI](#6--comprobación-4--visualizando-el-dag-y-las-stages-en-la-ui)
7. [✅ Comprobación 5 — Ejecución secuencial de Stages (no paralela entre sí)](#7--comprobación-5--ejecución-secuencial-de-stages-no-paralela-entre-sí)
8. [✅ Comprobación 6 — Un bucle `for` no crea un ciclo en el DAG](#8--comprobación-6--un-bucle-for-no-crea-un-ciclo-en-el-dag)
9. [✅ Comprobación 7 — Reconstrucción determinista tras un fallo real](#9--comprobación-7--reconstrucción-determinista-tras-un-fallo-real)
10. [✅ Comprobación 8 — Cuando el determinismo se rompe: funciones no puras](#10--comprobación-8--cuando-el-determinismo-se-rompe-funciones-no-puras)
11. [Troubleshooting](#11-troubleshooting)
12. [Checklist final de validación](#12-checklist-final-de-validación)

---

## 1. Requisitos previos

Este laboratorio reutiliza el clúster Docker Standalone de la Sección 1 (`spark-master` + `spark-worker-1` + `spark-worker-2`) y los datos generados en el laboratorio de la Sección 1 (`/opt/data/ventas_normal/`). Si no los tienes:

```bash
docker compose ps   # confirma que el clúster sigue arriba
ls data/ventas_normal/   # confirma que los datos de prueba existen
```

Si faltan los datos, vuelve a ejecutar `apps/generar_datos.py` del laboratorio de la Sección 1 antes de continuar.

---

## 2. Preparar el entorno de este laboratorio

```bash
mkdir -p apps/seccion3
```

Todos los scripts se ejecutan igual que en los laboratorios anteriores:

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/apps/seccion3/nombre_del_script.py
```

---

## 3. ✅ Comprobación 1 — Las transformaciones no ejecutan nada (Lazy Evaluation)

Crea `apps/seccion3/01_lazy_evaluation.py`:

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("LazyEvaluation").master("spark://spark-master:7077").getOrCreate()

print("Application ID:", spark.sparkContext.applicationId)
print("Revisa YA MISMO la UI en :4040 -> pestaña Jobs. Debe estar VACÍA.")
time.sleep(15)

df = spark.read.parquet("/opt/data/ventas_normal/")
paso1 = df.filter(df.monto > 100)
paso2 = paso1.select("cliente_id", "categoria", "monto")
paso3 = paso2.groupBy("categoria")
paso4 = paso3.sum("monto")
paso5 = paso4.orderBy("categoria")

print("\nSe encadenaron 5 transformaciones (incluyendo lectura, filter, select, groupBy, orderBy).")
print("Revisa la UI de nuevo AHORA. La pestaña Jobs debe SEGUIR VACÍA.")
time.sleep(15)

print("\nEjecutando la Acción .collect() ahora...")
resultado = paso5.collect()
print("Resultado:", resultado)
print("\nRevisa la UI: AHORA debe aparecer exactamente 1 Job nuevo.")
time.sleep(20)

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/01_lazy_evaluation.py
```

**Comprobación esperada (ve alternando entre la terminal y `http://localhost:4040`):**
- [ ] Tras iniciar la `SparkSession`, la pestaña "Jobs" de la UI está **vacía** (0 Jobs).
- [ ] Tras encadenar las 5 transformaciones (`filter`, `select`, `groupBy`, `sum`, `orderBy`), la UI **sigue mostrando 0 Jobs** — ninguna transformación, ni siquiera una Wide como `groupBy`, dispara ejecución por sí sola.
- [ ] Solo **después** de `.collect()` aparece **1 Job** en la UI.

---

## 4. ✅ Comprobación 2 — Identificar Narrow vs Wide por código

Crea `apps/seccion3/02_narrow_vs_wide.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NarrowVsWide").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext

pares = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("c", 4)], numSlices=4)

operaciones = {
    "map (Narrow esperado)": pares.map(lambda kv: (kv[0], kv[1] * 10)),
    "filter (Narrow esperado)": pares.filter(lambda kv: kv[1] > 1),
    "flatMap (Narrow esperado)": pares.flatMap(lambda kv: [kv, kv]),
    "groupByKey (Wide esperado)": pares.groupByKey(),
    "reduceByKey (Wide esperado)": pares.reduceByKey(lambda a, b: a + b),
    "repartition (Wide esperado)": pares.repartition(2),
}

for nombre, rdd_resultado in operaciones.items():
    tipo_dep = type(rdd_resultado.dependencies()[0]).__name__
    print(f"{nombre:35s} -> dependencia real: {tipo_dep}")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/02_narrow_vs_wide.py
```

**Comprobación esperada (salida en consola):**

```
map (Narrow esperado)               -> dependencia real: OneToOneDependency
filter (Narrow esperado)            -> dependencia real: OneToOneDependency
flatMap (Narrow esperado)           -> dependencia real: OneToOneDependency
groupByKey (Wide esperado)          -> dependencia real: ShuffleDependency
reduceByKey (Wide esperado)         -> dependencia real: ShuffleDependency
repartition (Wide esperado)         -> dependencia real: ShuffleDependency
```

- [ ] Confirma que cada operación produjo exactamente el tipo de dependencia esperado según su clasificación teórica.

---

## 5. ✅ Comprobación 3 — Las 4 representaciones del plan (Parsed → Analyzed → Optimized → Physical)

Crea `apps/seccion3/03_planes_dag.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PlanesDAG").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")
resultado = (
    df.filter(df.monto > 100)
    .select("categoria", "monto")
    .groupBy("categoria")
    .sum("monto")
)

print("========== PLAN COMPLETO (4 niveles) ==========")
resultado.explain(extended=True)

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/03_planes_dag.py
```

**Comprobación esperada, buscando en la salida cada una de estas 4 secciones:**

- [ ] `== Parsed Logical Plan ==` — el plan tal cual se interpretó tu código, sin validar aún contra el catálogo.
- [ ] `== Analyzed Logical Plan ==` — ya resuelto contra el esquema real (nombres de columnas confirmados).
- [ ] `== Optimized Logical Plan ==` — tras aplicar reglas de optimización lógica (Predicate Pushdown, Column Pruning, etc. — se profundiza en la siguiente sección del temario).
- [ ] `== Physical Plan ==` — el plan final de ejecución, con el nodo `Exchange` marcando la frontera de Stage causada por el `groupBy`.

---

## 6. ✅ Comprobación 4 — Visualizando el DAG y las Stages en la UI

Crea `apps/seccion3/04_dag_visual.py`:

```python
from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("DAGVisual")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

df = spark.read.parquet("/opt/data/ventas_normal/")
resultado = (
    df.filter(df.monto > 100)
    .select("cliente_id", "categoria", "monto")
    .groupBy("categoria")
    .sum("monto")
    .orderBy("categoria")
)

print("Ejecutando Job... revisa :4040 -> Jobs -> (click en el Job) -> 'DAG Visualization'")
resultado.collect()

input("Presiona Enter para terminar (mantén la UI abierta mientras exploras)...")
spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/04_dag_visual.py
```

**En la UI, entra al Job generado y haz clic en "DAG Visualization". Comprobación esperada:**

- [ ] Ves **3 recuadros de Stage** (uno para el `filter`+`select` iniciales, otro para el `groupBy`+`sum`, y otro para el `orderBy` — el `orderBy` global también requiere shuffle, generando su propia frontera).
- [ ] Dentro de cada recuadro aparecen los operadores físicos concretos (`Scan parquet`, `Filter`, `Project`, `HashAggregate`, `Sort`, etc.).
- [ ] Las flechas que **conectan** los recuadros de Stage representan los puntos de shuffle.
- [ ] En la pestaña "Stages", cada Stage muestra **8 Tasks** en las etapas post-shuffle (por `spark.sql.shuffle.partitions=8`).

---

## 7. ✅ Comprobación 5 — Ejecución secuencial de Stages (no paralela entre sí)

Este script fuerza una demora artificial en la primera Stage para que puedas **observar en tiempo real** que la segunda Stage no arranca hasta que la primera termine por completo.

Crea `apps/seccion3/05_secuencialidad_stages.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import time

spark = (
    SparkSession.builder
    .appName("SecuencialidadStages")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

@udf(returnType=DoubleType())
def transformacion_lenta(monto):
    time.sleep(0.5)   # simula trabajo pesado por FILA, para alargar la Stage 0 visiblemente
    return monto * 1.18

df = spark.read.parquet("/opt/data/ventas_normal/").limit(200)  # pocas filas, para que el experimento sea rápido pero visible
con_demora = df.withColumn("monto_ajustado", transformacion_lenta(df.monto))

resultado = con_demora.groupBy("categoria").sum("monto_ajustado")

print("Job iniciado. Ve a :4040 -> Stages, y observa:")
print("  - Stage 0 tardará varios segundos (por la UDF lenta)")
print("  - Stage 1 (post-shuffle) debe aparecer como 'Pending'/no iniciada hasta que Stage 0 termine")
resultado.collect()
print("Job terminado.")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/05_secuencialidad_stages.py
```

**Mientras el Job corre, refresca la pestaña "Stages" de la UI repetidamente. Comprobación esperada:**

- [ ] Mientras la **Stage 0** está `Active`, la **Stage 1** aparece como `Pending` (aún no ha empezado ninguna de sus Tasks).
- [ ] Solo cuando la Stage 0 pasa a `Completed`, la Stage 1 cambia a `Active` y comienza a ejecutar sus Tasks.
- [ ] Esto confirma en la práctica la afirmación teórica: **las Stages se ejecutan de forma secuencial**, respetando la frontera de shuffle, sin importar que dentro de cada Stage las Tasks sí corran en paralelo.

---

## 8. ✅ Comprobación 6 — Un bucle `for` no crea un ciclo en el DAG

Crea `apps/seccion3/06_bucle_no_es_ciclo.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BucleNoEsCiclo").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(range(0, 100), numSlices=2)

for i in range(6):  # 6 "iteraciones" -> deben generar 6 nodos NUEVOS, no un ciclo
    rdd = rdd.map(lambda x, i=i: x + i)

print("=== Linaje resultante tras 6 iteraciones del bucle ===")
print(rdd.toDebugString().decode("utf-8"))

print(f"\nResultado final (primeros 5): {rdd.take(5)}")
print("\nCuenta cuántos niveles de 'PythonRDD' o 'MapPartitionsRDD' aparecen arriba:")
print("deben ser aproximadamente 6, uno por cada vuelta del bucle, en una CADENA LINEAL.")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/06_bucle_no_es_ciclo.py
```

**Comprobación esperada:**
- [ ] `toDebugString()` muestra una cadena de **múltiples niveles indentados**, uno por cada vuelta del bucle — no una estructura circular ni un único nodo repetido.
- [ ] El resultado (`take(5)`) refleja la suma acumulada esperada: cada elemento original incrementado por `0+1+2+3+4+5 = 15` en total.

---

## 9. ✅ Comprobación 7 — Reconstrucción determinista tras un fallo real

> Requiere el clúster Docker real (no `local[*]`).

Crea `apps/seccion3/07_reconstruccion_determinista.py`:

```python
from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("ReconstruccionDeterminista")
    .master("spark://spark-master:7077")
    .config("spark.executor.instances", "2")
    .getOrCreate()
)
sc = spark.sparkContext

# Transformaciones 100% deterministas (sin aleatoriedad)
rdd = sc.parallelize(range(0, 3_000_000), numSlices=8)
transformado = rdd.map(lambda x: x * 3).filter(lambda x: x % 7 == 0).map(lambda x: (x, x % 100))
transformado.cache()

resultado_1 = sorted(transformado.take(20))
print("Primeras 20 tuplas (ejecución 1):", resultado_1)
print(f"Total de elementos (ejecución 1): {transformado.count()}")

print("\nAHORA, en otra terminal, ejecuta: docker restart spark-worker-2")
print("Esperando 25 segundos...")
time.sleep(25)

resultado_2 = sorted(transformado.take(20))
total_2 = transformado.count()
print("\nPrimeras 20 tuplas (ejecución 2, tras el fallo simulado):", resultado_2)
print(f"Total de elementos (ejecución 2): {total_2}")

print("\n¿Los resultados de ambas ejecuciones son idénticos?", resultado_1 == resultado_2)

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/07_reconstruccion_determinista.py
```

En otra terminal, cuando el script lo indique:

```bash
docker restart spark-worker-2
```

**Comprobación esperada:**
- [ ] `resultado_1 == resultado_2` → `True`: a pesar de haber perdido un worker completo en medio del proceso, el resultado reconstruido es **idéntico bit a bit** al original.
- [ ] El conteo total también coincide exactamente entre ambas ejecuciones.
- [ ] En los logs del Driver (o en la UI, pestaña "Executors"), busca evidencia de que se perdió el Executor de `spark-worker-2` y de que sus Tasks se reasignaron y reejecutaron.

---

## 10. ✅ Comprobación 8 — Cuando el determinismo se rompe: funciones no puras

Este experimento demuestra, a propósito, el matiz importante mencionado en el manual: Spark garantiza reproducir **la misma secuencia de pasos**, pero no puede garantizar el mismo **contenido** si tu función no es determinista.

Crea `apps/seccion3/08_no_determinista.py`:

```python
from pyspark.sql import SparkSession
import random

spark = SparkSession.builder.appName("NoDeterminista").master("spark://spark-master:7077").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(range(0, 20), numSlices=4)

# OJO: esta función usa random SIN semilla fija -> no es determinista
rdd_aleatorio = rdd.map(lambda x: (x, random.random()))

print("=== Primera materialización ===")
print(rdd_aleatorio.collect())

print("\n=== Segunda materialización (SIN cache, se recalcula desde cero) ===")
print(rdd_aleatorio.collect())

print("\nLos valores aleatorios de ambas listas casi con certeza serán DISTINTOS,")
print("aunque la secuencia de transformaciones ejecutada fue idéntica en ambos casos.")
print("Esto ilustra que el determinismo del DAG cubre 'qué pasos se repiten',")
print("no garantiza que TU función produzca el mismo contenido si no es pura.")

# Comparemos contra una versión CON semilla fija -> sí sería reproducible
random.seed(42)
rdd_con_semilla = rdd.map(lambda x: (x, random.Random(42 + x).random()))
print("\n=== Con semilla determinista basada en 'x' ===")
print("Primera vez:", rdd_con_semilla.collect())
print("Segunda vez:", rdd_con_semilla.collect())
print("Estas dos listas SÍ deberían ser idénticas.")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion3/08_no_determinista.py
```

**Comprobación esperada:**
- [ ] Las dos primeras listas (con `random.random()` sin semilla) **difieren** entre sí.
- [ ] Las dos últimas listas (con semilla fija derivada de `x`) son **idénticas** entre sí.
- [ ] Esto confirma en la práctica que el "determinismo" del DAG se refiere a la reproducción de la **secuencia de operaciones**, y que la responsabilidad de que el **contenido** sea reproducible recae en que tus propias funciones sean puras.

---

## 11. Troubleshooting

| Problema | Causa probable | Solución |
|---|---|---|
| La UI muestra Jobs antes de llamar a una Acción | Puede que estés viendo Jobs de un script anterior que no cerraste | Verifica el `applicationId` en la UI y confirma que corresponde al script actual |
| `docker restart spark-worker-2` no genera ningún efecto visible | El worker se recuperó demasiado rápido, antes de que Spark necesitara reasignar Tasks | Aumenta el `time.sleep()` en el script, o reinicia el worker justo cuando la Stage esté en pleno progreso (revisa la UI en tiempo real) |
| La Stage 1 en la Comprobación 5 no aparece como `Pending` sino que ya tiene Tasks completadas | El clúster tiene más cores libres de los esperados y adelantó parte del trabajo, o la UDF lenta no fue suficientemente lenta | Aumenta el `time.sleep(0.5)` dentro de la UDF, o reduce `spark.executor.cores` temporalmente para forzar más contención |
| `resultado_1 != resultado_2` en la Comprobación 7 | Revisa que no haya ninguna fuente de aleatoriedad escondida en tus transformaciones | Confirma que las funciones (`map`, `filter`) sean 100% deterministas antes de repetir la prueba |

---

## 12. Checklist final de validación

- [ ] Confirmaste que ninguna transformación (ni siquiera una Wide como `groupBy`) dispara un Job por sí sola — solo las Acciones lo hacen
- [ ] Verificaste con `.dependencies()` que `map`/`filter`/`flatMap` producen `OneToOneDependency` y que `groupByKey`/`reduceByKey`/`repartition` producen `ShuffleDependency`
- [ ] Localizaste las 4 representaciones del plan (`Parsed`, `Analyzed`, `Optimized`, `Physical`) con `.explain(extended=True)`
- [ ] Visualizaste el DAG completo en la UI, identificando recuadros de Stage y los puntos de shuffle entre ellos
- [ ] Observaste en tiempo real que una Stage permanece `Pending` mientras la anterior sigue `Active`, confirmando la ejecución secuencial de Stages
- [ ] Confirmaste que un bucle `for` genera una cadena lineal de nodos nuevos en el linaje, no un ciclo
- [ ] Provocaste un fallo real de un Executor y confirmaste que el resultado reconstruido es **idéntico** al original (determinismo del DAG)
- [ ] Reprodujiste el caso de una función no determinista (`random` sin semilla) y confirmaste que el determinismo del DAG no cubre el contenido de funciones impuras

Si completaste todos estos puntos, tienes una comprensión práctica y verificada de cómo Spark construye, ejecuta y recupera su Grafo Acíclico Dirigido — no solo la teoría, sino la evidencia observada directamente en tu propio clúster.
