# Laboratorio de Pruebas: El Optimizador Catalyst — Ciclo de Vida de una Consulta

> Objetivo: usando el mismo clúster Spark Standalone de los laboratorios anteriores, comprobar en vivo cada una de las 4 fases de Catalyst: Análisis, Optimización Lógica, Planificación Física (con CBO) y Generación de Código.

## Índice

1. [Requisitos previos](#1-requisitos-previos)
2. [Preparar el entorno de este laboratorio](#2-preparar-el-entorno-de-este-laboratorio)
3. [✅ Comprobación 1 — Fase 1: `AnalysisException` antes de ejecutar nada](#3--comprobación-1--fase-1-analysisexception-antes-de-ejecutar-nada)
4. [✅ Comprobación 2 — Fase 1: comparar SQL vs DataFrame API](#4--comprobación-2--fase-1-comparar-sql-vs-dataframe-api)
5. [✅ Comprobación 3 — Fase 2: Predicate Pushdown en acción](#5--comprobación-3--fase-2-predicate-pushdown-en-acción)
6. [✅ Comprobación 4 — Fase 2: Column Pruning en acción](#6--comprobación-4--fase-2-column-pruning-en-acción)
7. [✅ Comprobación 5 — Fase 2: Constant Folding y Combine Filters](#7--comprobación-5--fase-2-constant-folding-y-combine-filters)
8. [✅ Comprobación 6 — Fase 3: CBO y estrategias de Join](#8--comprobación-6--fase-3-cbo-y-estrategias-de-join)
9. [✅ Comprobación 7 — Fase 3: forzando estrategias con hints](#9--comprobación-7--fase-3-forzando-estrategias-con-hints)
10. [✅ Comprobación 8 — Fase 4: identificar `*(n)` y fronteras de fusión](#10--comprobación-8--fase-4-identificar-n-y-fronteras-de-fusión)
11. [✅ Comprobación 9 — Fase 4: UDF de Python rompiendo la fusión](#11--comprobación-9--fase-4-udf-de-python-rompiendo-la-fusión)
12. [✅ Comprobación 10 — Ver el código Java generado realmente](#12--comprobación-10--ver-el-código-java-generado-realmente)
13. [Troubleshooting](#13-troubleshooting)
14. [Checklist final de validación](#14-checklist-final-de-validación)

---

## 1. Requisitos previos

Reutiliza el clúster Docker Standalone (`spark-master` + `spark-worker-1` + `spark-worker-2`) y los datos de `/opt/data/ventas_normal/` generados en el laboratorio de la Sección 1.

```bash
docker compose ps
ls data/ventas_normal/
```

---

## 2. Preparar el entorno de este laboratorio

```bash
mkdir -p apps/seccion4
```

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/apps/seccion4/nombre_del_script.py
```

---

## 3. ✅ Comprobación 1 — Fase 1: `AnalysisException` antes de ejecutar nada

Crea `apps/seccion4/01_fase1_analysis_exception.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Fase1Analysis").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")
df.createOrReplaceTempView("ventas")

print("Columnas reales de 'ventas':", df.columns)

print("\n=== Caso 1: columna con typo ===")
try:
    spark.sql("SELECT clientee_id FROM ventas").show()  # typo deliberado
except Exception as e:
    print(f"{type(e).__name__} detectada ANTES de ejecutar ningún Job.")
    print(str(e)[:200])

print("\n=== Caso 2: tabla inexistente ===")
try:
    spark.sql("SELECT * FROM tabla_fantasma").show()
except Exception as e:
    print(f"{type(e).__name__} detectada ANTES de ejecutar ningún Job.")
    print(str(e)[:200])

print("\n=== Caso 3: función inexistente ===")
try:
    spark.sql("SELECT FUNCION_INVENTADA(monto) FROM ventas").show()
except Exception as e:
    print(f"{type(e).__name__} detectada ANTES de ejecutar ningún Job.")
    print(str(e)[:200])

print("\nRevisa la UI (:4040 -> Jobs): NINGUNO de estos 3 casos debió generar un Job,")
print("porque el error ocurre en la Fase de Análisis, antes de cualquier ejecución física.")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/01_fase1_analysis_exception.py
```

**Comprobación esperada:**
- [ ] Los 3 casos lanzan `AnalysisException` (o subclase), capturada por el `try/except`.
- [ ] En la UI (`:4040` → "Jobs"), **ninguno** de los 3 casos generó un Job — confirma que el error se detectó antes de tocar datos.

---

## 4. ✅ Comprobación 2 — Fase 1: comparar SQL vs DataFrame API

Crea `apps/seccion4/02_sql_vs_dataframe_mismo_analyzer.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SQLvsDataFrame").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")
df.createOrReplaceTempView("ventas")

resultado_sql = spark.sql("SELECT categoria, SUM(monto) AS total FROM ventas WHERE monto > 100 GROUP BY categoria")
resultado_df = (
    spark.table("ventas")
    .filter(col("monto") > 100)
    .groupBy("categoria")
    .sum("monto")
)

print("=== Analyzed Logical Plan (camino SQL) ===")
resultado_sql.explain(True)

print("\n=== Analyzed Logical Plan (camino DataFrame) ===")
resultado_df.explain(True)

print("\nCompara ambas secciones '== Analyzed Logical Plan ==':")
print("deben ser estructuralmente equivalentes, confirmando que ambos caminos")
print("pasan por el MISMO Analyzer.")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/02_sql_vs_dataframe_mismo_analyzer.py
```

**Comprobación esperada:**
- [ ] Ambos `Analyzed Logical Plan` muestran la misma estructura (`Aggregate` sobre `Filter` sobre `Relation`), con los mismos nombres de columnas resueltas y tipos — solo puede variar el alias del resultado de la suma.

---

## 5. ✅ Comprobación 3 — Fase 2: Predicate Pushdown en acción

Crea `apps/seccion4/03_fase2_predicate_pushdown.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PredicatePushdown").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")
resultado = df.filter((df.pais == "PE") & (df.monto > 300))

print("=== Physical Plan: busca 'PushedFilters' ===")
resultado.explain()

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/03_fase2_predicate_pushdown.py
```

**Comprobación esperada:**
- [ ] En la línea `FileScan parquet [...]`, aparece `PushedFilters: [IsNotNull(pais), EqualTo(pais,PE), IsNotNull(monto), GreaterThan(monto,300.0)]` (o equivalente) — confirmando que ambos filtros se empujaron hasta la lectura de Parquet.

---

## 6. ✅ Comprobación 4 — Fase 2: Column Pruning en acción

Crea `apps/seccion4/04_fase2_column_pruning.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ColumnPruning").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")
print("Columnas totales disponibles:", df.columns)

resultado = df.select("categoria", "monto").filter(df.monto > 100)

print("\n=== Physical Plan: busca 'ReadSchema' ===")
resultado.explain()

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/04_fase2_column_pruning.py
```

**Comprobación esperada:**
- [ ] `df.columns` muestra **más** de 2 columnas (ej. `id`, `cliente_id`, `categoria`, `pais`, `monto`).
- [ ] En el plan físico, `ReadSchema: struct<categoria:string,monto:double>` — **solo** las 2 columnas usadas, confirmando que las demás nunca se leyeron.

---

## 7. ✅ Comprobación 5 — Fase 2: Constant Folding y Combine Filters

Crea `apps/seccion4/05_fase2_constant_folding.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ConstantFolding").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")

resultado = (
    df.filter(col("monto") > (30 * 2))       # candidato a Constant Folding: 30*2 -> 60
    .filter(col("pais") != "MX")               # candidato a Combine Filters
    .select("categoria", "monto", "pais")      # candidato a Column Pruning
)

print("=== Analyzed Logical Plan (tal cual escrito, 2 Filter separados, 30*2 sin evaluar) ===")
resultado.explain(True)

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/05_fase2_constant_folding.py
```

**Comprobación esperada, comparando `Analyzed Logical Plan` vs `Optimized Logical Plan` en la salida:**
- [ ] En `Analyzed`, ves **dos** nodos `Filter` separados y la expresión `(30 * 2)` sin evaluar.
- [ ] En `Optimized`, ves **un único** `Filter` combinando ambas condiciones con `AND`, y el valor `60.0` ya calculado en vez de `30 * 2`.

---

## 8. ✅ Comprobación 6 — Fase 3: CBO y estrategias de Join

Crea `apps/seccion4/06_fase3_cbo_joins.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CBOJoins").master("spark://spark-master:7077").getOrCreate()

# Tabla "grande": la ya generada en la Sección 1
ventas = spark.read.parquet("/opt/data/ventas_normal/")
ventas.write.mode("overwrite").saveAsTable("ventas_tabla")

# Tabla "pequeña": categorías
categorias = spark.createDataFrame(
    [("electro", "Electrónica"), ("hogar", "Hogar"), ("moda", "Moda"), ("deporte", "Deporte")],
    ["categoria", "nombre_largo"],
)
categorias.write.mode("overwrite").saveAsTable("categorias_tabla")

spark.sql("ANALYZE TABLE ventas_tabla COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE categorias_tabla COMPUTE STATISTICS FOR ALL COLUMNS")

print("=== DESCRIBE EXTENDED: revisa la fila 'Statistics' ===")
spark.sql("DESCRIBE EXTENDED categorias_tabla").filter("col_name = 'Statistics'").show(truncate=False)

print("\n=== Join automático (Spark debe elegir Broadcast, tabla pequeña) ===")
resultado = spark.table("ventas_tabla").join(spark.table("categorias_tabla"), "categoria")
resultado.explain()

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/06_fase3_cbo_joins.py
```

**Comprobación esperada:**
- [ ] `DESCRIBE EXTENDED` muestra una fila `Statistics` con bytes/filas reales calculados.
- [ ] En el plan físico del join, aparece `BroadcastHashJoin` y `BroadcastExchange` — confirmando que Spark detectó que `categorias_tabla` es pequeña y evitó el shuffle de `ventas_tabla`.

---

## 9. ✅ Comprobación 7 — Fase 3: forzando estrategias con hints

Crea `apps/seccion4/07_fase3_hints_join.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("HintsJoin").master("spark://spark-master:7077").getOrCreate()

print("=== Sin hint: Spark decide automáticamente ===")
auto = spark.table("ventas_tabla").join(spark.table("categorias_tabla"), "categoria")
auto.explain()

print("\n=== Forzando Sort-Merge Join con hint SQL ===")
spark.sql("""
    SELECT /*+ MERGE(c) */ v.*, c.nombre_largo
    FROM ventas_tabla v JOIN categorias_tabla c ON v.categoria = c.categoria
""").explain()

print("\n=== Confirmando Broadcast explícito con la función broadcast() ===")
forzado_broadcast = spark.table("ventas_tabla").join(broadcast(spark.table("categorias_tabla")), "categoria")
forzado_broadcast.explain()

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/07_fase3_hints_join.py
```

**Comprobación esperada:**
- [ ] El plan **sin hint** y el plan **con `broadcast()`** muestran ambos `BroadcastHashJoin` (coinciden, porque Spark ya iba a elegir eso automáticamente).
- [ ] El plan con **`/*+ MERGE(c) */`** muestra `SortMergeJoin` con `Exchange hashpartitioning` en ambos lados — confirmando que el hint **forzó** una estrategia distinta a la que el CBO hubiera elegido por defecto.

---

## 10. ✅ Comprobación 8 — Fase 4: identificar `*(n)` y fronteras de fusión

Crea `apps/seccion4/08_fase4_wholestage_codegen.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WholeStageCodeGen").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")

print("=== Cadena SIN shuffle: debe fusionarse toda en *(1) ===")
sin_shuffle = df.filter(df.monto > 100).select("categoria", "monto")
sin_shuffle.explain()

print("\n=== Cadena CON shuffle (groupBy): debe partirse en *(1) y *(2), separadas por Exchange ===")
con_shuffle = df.filter(df.monto > 100).groupBy("categoria").sum("monto")
con_shuffle.explain()

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/08_fase4_wholestage_codegen.py
```

**Comprobación esperada:**
- [ ] En la primera consulta, `FileScan`, `Filter` y `Project` comparten el **mismo** `*(1)`.
- [ ] En la segunda consulta, ves `*(1)` para el `FileScan`+`Filter`+agregación parcial, luego un `Exchange` **sin asterisco**, y finalmente `*(2)` para la agregación final — confirmando que el shuffle rompió la fusión en dos unidades distintas.

---

## 11. ✅ Comprobación 9 — Fase 4: UDF de Python rompiendo la fusión

Crea `apps/seccion4/09_fase4_udf_rompe_fusion.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType
import time

spark = SparkSession.builder.appName("UDFRompeFusion").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")

print("=== CAMINO 1: función nativa (mantiene Whole-Stage CodeGen) ===")
nativo = df.filter(df.monto > 100).withColumn("monto_ajustado", col("monto") * 1.18)
nativo.explain()

inicio = time.time()
nativo.count()
print(f"Tiempo camino nativo: {time.time() - inicio:.2f}s")

print("\n=== CAMINO 2: UDF de Python (rompe Whole-Stage CodeGen) ===")
@udf(returnType=DoubleType())
def ajustar_udf(monto):
    return monto * 1.18

con_udf = df.filter(df.monto > 100).withColumn("monto_ajustado", ajustar_udf(col("monto")))
con_udf.explain()

inicio = time.time()
con_udf.count()
print(f"Tiempo camino con UDF: {time.time() - inicio:.2f}s")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/09_fase4_udf_rompe_fusion.py
```

**Comprobación esperada:**
- [ ] En el **camino nativo**, todo el plan comparte `*(1)` (Filter + Project fusionados).
- [ ] En el **camino con UDF**, aparece un nodo `BatchEvalPython` **sin asterisco**, rompiendo la fusión en ese punto.
- [ ] Compara los tiempos: en datasets suficientemente grandes, el camino con UDF suele ser más lento, evidenciando el costo de la ruptura de fusión + serialización hacia el proceso Python.

---

## 12. ✅ Comprobación 10 — Ver el código Java generado realmente

Crea `apps/seccion4/10_fase4_ver_codigo_generado.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerCodigoGenerado").master("spark://spark-master:7077").getOrCreate()

df = spark.read.parquet("/opt/data/ventas_normal/")
resultado = df.filter(df.monto > 100).select("categoria", "monto")

print("=== Código Java generado por Whole-Stage CodeGen ===")
resultado.explain(mode="codegen")

spark.stop()
```

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/apps/seccion4/10_fase4_ver_codigo_generado.py
```

**Comprobación esperada:**
- [ ] La salida incluye código fuente Java real (métodos como `processNext()`), con la lógica del filtro (`if (monto > 100.0)`) y la proyección **inline**, sin llamadas a objetos `FilterExec`/`ProjectExec` separados.
- [ ] Confirma que este código es específico para esta consulta exacta (nombres de variables y condiciones que reflejan tu filtro y columnas concretas).

---

## 13. Troubleshooting

| Problema | Causa probable | Solución |
|---|---|---|
| No aparece `PushedFilters` en el plan | El formato de lectura no soporta pushdown, o el filtro no es sargable | Confirma que estás leyendo Parquet y usando comparaciones simples (`>`, `==`) |
| `ANALYZE TABLE` falla o no genera estadísticas visibles | La tabla no fue guardada como tabla administrada (`saveAsTable`) sino solo leída como DataFrame temporal | Usa `.write.saveAsTable(...)` antes de analizar, como en el script de la Comprobación 6 |
| El hint `/*+ MERGE(...) */` no cambia el plan | El alias en el hint no coincide con el alias usado en la consulta SQL | Verifica que el alias (`c` en el ejemplo) coincida exactamente con el usado en el `JOIN` |
| No ves diferencia de tiempo entre UDF nativa y UDF Python | Dataset demasiado pequeño para que el overhead de serialización se note | Aumenta el tamaño del dataset de prueba, o enfócate en la diferencia estructural del plan (`BatchEvalPython`), no solo en el tiempo |
| `.explain(mode="codegen")` no está disponible | Versión de Spark distinta a la esperada en el `docker-compose.yml` (usamos `bitnami/spark:3.5`) | Verifica la versión con `spark.version` dentro de un script |

---

## 14. Checklist final de validación

- [ ] Provocaste 3 tipos de `AnalysisException` (columna, tabla, función) y confirmaste que ninguno generó un Job en la UI
- [ ] Confirmaste que SQL y DataFrame API producen el mismo `Analyzed Logical Plan`
- [ ] Encontraste `PushedFilters` en un plan físico con múltiples condiciones
- [ ] Encontraste un `ReadSchema` reducido confirmando Column Pruning
- [ ] Comparaste `Analyzed` vs `Optimized Logical Plan` viendo Constant Folding y Combine Filters en acción
- [ ] Ejecutaste `ANALYZE TABLE` y confirmaste estadísticas reales con `DESCRIBE EXTENDED`
- [ ] Confirmaste que Spark elige automáticamente `BroadcastHashJoin` para una tabla pequeña
- [ ] Forzaste `SortMergeJoin` con un hint, contradiciendo la elección automática del CBO
- [ ] Identificaste el símbolo `*(n)` y confirmaste que un `Exchange` rompe la fusión en dos unidades
- [ ] Confirmaste que una UDF de Python aparece como `BatchEvalPython` sin asterisco, rompiendo Whole-Stage CodeGen
- [ ] Visualizaste el código Java real generado con `.explain(mode="codegen")`

Si completaste todos estos puntos, tienes evidencia práctica de las 4 fases completas del ciclo de vida de una consulta en Catalyst — no solo la teoría, sino la observación directa de cada mecanismo en tu propio clúster.
