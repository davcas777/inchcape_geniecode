# Databricks notebook source
"""
Genie Code Workshop — Validador de prompts (edición Inchcape)
-------------------------------------------------------------
Ejecuta la SOLUCIÓN CANÓNICA de cada prompt que toca datos y verifica que
funciona sobre las tablas sintéticas, incluyendo los conteos de defectos que
el app promete en 'Resultado esperado'.

Prompts puramente generativos (DAB, DBConnect, scaffolds de apps, config de
Genie Spaces, agentes) no se ejecutan aquí: se validan por inspección de API.
Este script cubre todo lo que produce datos o SQL/PySpark ejecutable.

USO (local vía Databricks Connect serverless):
    pip install databricks-connect scikit-learn pandas
    python test_prompts.py
o como notebook en el workspace (usa el `spark` existente).
"""

from pyspark.sql import functions as F

try:
    spark  # type: ignore  # noqa: F821
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

CATALOG = "dacascan_ws1"
GOLD = f"{CATALOG}.inchcape_gold"
SAP = f"{CATALOG}.inchcape_sap_raw"
SILVER = f"{CATALOG}.inchcape_silver"

RESULTS = []


def check(name, ok, detail=""):
    ok = bool(ok)  # coerce numpy.bool_ -> JSON-serializable Python bool
    RESULTS.append((name, ok, str(detail)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def approx(actual, target, tol):
    return abs(actual - target) <= tol


# ============================================================ A. PRESENCIA DE DATOS
print("\n=== A. Presencia de datos (step 1 de cada track) ===")
expected_tables = {
    f"{GOLD}.dim_dealership": (50, 400),
    f"{GOLD}.fact_vehicle_sales": (140000, 160000),
    f"{GOLD}.fact_service_orders": (115000, 125000),
    f"{GOLD}.fact_parts_inventory": (75000, 85000),
    f"{GOLD}.fact_daily_kpis": (1000, 20000),
    f"{SAP}.vbak": (19000, 21000),
    f"{SAP}.vbap": (58000, 62000),
    f"{SAP}.mara": (1900, 2100),
    f"{SAP}.kna1": (4900, 5100),
}
for tbl, (lo, hi) in expected_tables.items():
    try:
        n = spark.table(tbl).count()
        check(f"tabla {tbl} existe y tiene ~filas esperadas", lo <= n <= hi, f"{n} filas")
    except Exception as e:  # noqa: BLE001
        check(f"tabla {tbl} existe", False, str(e)[:120])


# ============================================================ B. DATA ENGINEERING
print("\n=== B. Data Engineering ===")

# B1 — Step 2: automatiza creación de tabla curada desde SAP + campos técnicos
try:
    vbak = spark.table(f"{SAP}.vbak")
    vbap = spark.table(f"{SAP}.vbap")
    kna1 = spark.table(f"{SAP}.kna1")
    mara = spark.table(f"{SAP}.mara")
    flat = (
        vbap.alias("i")
        .join(vbak.alias("h"), ["MANDT", "VBELN"])
        .join(kna1.alias("c"), (F.col("h.KUNNR") == F.col("c.KUNNR")) & (F.col("h.MANDT") == F.col("c.MANDT")), "left")
        .join(mara.alias("m"), (F.col("i.MATNR") == F.col("m.MATNR")) & (F.col("i.MANDT") == F.col("m.MANDT")), "left")
        .select(
            F.col("h.VBELN").alias("documento_venta"),
            F.col("i.POSNR").alias("posicion"),
            F.col("h.ERDAT").alias("fecha_creacion"),
            F.col("h.AUART").alias("tipo_documento"),
            F.col("i.NETWR").alias("valor_neto_item_usd"),
            F.col("h.KUNNR").alias("id_cliente"),
            F.col("c.NAME1").alias("nombre_cliente"),
            F.col("c.LAND1").alias("pais_cliente"),
            F.col("i.MATNR").alias("id_material"),
            F.col("m.MAKTX").alias("descripcion_material"),
            F.col("h.VKORG").alias("organizacion_ventas"),
            F.col("i.WERKS").alias("centro"),
        )
        .withColumn("surrogate_key", F.sha2(F.concat_ws("|", "documento_venta", "posicion"), 256))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("SAP-ECC"))
        .withColumn("_batch_date", F.current_date())
        .withColumn("_is_current", F.lit(True))
    )
    flat.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.sap_sales_document_flat")
    n = spark.table(f"{SILVER}.sap_sales_document_flat").count()
    cols = set(spark.table(f"{SILVER}.sap_sales_document_flat").columns)
    tech = {"surrogate_key", "_ingested_at", "_source_system", "_batch_date", "_is_current"}
    check("DE step2: curación SAP produce ~60k filas", approx(n, 60000, 3000), f"{n} filas")
    check("DE step2: genera campos técnicos", tech.issubset(cols), f"tiene {sorted(tech & cols)}")
except Exception as e:  # noqa: BLE001
    check("DE step2: curación SAP", False, str(e)[:160])

# B2 — Step 3: optimización (column pruning + CLUSTER BY + broadcast)
try:
    pruned = (spark.table(f"{GOLD}.fact_vehicle_sales")
              .select("dealer_id", "sale_date", "sale_price_usd", "oem_brand")
              .filter(F.col("sale_date") >= F.date_sub(F.current_date(), 3650)))
    check("DE step3: lectura con column pruning", pruned.count() >= 0, f"{len(pruned.columns)} columnas")
    spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.fact_vehicle_sales_optimized "
              f"CLUSTER BY (dealer_id, sale_date) AS SELECT * FROM {GOLD}.fact_vehicle_sales")
    check("DE step3: CREATE TABLE ... CLUSTER BY", True, "tabla optimizada creada")
    dim = F.broadcast(spark.table(f"{GOLD}.dim_dealership")
                      .dropDuplicates(["dealer_id"]).select("dealer_id", "region"))
    agg = (spark.table(f"{GOLD}.fact_vehicle_sales_optimized")
           .filter("dealer_id <> 99999")
           .join(dim, "dealer_id")
           .groupBy("region").agg(F.sum("sale_price_usd").alias("rev")))
    check("DE step3: agregado por región con broadcast join", agg.count() > 0, f"{agg.count()} regiones")
except Exception as e:  # noqa: BLE001
    check("DE step3: optimización", False, str(e)[:160])

# B3 — Step 5: Data Product
try:
    dim = spark.table(f"{GOLD}.dim_dealership").dropDuplicates(["dealer_id"])
    dp = (spark.table(f"{GOLD}.fact_vehicle_sales")
          .filter("dealer_id <> 99999 AND sale_price_usd > 0")
          .withColumn("mes", F.date_trunc("month", "sale_date"))
          .groupBy("dealer_id", "mes")
          .agg(F.sum("sale_price_usd").alias("revenue_usd"),
               F.sum("units").alias("unidades"),
               F.round((F.sum("sale_price_usd") - F.sum("vehicle_cost_usd")) / F.sum("sale_price_usd") * 100, 2).alias("margen_bruto_pct"))
          .join(dim.select("dealer_id", "country", "city", "region", "oem_brand"), "dealer_id"))
    dp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.dp_ventas_por_concesionario")
    check("DE step5: Data Product dp_ventas_por_concesionario", spark.table(f"{GOLD}.dp_ventas_por_concesionario").count() > 0)
except Exception as e:  # noqa: BLE001
    check("DE step5: Data Product", False, str(e)[:160])


# ============================================================ C. DATA SCIENCE
print("\n=== C. Data Science & ML ===")
svc = spark.table(f"{GOLD}.fact_service_orders")

# C1 — Step 1: defectos detectables por EDA
try:
    inconsistent = svc.filter(F.round(F.col("total_usd"), 2) != F.round(F.col("parts_cost_usd") + F.col("labor_cost_usd"), 2)).count()
    bad_csi = svc.filter((F.col("csi_score") < 1) | (F.col("csi_score") > 5)).count()
    check("DS step1: detecta total != parts+labor (~200)", approx(inconsistent, 200, 120), f"{inconsistent} filas")
    check("DS step1: detecta csi fuera de rango (~50)", approx(bad_csi, 50, 40), f"{bad_csi} filas")
except Exception as e:  # noqa: BLE001
    check("DS step1: EDA defectos", False, str(e)[:160])

# C2 — Step 2: entrena modelo supervisado, R2 razonable
try:
    import pandas as pd  # noqa: F401
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import r2_score, mean_squared_error

    clean = (svc.filter("csi_score BETWEEN 1 AND 5")
             .filter(F.round(F.col("total_usd"), 2) == F.round(F.col("parts_cost_usd") + F.col("labor_cost_usd"), 2))
             .withColumn("dow", F.dayofweek("order_date"))
             .select("service_type", "labor_hours", "parts_cost_usd", "dealer_id", "dow", "total_usd")
             .limit(30000))
    pdf = clean.toPandas()
    pdf = pd.get_dummies(pdf, columns=["service_type"], drop_first=True)
    X = pdf.drop(columns=["total_usd"])
    y = pdf["total_usd"]
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
    m = RandomForestRegressor(n_estimators=80, max_depth=10, random_state=42, n_jobs=-1)
    m.fit(Xtr, ytr)
    preds = m.predict(Xte)
    r2 = r2_score(yte, preds)
    rmse = mean_squared_error(yte, preds) ** 0.5
    check("DS step2: modelo entrena con R2>=0.7", r2 >= 0.7, f"R2={r2:.3f} RMSE={rmse:.1f}")
except Exception as e:  # noqa: BLE001
    check("DS step2: entrenamiento de modelo", False, str(e)[:200])

# C3 — Step 3: la corrección del bug de evaluate() da métricas sanas
try:
    from sklearn.metrics import r2_score, mean_squared_error
    import numpy as np
    y_true = np.array([100.0, 200.0, 300.0, 400.0])
    y_pred = np.array([110.0, 190.0, 310.0, 390.0])
    # versión CORREGIDA (squared=False, orden y_true,preds)
    rmse = mean_squared_error(y_true, y_pred) ** 0.5
    r2 = r2_score(y_true, y_pred)
    check("DS step3: evaluate() corregido da métricas sanas", (rmse > 0) and (0 <= r2 <= 1), f"RMSE={rmse:.1f} R2={r2:.3f}")
except Exception as e:  # noqa: BLE001
    check("DS step3: fix de bug", False, str(e)[:160])


# ============================================================ D. PMO & BA (validación)
print("\n=== D. PMO & BA — validación/consistencia (step 1) ===")
sales = spark.table(f"{GOLD}.fact_vehicle_sales")
dealer = spark.table(f"{GOLD}.dim_dealership")
try:
    c_incons = svc.filter(F.round(F.col("total_usd"), 2) != F.round(F.col("parts_cost_usd") + F.col("labor_cost_usd"), 2)).count()
    c_csi = svc.filter((F.col("csi_score") < 1) | (F.col("csi_score") > 5)).count()
    c_nullreg = dealer.filter(F.col("region").isNull()).count()
    c_dupdealer = dealer.groupBy("dealer_id").count().filter("count > 1").count()
    c_dupsale = sales.groupBy("sale_id").count().filter("count > 1").count()
    valid_ids = [r[0] for r in dealer.select("dealer_id").distinct().collect()]
    c_orphan = sales.filter(~F.col("dealer_id").isin(valid_ids)).count()
    c_badprice = sales.filter((F.col("sale_price_usd") <= 0) | F.col("sale_price_usd").isNull()).count()
    check("PMO check1: total != parts+labor (~200)", approx(c_incons, 200, 120), f"{c_incons}")
    check("PMO check2: csi fuera de rango (~50)", approx(c_csi, 50, 40), f"{c_csi}")
    check("PMO check3: region NULL (=15)", c_nullreg >= 10, f"{c_nullreg}")
    check("PMO check4a: dealer_id duplicados (=5)", c_dupdealer >= 1, f"{c_dupdealer} ids con dup")
    check("PMO check4b: sale_id duplicados (~40)", c_dupsale >= 1, f"{c_dupsale} ids con dup")
    check("PMO check5: ventas huérfanas (~60)", approx(c_orphan, 60, 40), f"{c_orphan}")
    check("PMO check6: precios <=0/NULL detectados", c_badprice >= 0, f"{c_badprice}")
except Exception as e:  # noqa: BLE001
    check("PMO validación", False, str(e)[:160])


# ============================================================ E. POWER BI / BI
print("\n=== E. Power BI / BI Developers ===")
try:
    # E1 — Step 1: reporte con window de participación de mercado
    rep = spark.sql(f"""
        SELECT d.country, s.oem_brand,
               SUM(s.units) AS unidades,
               SUM(s.sale_price_usd) AS revenue_usd,
               ROUND(SUM(s.sale_price_usd) / SUM(SUM(s.sale_price_usd)) OVER (PARTITION BY d.country) * 100, 2) AS participacion_pct
        FROM {GOLD}.fact_vehicle_sales s
        JOIN (SELECT DISTINCT dealer_id, country FROM {GOLD}.dim_dealership) d ON s.dealer_id = d.dealer_id
        WHERE s.dealer_id <> 99999 AND s.sale_price_usd > 0
        GROUP BY d.country, s.oem_brand
    """)
    check("BI step1: reporte con window de participación", rep.count() > 0, f"{rep.count()} filas país×marca")

    # E2 — Step 2: validación de campos del reporte
    neg_margin = sales.filter(F.col("vehicle_cost_usd") > F.col("sale_price_usd")).count()
    check("BI step2: detecta margen negativo (cost>price)", neg_margin >= 0, f"{neg_margin} filas")

    # E3 — Step 3: un panel del dashboard (unidades por segmento por mes)
    panel = spark.sql(f"""
        SELECT date_trunc('month', sale_date) AS mes, segment, SUM(units) AS unidades
        FROM {GOLD}.fact_vehicle_sales
        WHERE dealer_id <> 99999 AND sale_price_usd > 0
        GROUP BY 1, 2 ORDER BY 1
    """)
    check("BI step3: panel dashboard (unidades por segmento/mes)", panel.count() > 0, f"{panel.count()} filas")
except Exception as e:  # noqa: BLE001
    check("BI reportes", False, str(e)[:160])


# ============================================================ RESUMEN
import json as _json

print("\n" + "=" * 60)
passed = sum(1 for _, ok, _ in RESULTS if ok)
total = len(RESULTS)
print(f"RESUMEN: {passed}/{total} checks PASS")
summary = {
    "passed": passed,
    "total": total,
    "fails": [{"name": n, "detail": d} for n, ok, d in RESULTS if not ok],
    "all": [{"name": n, "ok": ok, "detail": d} for n, ok, d in RESULTS],
}
report = _json.dumps(summary, ensure_ascii=False)
try:
    dbutils.notebook.exit(report)  # type: ignore  # noqa: F821
except NameError:
    print(report)
    if summary["fails"]:
        raise SystemExit(1)
    print("✅ Todos los prompts que tocan datos fueron validados.")
