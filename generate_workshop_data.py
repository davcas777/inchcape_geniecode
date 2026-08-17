# Databricks notebook source
# =============================================================================
# Genie Code Workshop — Generador de datos sintéticos (edición Inchcape)
# -----------------------------------------------------------------------------
# Dominio: distribución automotriz Inchcape en LatAm, con capa cruda estilo SAP.
#
# Crea 3 schemas en el catálogo objetivo:
#   <catalog>.inchcape_sap_raw  → tablas crudas estilo SAP (VBAK, VBAP, MARA, KNA1)
#   <catalog>.inchcape_gold     → tablas curadas de negocio (dealerships, ventas,
#                                 repuestos, servicio, KPIs)
#   <catalog>.inchcape_bronze / inchcape_silver → schemas vacíos para los ejercicios
#
# Siembra ~400 defectos de calidad a propósito (nulos, duplicados, huérfanos,
# rangos inválidos, inconsistencias) para los ejercicios de validación (tracks
# Data Engineering, PMO & BA y Power BI).
#
# USO:
#   - Ejecuta como notebook en Databricks con un cluster activo, o
#   - Súbelo con `databricks workspace import` y córrelo con `databricks jobs`.
#   Ajusta CATALOG a tu catálogo (por defecto dacascan_ws1 en el workspace de David;
#   en el workspace de Inchcape, cámbialo por el catálogo del cliente).
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql import Window

# Permite correr como notebook (spark ya existe) o local vía Databricks Connect.
try:
    spark  # type: ignore  # noqa: F821
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

# ----------------------------------------------------------------------------- CONFIG
CATALOG = "dacascan_ws1"          # <-- cámbialo por el catálogo de Inchcape en delivery
SAP_SCHEMA = "inchcape_sap_raw"
GOLD_SCHEMA = "inchcape_gold"
SEED = 42

spark.conf.set("spark.sql.shuffle.partitions", "8")

for sch in [SAP_SCHEMA, GOLD_SCHEMA, "inchcape_bronze", "inchcape_silver"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{sch}")

# ----------------------------------------------------------------------------- REFERENCE DATA
# Inchcape Americas: seis mercados LatAm donde distribuye vehículos.
COUNTRIES = [
    ("Colombia", "COP", ["Bogotá", "Medellín", "Cali", "Barranquilla"], "Andina"),
    ("Chile",    "CLP", ["Santiago", "Concepción", "Antofagasta"],       "Cono Sur"),
    ("Perú",     "PEN", ["Lima", "Arequipa", "Trujillo"],                "Andina"),
    ("Ecuador",  "USD", ["Quito", "Guayaquil"],                          "Andina"),
    ("Panamá",   "USD", ["Ciudad de Panamá", "David"],                   "Centroamérica"),
    ("Costa Rica","USD",["San José", "Alajuela"],                        "Centroamérica"),
]
OEM_BRANDS = ["Toyota", "Suzuki", "Subaru", "BMW", "Jaguar Land Rover", "Geely", "Great Wall"]
DEALER_TYPES = ["Flagship", "Standard", "Service-Only", "Used-Car"]
SEGMENTS = ["SUV", "Sedán", "Pickup", "EV", "Van", "Hatchback"]
SERVICE_TYPES = ["Mantenimiento", "Garantía", "Reparación", "Campaña/Recall"]
PART_CATEGORIES = ["Frenos", "Filtros", "Lubricantes", "Eléctrico", "Carrocería", "Motor", "Llantas"]

# ============================================================================= 1. dim_dealership
rows = []
did = 1
for (country, ccy, cities, region) in COUNTRIES:
    for city in cities:
        n = 5 if city in ("Bogotá", "Santiago", "Lima", "Ciudad de Panamá") else 3
        for _ in range(n):
            code = f"DLR-{country[:2].upper()}-{did:04d}"
            rows.append((
                did, code, f"Inchcape {city} {DEALER_TYPES[did % 4]}",
                country, city, region, DEALER_TYPES[did % 4],
                OEM_BRANDS[did % len(OEM_BRANDS)],
            ))
            did += 1

dealer_df = spark.createDataFrame(
    rows,
    "dealer_id int, dealer_code string, dealer_name string, country string, "
    "city string, region string, dealer_type string, oem_brand string",
)
dealer_df = (
    dealer_df
    .withColumn("opened_date", F.expr("date_add('2005-01-01', cast(rand(1)*6000 as int))"))
    .withColumn("latitude", F.round(F.expr("rand(2)*40 - 30"), 5))
    .withColumn("longitude", F.round(F.expr("rand(3)*40 - 85"), 5))
)

# --- Defectos sembrados: ~14 regiones NULL, 5 dealer_id duplicados, ~5 coords (0,0)
dealer_df = dealer_df.withColumn(
    "region", F.when(F.col("dealer_id") % 4 == 0, F.lit(None)).otherwise(F.col("region"))
)
dealer_df = dealer_df.withColumn(
    "latitude", F.when(F.col("dealer_id") % 11 == 0, F.lit(0.0)).otherwise(F.col("latitude"))
).withColumn(
    "longitude", F.when(F.col("dealer_id") % 11 == 0, F.lit(0.0)).otherwise(F.col("longitude"))
)
dupes = dealer_df.orderBy("dealer_id").limit(5)  # 5 filas duplicadas
dealer_df = dealer_df.unionByName(dupes)

(dealer_df.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_dealership"))
spark.sql(f"COMMENT ON TABLE {CATALOG}.{GOLD_SCHEMA}.dim_dealership IS "
          f"'Dimensión de concesionarios Inchcape por país/ciudad/marca OEM'")
DEALER_COUNT = dealer_df.select("dealer_id").distinct().count()
print(f"dim_dealership: {dealer_df.count()} filas ({DEALER_COUNT} dealers únicos)")

VALID_DEALERS = [r[0] for r in dealer_df.select("dealer_id").distinct().collect()]

# ============================================================================= 2. fact_vehicle_sales
N_SALES = 150000
sales = (
    spark.range(N_SALES).withColumnRenamed("id", "row_id")
    .withColumn("sale_id", F.concat(F.lit("VS-"), F.lpad(F.col("row_id").cast("string"), 8, "0")))
    .withColumn("sale_date", F.expr("date_add('2024-01-01', cast(rand(10)*730 as int))"))
    .withColumn("dealer_id", F.expr(f"cast(rand(11)*{DEALER_COUNT} as int) + 1"))
    .withColumn("vin", F.concat(F.lit("VIN"), F.lpad((F.col("row_id")*7 % 99999999).cast("string"), 8, "0")))
    .withColumn("oem_brand", F.element_at(F.array(*[F.lit(b) for b in OEM_BRANDS]), (F.expr("cast(rand(12)*7 as int)")+1)))
    .withColumn("segment", F.element_at(F.array(*[F.lit(s) for s in SEGMENTS]), (F.expr("cast(rand(13)*6 as int)")+1)))
    .withColumn("model", F.concat(F.col("oem_brand"), F.lit(" "), F.col("segment")))
    .withColumn("units", F.lit(1))
    .withColumn("sale_price_usd", F.round(F.expr("15000 + rand(14)*60000"), 2))
    .withColumn("vehicle_cost_usd", F.round(F.col("sale_price_usd") * F.expr("0.80 + rand(15)*0.12"), 2))
    .withColumn("finance_attached", F.expr("rand(16) < 0.55"))
    .withColumn("salesperson_id", F.concat(F.lit("SP-"), F.lpad(F.expr("cast(rand(17)*400 as int)").cast("string"), 4, "0")))
    .withColumn("days_to_deliver", F.expr("cast(3 + rand(18)*45 as int)"))
    .drop("row_id")
)
# --- Defectos: ~60 dealers huérfanos (DLR-ZZ-9*), precios <=0, sale_id duplicados
orphan = (
    sales.limit(60)
    .withColumn("dealer_id", F.lit(99999))       # dealer_id inexistente
    .withColumn("sale_id", F.concat(F.lit("VS-ZZ"), F.lpad(F.monotonically_increasing_id().cast("string"), 6, "0")))
)
sales = sales.withColumn(
    "sale_price_usd",
    F.when(F.expr("rand(19) < 0.0005"), F.lit(0.0)).otherwise(F.col("sale_price_usd"))
)
sales = sales.unionByName(orphan)
dup_sales = sales.limit(40)                        # 40 sale_id duplicados
sales = sales.unionByName(dup_sales)

(sales.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_vehicle_sales"))
print(f"fact_vehicle_sales: {sales.count()} filas")

# ============================================================================= 3. fact_service_orders
N_SVC = 120000
svc = (
    spark.range(N_SVC).withColumnRenamed("id", "row_id")
    .withColumn("service_order_id", F.concat(F.lit("SO-"), F.lpad(F.col("row_id").cast("string"), 8, "0")))
    .withColumn("order_date", F.expr("date_add('2024-01-01', cast(rand(20)*730 as int))"))
    .withColumn("dealer_id", F.expr(f"cast(rand(21)*{DEALER_COUNT} as int) + 1"))
    .withColumn("vin", F.concat(F.lit("VIN"), F.lpad((F.col("row_id")*13 % 99999999).cast("string"), 8, "0")))
    .withColumn("service_type", F.element_at(F.array(*[F.lit(s) for s in SERVICE_TYPES]), (F.expr("cast(rand(22)*4 as int)")+1)))
    .withColumn("labor_hours", F.round(F.expr("0.5 + rand(23)*8"), 1))
    .withColumn("parts_cost_usd", F.round(F.expr("rand(24)*900"), 2))
    .withColumn("labor_cost_usd", F.round(F.col("labor_hours") * 45, 2))
    .withColumn("total_usd", F.round(F.col("parts_cost_usd") + F.col("labor_cost_usd"), 2))
    .withColumn("csi_score", F.expr("cast(3 + rand(25)*2 as int)"))   # 3..5
    .withColumn("status", F.when(F.expr("rand(26) < 0.85"), F.lit("Closed"))
                            .when(F.expr("rand(26) < 0.95"), F.lit("Open"))
                            .otherwise(F.lit("Cancelled")))
    .drop("row_id")
)
# --- Defectos: total != parts+labor en ~200 filas (para checks de consistencia PMO),
#     csi_score fuera de rango (>5) en ~50 filas
svc = svc.withColumn(
    "total_usd",
    F.when(F.expr("rand(27) < 0.0017"), F.round(F.col("total_usd") * 1.25, 2)).otherwise(F.col("total_usd"))
)
svc = svc.withColumn(
    "csi_score",
    F.when(F.expr("rand(28) < 0.0004"), F.lit(7)).otherwise(F.col("csi_score"))
)
(svc.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_service_orders"))
print(f"fact_service_orders: {svc.count()} filas")

# ============================================================================= 4. fact_parts_inventory
N_PARTS = 80000
parts = (
    spark.range(N_PARTS).withColumnRenamed("id", "row_id")
    .withColumn("snapshot_date", F.expr("date_add('2026-01-01', cast(rand(30)*180 as int))"))
    .withColumn("dealer_id", F.expr(f"cast(rand(31)*{DEALER_COUNT} as int) + 1"))
    .withColumn("part_number", F.concat(F.lit("PN-"), F.lpad((F.col("row_id") % 12000).cast("string"), 6, "0")))
    .withColumn("part_category", F.element_at(F.array(*[F.lit(c) for c in PART_CATEGORIES]), (F.expr("cast(rand(32)*7 as int)")+1)))
    .withColumn("on_hand_qty", F.expr("cast(rand(33)*300 as int)"))
    .withColumn("reorder_point", F.expr("cast(20 + rand(34)*60 as int)"))
    .withColumn("unit_cost_usd", F.round(F.expr("5 + rand(35)*500"), 2))
    .withColumn("last_movement_date", F.expr("date_add('2025-06-01', cast(rand(36)*400 as int))"))
    .withColumn("obsolete_flag", F.expr("rand(37) < 0.12"))
    .drop("row_id")
)
# --- Defecto: piezas marcadas obsoletas pero con movimiento reciente (inconsistencia)
parts = parts.withColumn(
    "last_movement_date",
    F.when(F.col("obsolete_flag") & (F.expr("rand(38) < 0.15")),
           F.expr("date_add('2026-06-01', cast(rand(39)*40 as int))")).otherwise(F.col("last_movement_date"))
)
(parts.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_parts_inventory"))
print(f"fact_parts_inventory: {parts.count()} filas")

# ============================================================================= 5. fact_daily_kpis
# KPIs agregados por país × región × fecha derivados de las ventas y servicio.
dealer_dim = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_dealership").dropDuplicates(["dealer_id"])
sales_j = (spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_vehicle_sales")
           .filter("dealer_id <> 99999")
           .join(dealer_dim.select("dealer_id", "country", "region"), "dealer_id"))
kpi_sales = (sales_j.groupBy(F.col("sale_date").alias("kpi_date"), "country", "region")
             .agg(F.sum("sale_price_usd").alias("vehicle_revenue_usd"),
                  F.sum("units").alias("units_sold"),
                  F.round((F.sum("sale_price_usd") - F.sum("vehicle_cost_usd")) / F.sum("sale_price_usd") * 100, 2).alias("gross_margin_pct")))
svc_j = (spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_service_orders")
         .join(dealer_dim.select("dealer_id", "country", "region"), "dealer_id"))
kpi_svc = (svc_j.groupBy(F.col("order_date").alias("kpi_date"), "country", "region")
           .agg(F.sum("labor_cost_usd").alias("service_revenue_usd"),
                F.sum("parts_cost_usd").alias("parts_revenue_usd"),
                F.round(F.avg("csi_score"), 2).alias("csi_avg")))
kpis = (kpi_sales.join(kpi_svc, ["kpi_date", "country", "region"], "outer")
        .fillna(0, ["vehicle_revenue_usd", "units_sold", "service_revenue_usd", "parts_revenue_usd"])
        .withColumn("yoy_growth_pct", F.round(F.expr("rand(40)*30 - 5"), 2)))
(kpis.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_daily_kpis"))
print(f"fact_daily_kpis: {kpis.count()} filas")

# ============================================================================= 6. SAP RAW (VBAK/VBAP/MARA/KNA1)
# Tablas crudas con nombres técnicos SAP para el ejercicio "automatiza la creación
# de tablas desde SAP con Genie Code, incluyendo generación de campos técnicos".

# --- KNA1 (Maestro de clientes)
N_KNA1 = 5000
kna1 = (
    spark.range(N_KNA1)
    .withColumn("MANDT", F.lit("100"))
    .withColumn("KUNNR", F.lpad(F.col("id").cast("string"), 10, "0"))
    .withColumn("NAME1", F.concat(F.lit("Cliente "), F.col("id").cast("string")))
    .withColumn("LAND1", F.element_at(F.array(F.lit("CO"), F.lit("CL"), F.lit("PE"), F.lit("EC"), F.lit("PA"), F.lit("CR")), (F.expr("cast(rand(50)*6 as int)")+1)))
    .withColumn("ORT01", F.lit("N/A"))
    .withColumn("REGIO", F.lpad(F.expr("cast(rand(51)*30 as int)").cast("string"), 2, "0"))
    .withColumn("PSTLZ", F.lpad(F.expr("cast(rand(52)*99999 as int)").cast("string"), 6, "0"))
    .drop("id")
)
(kna1.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SAP_SCHEMA}.kna1"))
spark.sql(f"COMMENT ON TABLE {CATALOG}.{SAP_SCHEMA}.kna1 IS 'SAP KNA1 — General Customer Master (raw)'")

# --- MARA (Maestro de materiales)
N_MARA = 2000
mara = (
    spark.range(N_MARA)
    .withColumn("MANDT", F.lit("100"))
    .withColumn("MATNR", F.lpad(F.col("id").cast("string"), 18, "0"))
    .withColumn("MTART", F.element_at(F.array(F.lit("FERT"), F.lit("HAWA"), F.lit("DIEN")), (F.expr("cast(rand(53)*3 as int)")+1)))
    .withColumn("MATKL", F.lpad(F.expr("cast(rand(54)*50 as int)").cast("string"), 4, "0"))
    .withColumn("MEINS", F.lit("EA"))
    .withColumn("BRGEW", F.round(F.expr("rand(55)*2000"), 3))
    .withColumn("NTGEW", F.round(F.expr("rand(56)*1800"), 3))
    .withColumn("MAKTX", F.concat(F.lit("Material "), F.col("id").cast("string")))
    .drop("id")
)
(mara.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SAP_SCHEMA}.mara"))
spark.sql(f"COMMENT ON TABLE {CATALOG}.{SAP_SCHEMA}.mara IS 'SAP MARA — General Material Data (raw)'")

# --- VBAK (Cabecera de documento de ventas)
N_VBAK = 20000
vbak = (
    spark.range(N_VBAK)
    .withColumn("MANDT", F.lit("100"))
    .withColumn("VBELN", F.lpad(F.col("id").cast("string"), 10, "0"))
    .withColumn("ERDAT", F.expr("date_add('2024-01-01', cast(rand(60)*730 as int))"))
    .withColumn("ERNAM", F.concat(F.lit("USER"), F.lpad(F.expr("cast(rand(61)*50 as int)").cast("string"), 3, "0")))
    .withColumn("AUART", F.element_at(F.array(F.lit("TA"), F.lit("OR"), F.lit("RE")), (F.expr("cast(rand(62)*3 as int)")+1)))
    .withColumn("NETWR", F.round(F.expr("10000 + rand(63)*70000"), 2))
    .withColumn("WAERK", F.lit("USD"))
    .withColumn("VKORG", F.element_at(F.array(F.lit("CO01"), F.lit("CL01"), F.lit("PE01"), F.lit("EC01"), F.lit("PA01"), F.lit("CR01")), (F.expr("cast(rand(64)*6 as int)")+1)))
    .withColumn("KUNNR", F.lpad(F.expr(f"cast(rand(65)*{N_KNA1} as int)").cast("string"), 10, "0"))
    .withColumn("VDATU", F.expr("date_add('2024-01-05', cast(rand(66)*730 as int))"))
    .drop("id")
)
(vbak.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SAP_SCHEMA}.vbak"))
spark.sql(f"COMMENT ON TABLE {CATALOG}.{SAP_SCHEMA}.vbak IS 'SAP VBAK — Sales Document Header Data (raw)'")

# --- VBAP (Ítems del documento de ventas) — ~3 ítems por cabecera
vbap = (
    vbak.select("MANDT", "VBELN").crossJoin(spark.range(3).withColumnRenamed("id", "posidx"))
    .withColumn("POSNR", F.lpad((F.col("posidx") * 10 + 10).cast("string"), 6, "0"))
    .withColumn("MATNR", F.lpad(F.expr(f"cast(rand(70)*{N_MARA} as int)").cast("string"), 18, "0"))
    .withColumn("ARKTX", F.concat(F.lit("Posición "), F.col("POSNR")))
    .withColumn("KWMENG", F.expr("cast(1 + rand(71)*5 as int)"))
    .withColumn("NETWR", F.round(F.expr("3000 + rand(72)*25000"), 2))
    .withColumn("WERKS", F.element_at(F.array(F.lit("P100"), F.lit("P200"), F.lit("P300")), (F.expr("cast(rand(73)*3 as int)")+1)))
    .withColumn("MEINS", F.lit("EA"))
    .drop("posidx")
)
(vbap.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SAP_SCHEMA}.vbap"))
spark.sql(f"COMMENT ON TABLE {CATALOG}.{SAP_SCHEMA}.vbap IS 'SAP VBAP — Sales Document Item Data (raw)'")

print("SAP raw:", spark.table(f"{CATALOG}.{SAP_SCHEMA}.vbak").count(), "VBAK /",
      spark.table(f"{CATALOG}.{SAP_SCHEMA}.vbap").count(), "VBAP /",
      spark.table(f"{CATALOG}.{SAP_SCHEMA}.mara").count(), "MARA /",
      spark.table(f"{CATALOG}.{SAP_SCHEMA}.kna1").count(), "KNA1")

print("\n✅ Datos del workshop Inchcape generados en",
      f"{CATALOG}.{GOLD_SCHEMA} + {CATALOG}.{SAP_SCHEMA}")
