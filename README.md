---
customer: Inchcape
type: workshop
status: active
created: 2026-08-16
owner: David Cascante
audience: Inchcape Colombia — 5 equipos (Data Engineering, Data Science, Apps Dev, PMO & BA, Power BI/BI Devs)
duration: ~1.5 h por track
delivery_date: 2026-08-26
locations: Bogotá (~53 asistentes) + Medellín (~20 asistentes)
repo: https://github.com/davcas777/inchcape_geniecode
---

# Genie Code Workshop — Inchcape

Workshop práctico de **Genie Code dentro de Databricks**, re-skineado por completo para **Inchcape** (distribución automotriz en LatAm), con datos sintéticos de **ventas de vehículos, servicio, repuestos y una capa cruda estilo SAP**. Todo el contenido está centrado en **Genie Code**, porque la mayoría de lo que pidieron los equipos se resuelve con esa herramienta.

- **Fecha de entrega:** 2026-08-26 (Bogotá + Medellín)
- **Repo destino:** https://github.com/davcas777/inchcape_geniecode
- **App en vivo (workspace de David):** https://inchcape-geniecode-workshop-520209755093735.15.azure.databricksapps.com _(detrás de SSO del workspace)_
- **Estado:** datos generados en `dacascan_ws1` · 29/29 prompts validados · app desplegada y RUNNING.

## Objetivo
Que cada uno de los 5 equipos de Inchcape aprenda a usar Genie Code para acelerar su trabajo diario, cubriendo a alto nivel los temas que solicitaron, con prompts probados que funcionan sobre datos sintéticos del dominio automotriz + SAP.

## Los 5 tracks (uno por equipo)
Cada track mapea directamente a lo que pidió ese equipo. Todo pasa por Genie Code.

| Track | Equipo | Pasos | Cubre (de lo que pidieron) |
|-------|--------|-------|-----------------------------|
| **Data Engineering** | Data Engineering | 7 | Automatizar tablas desde SAP + campos técnicos · optimizar lecturas/escrituras (Liquid Clustering) · Unity Catalog gobierno · Data Mesh / Data Products · Build an app con Genie Code (GBS/SAP) |
| **Data Science & ML** | Data Science | 7 | Casos con Genie Code para apps · ML/MLOps · debugging en test · DAB + DBConnect en VSCode · publicar predicciones como Data Product |
| **Apps Development** | Apps Dev | 7 | Despliegue seguro (local→Databricks) · web scraping seguro · Build an app con Genie Code (end-to-end + prereqs admin) · Model Serving · DAB + DBConnect |
| **PMO & BA** | PMO & BA | 6 | Validación/consistencia de datos · AI/BI dashboards · Genie ONE (Genie Space) · Genie AGENTS · apps internas sin código tradicional |
| **Power BI / BI Devs** | Power BI / BI | 7 | Genie Code para SQL de reportes · validar campos de reportes de mercado · dashboards AI/BI · Unity Catalog gobierno · app ligera · Genie AGENTS |

> **Decisión de estructura:** los 4 tracks del app original (Engineering/BI/DS/Governance) se reorganizaron a **5 tracks alineados a los 5 equipos** de Inchcape, porque cada equipo asiste a su propia sesión y los temas venían organizados por equipo. Los temas transversales (Build an app, DAB/DBConnect, Data Mesh, UC) aparecen en los tracks donde cada equipo los pidió.

## Datos del dominio
Se generan con `generate_workshop_data.py`. Catálogo por defecto: **`dacascan_ws1`** (workspace de David). En delivery en el workspace de Inchcape, cambia la constante `CATALOG` del script y haz find-replace de `dacascan_ws1` en `data/tracks.json`.

**`dacascan_ws1.inchcape_gold`** (capa curada de negocio):

| Tabla | Descripción |
|-------|-------------|
| `dim_dealership` | Concesionarios por país (CO/CL/PE/EC/PA/CR), ciudad, región, tipo y marca OEM. |
| `fact_vehicle_sales` | ~150k ventas de vehículos: VIN, marca, segmento, precio/costo USD, financiación, días de entrega. |
| `fact_service_orders` | ~120k órdenes de servicio (aftersales): tipo, horas, costo partes/labor, total, CSI. |
| `fact_parts_inventory` | ~80k snapshots de inventario de repuestos por concesionario. |
| `fact_daily_kpis` | KPIs agregados por país × región × fecha. |

**`dacascan_ws1.inchcape_sap_raw`** (capa cruda estilo SAP, para el ejercicio de automatización):

| Tabla | Equivalente SAP |
|-------|-----------------|
| `vbak` | Cabecera de documento de ventas (VBAK) |
| `vbap` | Ítems del documento de ventas (VBAP) |
| `mara` | Maestro de materiales (MARA) |
| `kna1` | Maestro de clientes (KNA1) |

Schemas `inchcape_bronze` / `inchcape_silver` se crean vacíos para los ejercicios.

### Defectos de calidad sembrados (~400)
Para los ejercicios de **validación/consistencia** (tracks DE, PMO y Power BI):
- `dim_dealership`: ~14 regiones NULL, 5 `dealer_id` duplicados, ~5 coordenadas (0,0). (~56 concesionarios únicos.)
- `fact_vehicle_sales`: ~60 ventas con `dealer_id` huérfano (99999), ~40 `sale_id` duplicados, algunos `sale_price_usd` = 0.
- `fact_service_orders`: ~200 filas con `total_usd` ≠ `parts_cost_usd + labor_cost_usd`, ~50 `csi_score` fuera de rango (=7).
- `fact_parts_inventory`: piezas marcadas obsoletas con movimiento reciente (inconsistencia).

## Branding
- **Paleta:** charcoal `#14161A` (primario), teal `#00A6A6` (acento), teal oscuro `#007C7B`, teal suave `#E0F5F4`.
- **Colores por track:** DE teal `#00A6A6`, DS violeta `#6D4AFF`, Apps naranja `#F26A1B`, PMO azul `#2D6CDF`, Power BI verde `#1FA971`.
- **Logo:** wordmark `frontend/img/inchcape-logo.svg` (recreado en SVG). **Nota:** los hex exactos de la marca Inchcape no son públicos; se usó una aproximación profesional (charcoal + teal). **Si tienes las guías de marca de Inchcape, reemplaza los hex en `index.html` (`:root`) y el logo por el oficial.**
- **Hero-art:** silueta abstracta de auto en teal + charcoal.

## Mapa de archivos
| Archivo | Propósito |
|---------|-----------|
| `generate_workshop_data.py` | Notebook/script PySpark que crea las 9 tablas (gold + SAP raw) con ~400 defectos. Ejecutar antes del workshop. |
| `data/tracks.json` | Contenido de los 5 tracks (34 pasos). Todo en español, prompts probados. |
| `frontend/index.html` | SPA con marca Inchcape (DM Sans, charcoal + teal). Lee de `/api/tracks`. |
| `frontend/img/*.svg` | Íconos de los 5 tracks + logo Inchcape + Genie + símbolo Databricks. |
| `main.py` | Backend FastAPI (2 endpoints + mount estático). |
| `app.yaml`, `requirements.txt` | Configuración para Databricks Apps. |
| `test_prompts.py` | Script de validación que ejecuta la solución canónica de cada prompt que toca datos, contra las tablas sintéticas. |

## Preparación previa al workshop
1. **Genera los datos:** ejecuta `generate_workshop_data.py` en el workspace (ver constante `CATALOG`).
2. **Permisos:** otorga a los asistentes `USE CATALOG` y `SELECT` sobre `inchcape_gold` e `inchcape_sap_raw`.
3. **Compute:** cada asistente necesita un cluster o SQL Warehouse activo con Genie Code habilitado.
4. **FMAPI:** algunos pasos (agentes PMO/BI) usan el endpoint `databricks-claude-sonnet-4`. Verifica que exista o ajústalo.
5. **App:** despliega la app de instrucciones (abajo) y comparte la URL con los asistentes.

## Despliegue de la app
```bash
# desde la carpeta del proyecto
databricks apps create inchcape-geniecode-workshop -p DEFAULT
databricks sync . /Workspace/Users/<user>/inchcape-geniecode-workshop -p DEFAULT
databricks apps deploy inchcape-geniecode-workshop \
  --source-code-path /Workspace/Users/<user>/inchcape-geniecode-workshop -p DEFAULT
```
La app no requiere binding de recursos: solo sirve contenido estático + JSON.

## Notas de diseño
- **Dominio:** distribución automotriz Inchcape en 6 mercados LatAm (Colombia, Chile, Perú, Ecuador, Panamá, Costa Rica). Moneda USD.
- **Contexto real:** Inchcape corre **SAP S/4HANA** con un equipo de **Global Business Services (GBS)**; plataforma objetivo **Azure Databricks** (serverless), con **Unity Catalog**, **AI/BI**, **Lakebase** y **data mesh** para un marketplace interno alimentado por SAP.
- **Foco:** todo pasa por Genie Code, por pedido explícito.
