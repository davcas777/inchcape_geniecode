---
customer: Inchcape
type: workshop
status: active
created: 2026-08-16
owner: David Cascante
audience: Inchcape Colombia — 5 equipos (Data Engineering, Data Science, Apps Dev, PMO & BA, Power BI/BI Devs)
duration: 2 días (Día 1 = Fundamentos para todos · Día 2 = track por equipo)
delivery_date: 2026-08-26
locations: Bogotá (~53 asistentes) + Medellín (~20 asistentes)
repo: https://github.com/davcas777/inchcape_geniecode
---

# Genie Code Workshop — Inchcape

Workshop práctico de **Genie Code dentro de Databricks**, re-skineado por completo para **Inchcape** (distribución automotriz en LatAm), con datos sintéticos de **ventas de vehículos, servicio, repuestos y una capa cruda estilo SAP**. Todo el contenido está centrado en **Genie Code**, porque la mayoría de lo que pidieron los equipos se resuelve con esa herramienta.

- **Fecha de entrega:** 2026-08-26 (Bogotá + Medellín)
- **Repo destino:** https://github.com/davcas777/inchcape_geniecode
- **App en vivo (workspace de David):** https://inchcape-geniecode-workshop-520209755093735.15.azure.databricksapps.com _(detrás de SSO del workspace)_
- **Estado:** catálogo autocontenido `inchcape_workshop` (creado por el generador) · **42/42 prompts validados** · app desplegada y RUNNING · workshop de 2 días (Día 1 fundamentos + Día 2 por equipo).

## Objetivo
Que cada uno de los 5 equipos de Inchcape aprenda a usar Genie Code para acelerar su trabajo diario, cubriendo a alto nivel los temas que solicitaron, con prompts probados que funcionan sobre datos sintéticos del dominio automotriz + SAP.

## Estructura: 2 días, 6 tracks, 61 pasos

**Día 1 — Fundamentos (todos los equipos juntos):** base común de Genie Code y Databricks antes de dividirse por equipo.

**Día 2 — Un track por equipo:** cada equipo profundiza en sus casos de uso. Todo pasa por Genie Code.

| Track | Día | Equipo | Pasos | Cubre |
|-------|-----|--------|-------|-------|
| **Fundamentos** | 1 | Todos | 12 | Activar Genie Code · navegar Unity Catalog · explorar datos en lenguaje natural · joins/agregaciones · notebooks (SQL+Python) · primer pipeline Delta · visualización · depurar · escribir buenos prompts · intro AI/BI + Genie Spaces · intro Databricks Apps |
| **Data Engineering** | 2 | Data Engineering | 10 | Automatizar tablas desde SAP + campos técnicos · optimizar lecturas/escrituras (Liquid Clustering) · Unity Catalog gobierno · Data Products · build-an-app (GBS/SAP) · reconciliación · Lakeflow Jobs · documentación automática |
| **Data Science & ML** | 2 | Data Science | 10 | Genie Code para apps · MLflow lifecycle · debugging en test · DAB + DBConnect · predicciones como Data Product · feature engineering · comparación de modelos · batch scoring + drift |
| **Apps Development** | 2 | Apps Dev | 10 | Despliegue seguro (local→Databricks) · web scraping seguro · build-an-app end-to-end + prereqs admin · Model Serving · DAB + DBConnect · auth/roles · logging/observabilidad · app con Genie embebido |
| **PMO & BA** | 2 | PMO & BA | 9 | Validación/consistencia · AI/BI dashboards · Genie ONE · Genie AGENTS · apps internas · checks avanzados (frescura/integridad) · tablero de alertas · exportar/compartir reporte |
| **Power BI / BI Devs** | 2 | Power BI / BI | 10 | Genie Code para SQL de reportes · validar campos · dashboards AI/BI · Unity Catalog gobierno · app ligera · Genie AGENTS · DAX→SQL · Metric Views · validaciones avanzadas |

> **Decisión de estructura:** partimos del app original (4 tracks genéricos) y lo reorganizamos a un **Día 1 de Fundamentos común** + **5 tracks alineados a los 5 equipos** de Inchcape (Día 2). El Día 1 nivela a todos en Genie Code + Databricks; el Día 2 cada equipo profundiza. Se agregaron reps (mismo nivel alto) para llenar los 2 días. Los temas transversales (build-an-app, DAB/DBConnect, Data Mesh, UC) aparecen donde cada equipo los pidió.

## Datos del dominio
Se generan con `generate_workshop_data.py`, que **crea el catálogo `inchcape_workshop`** y todos los schemas/tablas por sí mismo — no necesitas nada preexistente (ideal para un workspace Free Edition desde cero). Todos los prompts de la app referencian exactamente este catálogo. Si cambias la constante `CATALOG` del script, haz el mismo find-replace en `data/tracks.json`.

**`inchcape_workshop.inchcape_gold`** (capa curada de negocio):

| Tabla | Descripción |
|-------|-------------|
| `dim_dealership` | Concesionarios por país (CO/CL/PE/EC/PA/CR), ciudad, región, tipo y marca OEM. |
| `fact_vehicle_sales` | ~150k ventas de vehículos: VIN, marca, segmento, precio/costo USD, financiación, días de entrega. |
| `fact_service_orders` | ~120k órdenes de servicio (aftersales): tipo, horas, costo partes/labor, total, CSI. |
| `fact_parts_inventory` | ~80k snapshots de inventario de repuestos por concesionario. |
| `fact_daily_kpis` | KPIs agregados por país × región × fecha. |

**`inchcape_workshop.inchcape_sap_raw`** (capa cruda estilo SAP, para el ejercicio de automatización):

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
| `data/tracks.json` | Contenido de los 6 tracks (61 pasos): Fundamentos (Día 1) + 5 tracks por equipo (Día 2). Todo en español, prompts probados. |
| `frontend/index.html` | SPA con marca Inchcape (DM Sans, charcoal + teal). Lee de `/api/tracks`. |
| `frontend/img/*.svg` | Íconos de los 6 tracks (incl. Fundamentos) + logo Inchcape + Genie + símbolo Databricks. |
| `main.py` | Backend FastAPI (2 endpoints + mount estático). |
| `app.yaml`, `requirements.txt` | Configuración para Databricks Apps. |
| `test_prompts.py` | Script de validación que ejecuta la solución canónica de cada prompt que toca datos, contra las tablas sintéticas. **42/42 checks PASS.** |

## Preparación desde cero (incluye Databricks Free Edition)
Cada asistente arranca en su propio workspace nuevo. Todo lo necesario está en este repo.

1. **Clona el repo** (o descárgalo):
   ```bash
   git clone https://github.com/davcas777/inchcape_geniecode.git
   ```
2. **Sube el generador a tu workspace.** Importa `generate_workshop_data.py` como notebook: en la UI, Workspace → (tu carpeta) → Import → File, o con la CLI:
   ```bash
   databricks workspace import /Workspace/Users/<tu-usuario>/generate_workshop_data \
     --file generate_workshop_data.py --language PYTHON --format SOURCE --overwrite
   ```
3. **Ejecútalo** conectado a compute serverless. El script **crea el catálogo `inchcape_workshop`**, sus 4 schemas (`inchcape_gold`, `inchcape_sap_raw`, `inchcape_bronze`, `inchcape_silver`) y todas las tablas con los ~400 defectos. No necesitas crear nada a mano.
4. **(Solo si compartes el workspace) Permisos:** si otros usuarios usarán tu catálogo, otórgales `USE CATALOG inchcape_workshop` y `SELECT` sobre los schemas. En Free Edition, cada quien genera su propio catálogo y es dueño, así que normalmente no hace falta.
5. **Compute + Genie Code:** cada asistente necesita compute activo (serverless) con Genie Code habilitado (botón ✨ en el notebook).
6. **FMAPI (opcional):** algunos pasos (agentes de PMO/BI, asistente de apps) usan el endpoint `databricks-claude-sonnet-4`. Si no existe en tu workspace, ajusta el nombre del modelo o revisa solo el código generado.
7. **App de instrucciones:** despliega la app (abajo) y comparte la URL, o córrela local (`uvicorn main:app`). La app no necesita datos: solo sirve los prompts.

> **Todos los prompts referencian `inchcape_workshop.inchcape_gold` / `inchcape_workshop.inchcape_sap_raw`**, exactamente lo que crea el generador. No hay que editar nada para que coincidan.

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
