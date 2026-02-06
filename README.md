# H1 - Sistema de Análisis de Promociones y Conversión E-commerce

## 🎯 ¿Qué es este proyecto?

Sistema de análisis de datos que procesa **eventos de Google Analytics 4** para detectar automáticamente el cumplimiento de promociones comerciales y analizar el comportamiento de compra en el sitio de SorteosTec. El pipeline identifica oportunidades de mejora en la conversión mediante el análisis del timing de autenticación y patrones de abandono de carrito.

## 💡 Problema de Negocio que Resuelve

### Contexto
SorteosTec maneja múltiples tipos de promociones (simples y combinadas) que requieren condiciones específicas para activarse. El negocio necesitaba:

1. **Visibilidad en tiempo real** sobre qué promociones están funcionando
2. **Identificar "near-misses"** - usuarios que casi completan una promoción (ej: compraron 4 boletos cuando necesitaban 5)
3. **Entender el impacto del momento de login** en la conversión
4. **Cuantificar oportunidades perdidas** por abandono de carrito con promociones activas

### Solución
Este pipeline automatiza la detección de patrones de promociones en tres etapas del funnel (add_to_cart, begin_checkout, purchase) y genera KPIs accionables para optimización de conversión.

## 🏗️ Arquitectura del Pipeline

### Flujo de Procesamiento
```
1. EXTRACCIÓN (30-60s)
   ├── GA4 Events (BigQuery)
   ├── Catálogo Sorteos
   ├── Condiciones Promociones
   └── Vigencia Promociones

2. ENRIQUECIMIENTO (20-30s)
   ├── Match productos con catálogo
   ├── Cálculo de precios unitarios
   └── Interpretación de condiciones

3. DETECCIÓN DE PATRONES (5-10min)
   ├── Por sesión completa
   │   └── Promociones combinadas
   └── Por producto individual
       └── Promociones simples

4. AGREGACIÓN (30-60s)
   ├── Nivel sesión
   ├── Categorización login
   └── Cálculo de KPIs

5. OUTPUT (1-2min)
   ├── CSV local (análisis)
   └── BigQuery (persistencia)
```

### Scripts Principales

| Script | Función | Tiempo Ejecución | Cuándo Usar |
|--------|---------|------------------|-------------|
| **H1Script.py** | Pipeline completo con recreación de tablas base | 15-30 min | Ejecución diaria programada |
| **H1ShortScript.py** | Solo análisis sobre datos existentes | 5-10 min | Re-análisis rápido, debugging |

## 🗂️ Estructura de Datos

### Queries SQL Utilizadas

| Query | Propósito | Tabla Resultante |
|-------|-----------|------------------|
| `base_patrones.sql` | Crear tabla canónica de intentos desde GA4 | `intentos_producto_canonico_web_*` |
| `ga4_events.sql` | Formatear eventos para procesamiento Python | DataFrame temporal |
| `sorteo.sql` | Catálogo de productos y precios | Lookup table |
| `condiciones_promocion.sql` | Reglas de activación de promociones | Condiciones enriquecidas |
| `promociones_combinadas.sql` | Promociones multi-producto | Requisitos combinados |
| `complemento_funnel.sql` | Análisis de sesiones y timing de login | `sesiones_funnel_lineal_web_*` |
| `procesamiento_patrones.sql` | Consolidación final con categorías | `patrones_y_funnel_web_*` |

### Lógica de Procesamiento

#### 1. Detección de Promociones Simples
```python
# Promoción COMPLETA si:
- Cumple condición exacta (ej: 3 boletos)
- Promoción vigente en fecha del evento
- Ocurre en etapa evaluada

# Promoción INCOMPLETA (near-miss) si:
- Promoción activa
- Cantidad = N-1 (un boleto menos)
```

#### 2. Detección de Promociones Combinadas
```python
# COMPLETA cuando:
- TODOS los productos requeridos están en sesión
- CADA producto cumple su cantidad mínima
- Promoción vigente

# Ejemplo: "2 boletos Sorteo A + 3 boletos Sorteo B"
```

#### 3. Categorización de Login
El sistema clasifica el momento exacto cuando el usuario se autentica:
- `SIN LOGIN EN SESIÓN` - Usuario anónimo
- `LOGIN ANTES DE ADD_TO_CART` - Login temprano (mejor conversión)
- `LOGIN ENTRE [ETAPA_X] Y [ETAPA_Y]` - Login durante proceso
- `LOGIN DESPUÉS DE BEGIN_CHECKOUT` - Login tardío (fricción)
- `LOGIN YA INICIADO` - Sesión previa activa

## 📈 Outputs Generados

### Archivos CSV
```
Data/CSV/
├── ga4_patrones_promociones.csv         # Detalle por producto-intento
└── ga4_patrones_funnel_completo.csv    # Análisis completo con montos
```

### Tablas BigQuery
```sql
-- Tabla principal de análisis
sorteostec-ml.h1.patrones_y_funnel_web_{YYYYMMDD}_{YYYYMMDD}

-- Campos clave:
├── Identificadores: user_pseudo_id, session_id, ITEM
├── Métricas: qty_*, MONTO_*, precio_unitario_inferido
├── Patrones: PATRON_*, PROMOS_*_COMPLETAS/INCOMPLETAS
├── Contexto: categoria_login, discount_seen_after_login
└── Partición: attempt_date (optimización de costos)
```

## 🚀 Instalación y Configuración

### Requisitos
- Python 3.12+
- 8GB RAM mínimo
- Credenciales Google Cloud con acceso a BigQuery
- ~5GB espacio para outputs

### Setup Rápido
```bash
# 1. Clonar repositorio
git clone [URL_REPO]
cd H1

# 2. Instalar dependencias (con uv)
uv venv
source .venv/bin/activate
uv pip install -r pyproject.toml

# 3. Configurar credenciales
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"

# 4. Ajustar rutas en scripts si es necesario
# Editar: CREDENTIALS_PATH_ML, LOG_DIR, OUTPUT_CSV_*

# 5. Ejecutar pipeline
python H1Script.py  # Completo
# o
python H1ShortScript.py  # Solo análisis
```

## 📊 Casos de Uso de los Resultados

### 1. Optimización de Promociones
- Identificar promociones con alta tasa de incompletitud → Ajustar umbrales
- Detectar canibalización entre promociones → Rediseñar ofertas
- Medir ROI real por tipo de promoción → Priorizar inversión

### 2. Recuperación de Carritos Abandonados
- Usuarios con near-miss → Campaña email con incentivo adicional
- Patrones incompletos recurrentes → Revisar UX del checkout
- Timing de abandono → Optimizar momento de mostrar promociones

### 3. Personalización
- Usuarios frecuentes con patrones completos → Programa VIP
- Comportamiento por categoría de login → Estrategias diferenciadas
- Análisis de cohortes → Segmentación avanzada

## 🔧 Mantenimiento

### Logs y Monitoreo
```bash
# Ver últimos logs
tail -f logs/h1Logs.log

# Buscar errores
grep ERROR logs/h1Logs.log

# Verificar ejecución exitosa
grep "FIN EXITOSO" logs/h1Logs.log
```

### Actualización de Período
```python
# En H1Script.py y H1ShortScript.py
# En H1Script.py
DATE_START = "2024-10-01"  
DATE_END   = "2026-01-31"  # Actualizar fecha fin. El sufijo de tablas se calcula automático.
```

### Performance
- **Tiempo esperado**: 2-3 horas para 1 año de datos
- **Memoria pico**: ....
- **Registros procesados**: ...

## ⚠️ Consideraciones Importantes

1. **Costos BigQuery**: Las queries usan particionamiento para minimizar costos
2. **Vigencia de Promociones**: Se validan automáticamente contra fecha del evento
3. **Deduplicación**: Los eventos GA4 se deduplican usando `event_server_timestamp_offset`
4. **Zona Horaria**: Todos los timestamps se convierten a hora de México (America/Mexico_City)

## 📚 Documentación Adicional

- [`docs/01_arquitectura.md`](docs/01_arquitectura.md) - Detalles técnicos de la arquitectura
- [`docs/02_modelo_datos.md`](docs/02_modelo_datos.md) - Modelo de datos y flujo de transformaciones
- [`docs/03_logica_negocio.md`](docs/03_logica_negocio.md) - Reglas de negocio implementadas
- [`docs/04_guia_desarrollo.md`](docs/04_guia_desarrollo.md) - Guía para desarrolladores
- [`docs/05_mantenimiento.md`](docs/05_mantenimiento.md) - Manual de operación


*Desarrollado por el equipo de Data Science/analytics - SorteosTec*  
*Última actualización: Febrero 2026*