# ⚡ Referencia Rápida - ETL Pipeline Builder

## 🖱️ **Menú Contextual (Clic Derecho en Nodos)**
- **🏷️ Cambiar Nombre** → Nombres personalizados
- **📋 Duplicar Nodo** → Copia configuración
- **🗑️ Eliminar Nodo** → Elimina solo este nodo

## 🔧 **Nuevos Nodos de Transformación**

### **Básicos**
| Nodo | Función | Uso Común |
|------|---------|-----------|
| **Sort** | Ordenar datos | Ordenar por fecha/monto |
| **Unique** | Eliminar duplicados | Limpiar datos únicos |
| **Limit** | Limitar filas | Muestras de datos |
| **Fill Nulls** | Rellenar nulos | Completar datos faltantes |
| **Drop Nulls** | Eliminar nulos | Limpiar registros incompletos |

### **Avanzados**
| Nodo | Función | Uso Común |
|------|---------|-----------|
| **Window** | Funciones de ventana | Rankings, lag/lead |
| **Pivot** | Filas → Columnas | Tablas dinámicas |
| **Unpivot** | Columnas → Filas | Normalizar datos |
| **Explode** | Expandir arrays | Listas en filas separadas |
| **Text Transform** | Manipular texto | Limpiar/formatear strings |
| **Date Transform** | Manipular fechas | Extraer año/mes/día |

## 💾 **Auto-guardado Activo**
- ✅ **Todos los campos** se guardan automáticamente
- ✅ **Estado persiste** al cambiar nodos
- ✅ **Datos se mantienen** al guardar/abrir
- ❌ **No necesitas** "Probar Conexión" repetidamente

## 🚀 **Flujo de Trabajo Rápido**

### **Crear Pipeline**
1. **Arrastra** nodo fuente → Configura → Obtén datos
2. **Arrastra** transformación → Conecta → Configura
3. **Arrastra** destino → Conecta → Configura
4. **Ejecuta** pipeline ✅

### **Duplicar Configuración**
1. **Clic derecho** en nodo configurado
2. **"Duplicar Nodo"**
3. **Renombrar** y ajustar según necesidad
4. **Conectar** en pipeline ✅

### **Organizar Proyecto**
1. **Cambiar nombres** descriptivos a todos los nodos
2. **Agrupar visualmente** nodos relacionados
3. **Guardar frecuentemente** el proyecto
4. **Usar duplicación** para variaciones ✅

## 🔍 **Solución Rápida de Problemas**

| Problema | Solución Rápida |
|----------|-----------------|
| "Tipo de origen desconocido" | Configura todos los campos del nodo |
| Datos se pierden | ✅ Ya resuelto - persisten automáticamente |
| Pipeline no ejecuta | Verifica configuración y conexiones |
| Nodo sin nombre claro | Clic derecho → "Cambiar Nombre" |

## 💡 **Atajos de Productividad**

- **Ctrl+S** → Guardar proyecto
- **Ctrl+O** → Abrir proyecto  
- **F5** → Ejecutar pipeline
- **Clic derecho** → Menú contextual completo
- **Duplicar** → Reutilizar configuraciones
- **Nombres descriptivos** → Identificación rápida

## 📊 **Casos de Uso Comunes**

### **Análisis de Ventas**
```
Datos Ventas → Sort (fecha) → Window (ranking) → CSV Export
```

### **Limpieza de Datos**
```
Datos Raw → Drop Nulls → Unique → Fill Nulls → Clean Data
```

### **Reporte Mensual**
```
DB Ventas → Date Transform (mes) → Pivot (productos) → Excel Export
```

### **Combinación de Fuentes**
```
Datos Clientes ↘
                 → Join (ID) → Aggregate → Dashboard
Datos Ventas   ↗
```

---

**💡 Tip**: ¡Experimenta con las nuevas funcionalidades! Todo se guarda automáticamente y puedes duplicar nodos para probar variaciones sin perder trabajo.
