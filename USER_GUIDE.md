# 📖 Guía de Usuario - ETL Pipeline Builder

## 🚀 **Bienvenido a tu ETL Pipeline Builder Mejorado**

Esta guía te ayudará a aprovechar al máximo todas las nuevas funcionalidades implementadas en tu herramienta de construcción de pipelines ETL.

---

## 🎯 **Nuevas Funcionalidades Disponibles**

### 1. **🔧 Nodos de Transformación Avanzados**
### 2. **💾 Persistencia Automática de Estado**
### 3. **🖱️ Menú Contextual Mejorado**
### 4. **⚡ Auto-guardado Inteligente**

---

## 🔧 **1. Nodos de Transformación Avanzados**

### **📋 Nodos Básicos Disponibles**

#### **🔄 Sort (Ordenamiento)**
- **Función**: Ordena los datos por una o más columnas
- **Uso**: Clic derecho en paleta → Transformación → Sort
- **Configuración**: 
  - Selecciona columnas para ordenar
  - Elige orden ascendente/descendente
- **Ejemplo**: Ordenar ventas por fecha y monto

#### **🎯 Unique (Eliminar Duplicados)**
- **Función**: Elimina filas duplicadas del dataset
- **Uso**: Arrastra "Unique" desde la paleta
- **Configuración**: Selecciona columnas para evaluar duplicados
- **Ejemplo**: Eliminar clientes duplicados por email

#### **📏 Limit (Limitar Filas)**
- **Función**: Limita el número de filas del resultado
- **Uso**: Perfecto para muestras o pruebas
- **Configuración**: Especifica número máximo de filas
- **Ejemplo**: Tomar solo las primeras 1000 filas

#### **🔧 Fill Nulls (Rellenar Nulos)**
- **Función**: Rellena valores nulos con valores específicos
- **Configuración**:
  - Selecciona columnas a procesar
  - Define valor de relleno (número, texto, fecha)
- **Ejemplo**: Rellenar edad nula con promedio

#### **🗑️ Drop Nulls (Eliminar Nulos)**
- **Función**: Elimina filas que contienen valores nulos
- **Configuración**: Selecciona columnas a evaluar
- **Ejemplo**: Eliminar registros sin email

### **📊 Nodos Avanzados Disponibles**

#### **🪟 Window Functions (Funciones de Ventana)**
- **Función**: Cálculos avanzados sobre grupos de datos
- **Tipos disponibles**:
  - **Rank**: Ranking de valores
  - **Lag/Lead**: Valores anteriores/siguientes
  - **Row Number**: Numeración de filas
- **Ejemplo**: Ranking de ventas por región

#### **🔄 Pivot/Unpivot (Transformar Forma)**
- **Pivot**: Convierte filas en columnas
- **Unpivot**: Convierte columnas en filas
- **Uso común**: Transformar datos para análisis
- **Ejemplo**: Pivot de ventas mensuales por producto

#### **💥 Explode (Expandir Arrays)**
- **Función**: Expande listas/arrays en filas separadas
- **Uso**: Datos con múltiples valores en una celda
- **Ejemplo**: Expandir lista de productos por pedido

#### **📝 Text Transform (Transformaciones de Texto)**
- **Funciones disponibles**:
  - Mayúsculas/minúsculas
  - Extraer subcadenas
  - Reemplazar texto
  - Dividir cadenas
- **Ejemplo**: Normalizar nombres de clientes

#### **📅 Date Transform (Transformaciones de Fecha)**
- **Funciones disponibles**:
  - Extraer año, mes, día
  - Calcular diferencias de fechas
  - Formatear fechas
  - Crear rangos de fechas
- **Ejemplo**: Extraer mes de fecha de venta

---

## 💾 **2. Persistencia Automática de Estado**

### **✅ Problema Resuelto**
**Antes**: Los datos se perdían al cambiar entre nodos
**Ahora**: Todo se mantiene automáticamente

### **🎯 Cómo Funciona**

#### **Datos Automáticamente Preservados**
- ✅ **Datos obtenidos** con "Obtener Datos"
- ✅ **Configuraciones** de todos los campos
- ✅ **Vistas previas** de tablas
- ✅ **Selecciones de columnas**
- ✅ **Conexiones de base de datos**

#### **Flujo de Trabajo Mejorado**
1. **Configura un nodo** (base de datos, archivo, etc.)
2. **Haz clic en "Obtener Datos"**
3. **Cambia a otro nodo** → ✅ Datos se mantienen
4. **Vuelve al nodo original** → ✅ Todo sigue ahí
5. **Guarda el proyecto** → ✅ Se preserva todo

### **💡 Consejos de Uso**
- **No necesitas** hacer clic en "Probar Conexión" repetidamente
- **Los datos persisten** entre sesiones al guardar/abrir
- **Las configuraciones se mantienen** al duplicar nodos

---

## 🖱️ **3. Menú Contextual Mejorado**

### **🎯 Nuevas Opciones (Clic Derecho en Nodos)**

#### **🏷️ Cambiar Nombre**
- **Función**: Asigna nombres personalizados a tus nodos
- **Cómo usar**:
  1. Clic derecho en cualquier nodo
  2. Selecciona "Cambiar Nombre"
  3. Escribe el nuevo nombre
  4. ✅ El nodo se actualiza inmediatamente

- **Ejemplos de nombres útiles**:
  - "Datos Clientes MySQL" (en lugar de "Fuente: Base de Datos")
  - "Filtrar Activos" (en lugar de "Transformación: Filtro")
  - "Reporte Final CSV" (en lugar de "Destino: CSV")

#### **📋 Duplicar Nodo**
- **Función**: Crea una copia exacta del nodo con su configuración
- **Cómo usar**:
  1. Clic derecho en el nodo a duplicar
  2. Selecciona "Duplicar Nodo"
  3. ✅ Aparece una copia desplazada

- **Qué se duplica**:
  - ✅ **Toda la configuración** (host, usuario, consultas, etc.)
  - ✅ **Tipo y subtipo** del nodo
  - ✅ **Nombre personalizado** (con sufijo "Copia")

- **Qué NO se duplica**:
  - ❌ **Datos obtenidos** (para evitar problemas de memoria)
  - ❌ **Conexiones** con otros nodos

#### **🗑️ Eliminar Nodo**
- **Función**: Elimina el nodo seleccionado (antes era "Eliminar Todos")
- **Más seguro**: Solo elimina el nodo específico
- **Elimina también**: Todas las conexiones del nodo

### **💡 Casos de Uso Prácticos**

#### **Escenario 1: Múltiples Fuentes Similares**
```
1. Configura "Datos Clientes MySQL"
2. Duplica → "Datos Clientes MySQL (Copia)"
3. Renombra → "Datos Productos MySQL"
4. Ajusta solo la consulta SQL
```

#### **Escenario 2: Variaciones de Filtros**
```
1. Crea "Filtrar Clientes Activos"
2. Duplica → "Filtrar Clientes Activos (Copia)"
3. Renombra → "Filtrar Clientes Premium"
4. Cambia solo los criterios de filtro
```

---

## ⚡ **4. Auto-guardado Inteligente**

### **✅ Guardado Automático Activado**
**Todos los campos se guardan automáticamente** mientras escribes

### **🎯 Campos con Auto-guardado**

#### **Base de Datos**
- ✅ Host, Puerto, Usuario, Contraseña
- ✅ Nombre de base de datos
- ✅ Consultas SQL
- ✅ Tipo de base de datos

#### **APIs**
- ✅ URL, Método HTTP
- ✅ Headers, Parámetros
- ✅ Configuraciones de autenticación

#### **Archivos**
- ✅ Rutas de archivos
- ✅ Configuraciones de formato
- ✅ Selecciones de columnas

#### **Transformaciones**
- ✅ Reglas de filtro
- ✅ Configuraciones de unión
- ✅ Parámetros de agregación

### **💡 Cómo Funciona**
1. **Escribes en cualquier campo** → Se guarda automáticamente
2. **Cambias de nodo** → Configuración se preserva
3. **Ejecutas pipeline** → ✅ Funciona sin "Probar Conexión"

---

## 🛠️ **Flujos de Trabajo Recomendados**

### **📊 Crear un Pipeline Completo**

#### **Paso 1: Configurar Fuentes**
1. Arrastra nodo "Fuente" al canvas
2. Clic derecho → "Cambiar Nombre" → "Datos Ventas"
3. Configura conexión a base de datos
4. Haz clic en "Obtener Datos"
5. ✅ Selecciona columnas necesarias

#### **Paso 2: Agregar Transformaciones**
1. Arrastra nodo "Transformación" 
2. Conecta desde "Datos Ventas"
3. Configura filtros o agregaciones
4. ✅ Vista previa se actualiza automáticamente

#### **Paso 3: Duplicar para Variaciones**
1. Clic derecho en transformación → "Duplicar"
2. Renombra → "Análisis Mensual"
3. Ajusta parámetros según necesidad

#### **Paso 4: Configurar Destinos**
1. Arrastra nodo "Destino"
2. Conecta desde transformaciones
3. Configura formato de salida
4. ✅ Ejecuta pipeline completo

### **🔄 Trabajar con Múltiples Fuentes**

#### **Escenario: Combinar Datos de Ventas y Clientes**
```
1. "Datos Ventas MySQL" → Configura y obtén datos
2. Duplicar → "Datos Clientes MySQL" 
3. Cambiar consulta SQL para clientes
4. Agregar nodo "Unión" 
5. Conectar ambas fuentes
6. Configurar JOIN por ID_Cliente
```

### **📈 Análisis Avanzado con Window Functions**

#### **Ranking de Ventas por Región**
```
1. Fuente: "Datos Ventas Completos"
2. Transformación: "Window Function"
3. Configurar:
   - Función: RANK
   - Partición: Región
   - Orden: Monto DESC
4. Resultado: Ranking de vendedores por región
```

---

## 🚨 **Solución de Problemas Comunes**

### **❌ "Tipo de origen desconocido"**
**Causa**: Nodo sin configuración guardada
**Solución**: 
1. Selecciona el nodo problemático
2. Configura todos los campos requeridos
3. El auto-guardado se encarga del resto

### **❌ "Datos se pierden al cambiar nodos"**
**Causa**: Problema resuelto en esta versión
**Verificación**:
1. Configura nodo → Obtén datos
2. Cambia a otro nodo
3. Vuelve → ✅ Datos deben estar ahí

### **❌ "Pipeline no ejecuta correctamente"**
**Solución**:
1. Verifica que todos los nodos tengan configuración
2. Revisa las conexiones entre nodos
3. Usa nombres descriptivos para identificar problemas

---

## 💡 **Consejos y Mejores Prácticas**

### **🎯 Organización de Proyectos**
- **Usa nombres descriptivos** para todos los nodos
- **Agrupa nodos similares** visualmente en el canvas
- **Duplica configuraciones** en lugar de recrear desde cero
- **Guarda frecuentemente** tu proyecto

### **⚡ Optimización de Rendimiento**
- **Usa LIMIT** en consultas de prueba
- **Filtra datos temprano** en el pipeline
- **Selecciona solo columnas necesarias**
- **Evita transformaciones innecesarias**

### **🔧 Debugging Efectivo**
- **Usa vista previa** en cada paso
- **Nombres descriptivos** facilitan identificar problemas
- **Duplica nodos** para probar variaciones
- **Revisa logs** de ejecución para errores específicos

---

## 📚 **Recursos Adicionales**

### **📖 Documentación Técnica**
- `INTEGRATION_GUIDE.md` - Guía de integración de nodos
- `IMPLEMENTATION_SUMMARY.md` - Resumen técnico completo
- `CONTEXT_MENU_FEATURES.md` - Detalles del menú contextual

### **🛠️ Archivos de Configuración**
- `new_transforms.py` - Nodos básicos de transformación
- `advanced_transforms.py` - Nodos avanzados
- `new_palette_nodes.py` - Configuración de paleta

### **🔧 Correcciones Implementadas**
- `FIXES_NODE_STATE_PERSISTENCE.md` - Soluciones de persistencia
- `SOLUTION_AUTOSAVE_CONFIG.md` - Detalles de auto-guardado

---

## 🎉 **¡Disfruta tu ETL Pipeline Builder Mejorado!**

Con estas nuevas funcionalidades, tu experiencia de construcción de pipelines ETL será:
- ✅ **Más eficiente** con auto-guardado y persistencia
- ✅ **Más organizada** con nombres personalizados y duplicación
- ✅ **Más potente** con 20+ nuevos nodos de transformación
- ✅ **Más confiable** con correcciones de errores críticos

**¿Tienes preguntas?** Consulta la documentación técnica o experimenta con las nuevas funcionalidades. ¡Todo está diseñado para ser intuitivo y fácil de usar!

---

*Última actualización: Implementación completa de todas las funcionalidades solicitadas*
