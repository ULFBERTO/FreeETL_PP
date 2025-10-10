# Nuevas Funcionalidades del Menú Contextual de Nodos

## ✅ **Funcionalidades Implementadas**

### 🎯 **Menú Contextual Mejorado**
Al hacer **clic derecho** en cualquier nodo, ahora aparecen las siguientes opciones:

1. **🏷️ Cambiar Nombre** - Permite personalizar el nombre del nodo
2. **📋 Duplicar Nodo** - Crea una copia del nodo con su configuración
3. **🗑️ Eliminar Nodo** - Elimina el nodo (antes era "Eliminar Todos")

### 🏷️ **Cambiar Nombre de Nodo**

#### **Funcionalidad:**
- Permite asignar un **nombre personalizado** a cualquier nodo
- El nombre se guarda en la configuración del nodo como `custom_name`
- El nombre personalizado se muestra en lugar del nombre por defecto

#### **Cómo usar:**
1. Clic derecho en un nodo → **"Cambiar Nombre"**
2. Aparece un diálogo con el nombre actual
3. Escribir el nuevo nombre → **OK**
4. El nodo se actualiza visualmente con el nuevo nombre

#### **Ejemplo:**
- **Antes**: "Fuente: Base de Datos"
- **Después**: "Datos de Clientes MySQL"

### 📋 **Duplicar Nodo**

#### **Funcionalidad:**
- Crea una **copia exacta** del nodo seleccionado
- Copia toda la configuración (host, usuario, consultas, etc.)
- **NO copia los datos** (dataframes) para evitar problemas de memoria
- Posiciona el nodo duplicado ligeramente desplazado

#### **Cómo usar:**
1. Clic derecho en un nodo → **"Duplicar Nodo"**
2. Se crea automáticamente una copia del nodo
3. Si tenía nombre personalizado, se agrega "(Copia)" al final

#### **Ejemplo:**
- **Original**: "Datos de Ventas"
- **Duplicado**: "Datos de Ventas (Copia)"

### 🗑️ **Eliminar Nodo**

#### **Funcionalidad:**
- Elimina el nodo seleccionado y todas sus conexiones
- Reemplaza la opción anterior "Eliminar Todos"
- Más preciso y seguro

## 🛠️ **Implementación Técnica**

### **Archivos Modificados:**
- `gui/pipeline_canvas.py` - Menú contextual y funcionalidades

### **Métodos Agregados:**

#### **1. Menú Contextual Mejorado**
```python
def contextMenuEvent(self, event):
    menu = QMenu()
    rename_action = menu.addAction("Cambiar Nombre")
    duplicate_action = menu.addAction("Duplicar Nodo")
    menu.addSeparator()
    delete_action = menu.addAction("Eliminar Nodo")
```

#### **2. Cambiar Nombre**
```python
def rename_node(self, node_id):
    # Muestra diálogo para cambiar nombre
    # Guarda en config['custom_name']
    # Actualiza texto visual del nodo
```

#### **3. Duplicar Nodo**
```python
def duplicate_node(self, node_id):
    # Copia configuración (sin dataframes)
    # Crea nuevo nodo en posición desplazada
    # Agrega "(Copia)" al nombre si existe
```

#### **4. Formato de Nombre Mejorado**
```python
def _format_node_name(self, node_type, subtype, config=None):
    # Prioriza custom_name si existe
    # Fallback al formato estándar
```

## 🎯 **Beneficios para el Usuario**

### ✅ **Organización Mejorada**
- **Nombres descriptivos** para identificar fácilmente los nodos
- **Duplicación rápida** para crear variaciones de configuración
- **Eliminación precisa** sin afectar otros nodos

### ✅ **Flujo de Trabajo Eficiente**
- **Menos clics** para operaciones comunes
- **Configuración rápida** mediante duplicación
- **Identificación clara** de nodos en pipelines complejos

### ✅ **Experiencia de Usuario**
- **Menú intuitivo** con opciones claras
- **Nombres personalizados** que se mantienen al guardar/cargar
- **Operaciones seguras** con confirmaciones apropiadas

## 🧪 **Casos de Uso**

### **Escenario 1: Pipeline con Múltiples Fuentes**
```
Nodo Original: "Fuente: Base de Datos"
Renombrado a: "Datos Clientes MySQL"
Duplicado como: "Datos Clientes MySQL (Copia)"
Renombrado a: "Datos Productos MySQL"
```

### **Escenario 2: Transformaciones Similares**
```
Nodo Original: "Transformación: Filtro" 
Renombrado a: "Filtrar Clientes Activos"
Duplicado como: "Filtrar Clientes Activos (Copia)"
Renombrado a: "Filtrar Productos Disponibles"
```

### **Escenario 3: Múltiples Destinos**
```
Nodo Original: "Destino: CSV"
Renombrado a: "Exportar Reporte Mensual"
Duplicado como: "Exportar Reporte Mensual (Copia)"
Renombrado a: "Exportar Backup Diario"
```

## 📋 **Compatibilidad**

### ✅ **Archivos Existentes**
- Los nombres personalizados se guardan en la configuración
- Compatible con archivos ETL existentes
- No afecta la funcionalidad actual

### ✅ **Persistencia**
- Los nombres personalizados se mantienen al:
  - Guardar/cargar pipelines
  - Cambiar entre nodos
  - Ejecutar el pipeline

---

**Resumen**: El menú contextual ahora ofrece funcionalidades esenciales para la gestión eficiente de nodos, mejorando significativamente la experiencia de usuario y la organización de pipelines complejos.
