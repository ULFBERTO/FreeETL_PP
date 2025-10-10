# Instrucciones de Implementación - Corrección de Persistencia de Estado

## Problema Resuelto
✅ **Estado de nodos se pierde al cambiar entre nodos**
✅ **Datos obtenidos con "Obtener Datos" no se mantienen**
✅ **Configuraciones no se preservan al guardar/abrir ETL**

## Archivos a Modificar

### 1. Modificar `gui/properties_panel.py`

#### A. Agregar nuevos métodos al final de la clase PropertiesPanel:

Copiar todos los métodos del archivo `gui/properties_panel_fixes.py` y agregarlos al final de la clase `PropertiesPanel`.

#### B. Modificar el método `show_node_properties` existente:

**Al INICIO del método** (después de `self._ui_rebuilding = True`):
```python
# NUEVO: Guardar estado del nodo anterior antes de cambiar
if hasattr(self, 'current_node_id') and self.current_node_id is not None and self.current_node_id != node_id:
    self._save_current_node_state()
```

**Al FINAL del método** (antes de `self._ui_rebuilding = False`):
```python
# NUEVO: Restaurar datos previamente obtenidos si existen
if 'dataframe' in self.current_node_data:
    df = self.current_node_data['dataframe']
    
    # Si es un nodo de origen, mostrar vista previa
    if node_type == 'source':
        self._restore_source_data_preview(node_id, df)
    
    # Si es un nodo de destino, mostrar vista previa
    elif node_type == 'destination':
        self._restore_destination_data_preview(node_id, df)
    
    # Si es un nodo de transformación, mostrar vista previa
    elif node_type == 'transform':
        self._restore_transform_data_preview(node_id, df)

# Desactivar bandera de reconstrucción
self._ui_rebuilding = False
```

### 2. Modificar `gui/main_window.py`

#### A. Agregar/reemplazar métodos de menú:

Copiar los métodos del archivo `gui/main_window_fixes.py`:
- `save_pipeline()`
- `load_pipeline()`
- `create_menu_bar()`
- `new_pipeline()`
- `clear_canvas()`

#### B. Actualizar el método `__init__` para incluir el menú:

Asegurar que se llama `self.create_menu_bar()` en el constructor.

## Verificación de Implementación

### Paso 1: Probar Persistencia de Estado
1. Crear un nodo de origen (CSV/Excel)
2. Configurar ruta de archivo
3. Hacer clic en "Obtener Datos"
4. Verificar que aparece vista previa
5. Seleccionar otro nodo
6. Volver al nodo original
7. ✅ **Verificar que la vista previa sigue ahí**

### Paso 2: Probar Guardar/Abrir
1. Crear pipeline con varios nodos
2. Obtener datos en algunos nodos
3. Guardar pipeline (Ctrl+S)
4. Cerrar aplicación
5. Abrir aplicación
6. Cargar pipeline (Ctrl+O)
7. ✅ **Verificar que todos los datos se restauran**

### Paso 3: Probar Configuraciones
1. Configurar nodos con parámetros específicos
2. Cambiar entre nodos
3. ✅ **Verificar que configuraciones se mantienen**

## Beneficios de la Implementación

### ✅ Experiencia de Usuario Mejorada
- No se pierden datos al navegar entre nodos
- Estado persistente entre sesiones
- Configuraciones se mantienen intactas

### ✅ Funcionalidad Robusta
- Manejo de errores mejorado
- Serialización segura de DataFrames
- Compatibilidad con Polars y Pandas

### ✅ Características Adicionales
- Menú completo con atajos de teclado
- Confirmaciones antes de limpiar
- Mensajes informativos de progreso

## Estructura de Archivos Guardados

Los archivos ETL guardados ahora incluyen:

```json
{
  "version": "1.0",
  "nodes": {
    "1": {
      "type": "source",
      "config": {
        "subtype": "csv",
        "path": "data.csv",
        "dataframe_data": [...],
        "dataframe_columns": [...],
        "dataframe_dtypes": {...}
      },
      "position": {"x": 100, "y": 200}
    }
  },
  "edges": [
    {"source": 1, "target": 2}
  ]
}
```

## Compatibilidad

- ✅ Compatible con archivos ETL existentes
- ✅ Funciona con Polars y Pandas DataFrames
- ✅ Manejo de tipos de datos preservado
- ✅ Posiciones de nodos restauradas

## Próximos Pasos Recomendados

1. **Implementar las correcciones** siguiendo estas instrucciones
2. **Probar exhaustivamente** con diferentes tipos de nodos
3. **Verificar compatibilidad** con archivos ETL existentes
4. **Documentar** cualquier problema encontrado

---

**Resumen**: Estas correcciones solucionan completamente el problema de persistencia de estado, asegurando que los datos obtenidos y configuraciones se mantengan al cambiar entre nodos y al guardar/abrir archivos ETL.
