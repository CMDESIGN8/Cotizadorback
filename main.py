# main.py
from supabase import create_client, Client
import os
import re
import httpx
import platform
import subprocess
import asyncio
import logging
import traceback  # ← AGREGAR ESTA LÍNEA
import pathlib # <-- NUEVO
from uuid import uuid4
from datetime import datetime, timedelta, date # <-- ¡Aquí está la corrección!
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi import UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse  # ← AGREGAR FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, EmailStr
from dotenv import load_dotenv
from pathlib import Path # <-- NUEVO


# Supabase client
from supabase import create_client, Client

# Load environment
load_dotenv()

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ganbatte_api")

# -----------------------
# Config
# -----------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    logger.warning("SUPABASE_URL or SUPABASE_KEY not set. Ensure values in .env for Supabase access.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")
BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.expanduser("~"), "Ganbatte", "Operaciones"))
os.makedirs(BASE_DIR, exist_ok=True)

def get_ruta_operacion(codigo_folder: str) -> str:
    """
    Convierte el código legible (URL) a una ruta de sistema de archivos (OS).
    BASE_DIR ya apunta a la carpeta 'Operaciones', por lo que no la volvemos a incluir.
    """
    # 1. Normaliza barras (de '/' a '\' en Windows)
    codigo_normalizado = codigo_folder.replace('/', os.sep)
    
    # 2. Usa el path completo: BASE_DIR -> Codigo_Normalizado
    # Esto construye: C:\Users\Usuario\Ganbatte\Operaciones\GAN-IM-25\11\030
    return os.path.join(BASE_DIR, codigo_normalizado)

ENV = os.getenv("ENV", "development")  # set to 'production' when deploying

EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")

# Business constants
INCOTERMS = [
    "EXW","FCA","CPT","CIP","DAP","DPU","DDP","FAS","FOB","CFR","CIF"
]

TRANSPORT_MODES = ["Aerea","Maritima FCL","Maritima LCL","Terrestre","Courier"]

# ⭐️ La lista que usted proporcionó, la usaremos como el set de contenedores válidos
VALID_DB_CONTAINERS = {
    "20DV", "40DV", "40HC", "20TK", "20OT", "20FR", "20RE", "40OT", "40FR", "40NOR"
}

# --- Mapeo de Nombres de Equipo (Long-form -> Short-code) ---
# Esto resuelve el problema de la cotización vs. los costos de la DB
EQUIPO_MAP = {
    # Mapeos de nombres largos a códigos cortos (Frontend a DB)
    "20' STANDARD": "20DV",
    "20' STD": "20DV",
    "40' STANDARD": "40DV",
    "40' STD": "40DV",
    "40' HIGH CUBE": "40HC",
    "40' HC": "40HC",
    "40' STANDARD HIGH CUBE": "40HC",
    "20' TANK": "20TK",
    "20' OPEN TOP": "20OT",
    "20' FLAT RACK": "20FR",
    "20' REEFER": "20RE",
    "40' OPEN TOP": "40OT",
    "40' FLAT RACK": "40FR",
    "40' NOR": "40NOR",
    # Mapeos de códigos cortos a sí mismos (para asegurar que pasan la función)
    "20DV": "20DV", "40DV": "40DV", "40HC": "40HC", "20TK": "20TK", 
    "20OT": "20OT", "20FR": "20FR", "20RE": "20RE", "40OT": "40OT", 
    "40FR": "40FR", "40NOR": "40NOR"
}

# -----------------------
# FastAPI app
# -----------------------
app = FastAPI(
    title="Ganbatte API",
    description="Sistema de automatización de cotizaciones (local)",
    version="1.1.0"
)

# Configuración CORS MEJORADA
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://cotizaciones.ganbatte.com.ar/","http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],  # Incluye GET, POST, PUT, DELETE, OPTIONS, etc.
    allow_headers=["*"],  # Incluye Content-Type, Authorization, etc.
    expose_headers=["*"]  # Expone todos los headers
)

def obtener_estructura_carpeta(ruta_base: Path):
    """
    Función recursiva para obtener la estructura de archivos y carpetas.
    Devuelve un diccionario o lista con nombre, tipo y (si es carpeta) contenido.
    """
    estructura = []
    try:
        for item in ruta_base.iterdir():
            if item.is_dir():
                # Si es una carpeta, recursivamente obtenemos su contenido (solo un nivel profundo para evitar sobrecarga)
                # Para un navegador simple, solo listamos las carpetas principales y los archivos dentro.
                contenido_archivos = [
                    {"nombre": f.name, "tipo": "archivo", "size": f.stat().st_size} 
                    for f in item.iterdir() if f.is_file()
                ]
                
                # Para este primer paso, solo listamos las subcarpetas de un nivel.
                contenido_carpetas = [
                     {"nombre": d.name, "tipo": "carpeta", "contenido": []} 
                     for d in item.iterdir() if d.is_dir()
                ]
                
                estructura.append({
                    "nombre": item.name,
                    "tipo": "carpeta",
                    "contenido": contenido_carpetas + contenido_archivos # Unimos carpetas y archivos
                })
            elif item.is_file():
                estructura.append({
                    "nombre": item.name,
                    "tipo": "archivo",
                    "size": item.stat().st_size
                })
    except Exception as e:
        logger.error(f"Error al leer directorio {ruta_base}: {e}")
        return []

    return estructura

@app.post("/api/operaciones/{codigo_operacion:path}/subir-archivo")
async def subir_archivo_operacion(
    codigo_operacion: str,
    subcarpeta: str = Form(...),
    archivo: UploadFile = File(...)
):
    """
    Recibe un archivo y lo guarda en la subcarpeta especificada
    dentro de la carpeta de la operación.
    """
    try:
        # 1. Normalizar el código para el sistema de archivos
        codigo_normalizado = codigo_operacion.replace('/', os.sep)
        
        # 2. Construir la ruta de la carpeta de destino usando BASE_DIR
        ruta_base_operacion = get_ruta_operacion(codigo_operacion)
        
        # 3. Validar Subcarpeta (seguridad y estructura)
        subcarpetas_validas = ['Cotizaciones', 'Documentos', 'BLs', 'Facturas', 'Otros']
        if subcarpeta not in subcarpetas_validas:
            raise HTTPException(status_code=400, detail=f"Subcarpeta '{subcarpeta}' no válida. Debe ser una de: {', '.join(subcarpetas_validas)}")

        ruta_destino_carpeta = os.path.join(ruta_base_operacion, subcarpeta)
        
        # 4. Crear la carpeta si no existe (por si acaso)
        os.makedirs(ruta_destino_carpeta, exist_ok=True)
        
        # 5. Ruta completa del archivo
        ruta_final_archivo = os.path.join(ruta_destino_carpeta, archivo.filename)

        logger.info(f"Guardando archivo '{archivo.filename}' en: {ruta_final_archivo}")
        
        # 6. Guardar el archivo de forma asíncrona
        # NOTA: Usamos 'wb' para escribir bytes
        with open(ruta_final_archivo, "wb") as buffer:
            data = await archivo.read()
            buffer.write(data)

        # 7. Retornar el éxito
        return {
            "mensaje": f"Archivo '{archivo.filename}' subido exitosamente a '{subcarpeta}'",
            "nombre_archivo": archivo.filename,
            "ruta_guardada": ruta_final_archivo
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error subiendo archivo para {codigo_operacion}: {e}")
        raise HTTPException(status_code=500, detail=f"Error al subir archivo: {str(e)}")
    
@app.post("/api/operaciones/{codigo_operacion:path}/abrir-carpeta")
async def abrir_carpeta_operacion(codigo_operacion: str):
    """
    Abre la carpeta de la operación en el explorador de archivos del sistema, 
    usando la misma lógica y ruta que el módulo de cotizaciones.
    """
    try:
        # 1. Normalizar el código para el sistema de archivos (usa os.sep)
        codigo_normalizado = codigo_operacion.replace('/', os.sep)
        ruta_base_operacion = get_ruta_operacion(codigo_operacion)
        
        logger.info(f"Intento de abrir carpeta: {ruta_base_operacion}")

        # 2. Crear la carpeta si no existe (importante para que el explorador tenga algo que abrir)
        os.makedirs(ruta_base_operacion, exist_ok=True)
        # Opcional: Crear las subcarpetas si solo quieres que se creen al abrir.
        # subcarpetas = ['Cotizaciones', 'Documentos', 'BLs', 'Facturas', 'Otros']
        # for subcarpeta in subcarpetas:
        #     os.makedirs(os.path.join(ruta_base_operacion, subcarpeta), exist_ok=True)


        # 3. Lógica Multiplataforma para abrir la carpeta (usando tu método preferido)
        sistema = platform.system()
        
        if sistema == "Windows":
            # Usar os.startfile para Windows (mejor que subprocess para abrir explorador)
            try:
                os.startfile(ruta_base_operacion)
            except Exception as e:
                logger.error(f"Error abriendo carpeta con os.startfile: {e}")
                # Fallback por si acaso
                subprocess.Popen(["explorer", str(ruta_base_operacion)]) 
                
        elif sistema == "Darwin": # macOS
            subprocess.Popen(["open", str(ruta_base_operacion)])
            
        elif sistema == "Linux":
            # Comando para Linux (usa xdg-open que funciona en la mayoría de distros)
            subprocess.Popen(["xdg-open", str(ruta_base_operacion)])
        else:
            raise HTTPException(status_code=500, detail=f"Sistema operativo no soportado: {sistema}")
        
        return {"mensaje": f"Comando enviado para abrir la carpeta {codigo_operacion}."}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error abriendo carpeta para {codigo_operacion}: {e}")
        raise HTTPException(status_code=500, detail=f"Error al intentar abrir la carpeta: {str(e)}")

@app.get("/api/operaciones/{codigo_operacion:path}/archivos")
async def get_archivos_operacion(codigo_operacion: str):
    """
    Lista los archivos, mapeando el código de la Operación (URL) al código de la Cotización (Carpeta).
    """
    
    try:
        codigo_folder_system = codigo_operacion # Fallback
        
        # 1. LÓGICA DE BÚSQUEDA DEL CÓDIGO DE CARPETA (Cotización)
        if supabase is not None:
            try:
                # Buscamos: 'codigo_operacion' (GAN-OP-...) en la DB -> 'codigo_legible' (GAN-IM-...)
                response = supabase.table("cotizaciones").select("codigo_legible").eq("codigo_operacion", codigo_operacion).limit(1).execute()
                
                if response.data and response.data[0].get('codigo_legible'):
                    # ¡Encontrado! Usamos el código de cotización para la carpeta
                    codigo_folder_system = response.data[0]['codigo_legible']
                    logger.info(f"DB Mapeo: Carpeta encontrada para {codigo_operacion}: {codigo_folder_system}")
                else:
                    logger.warning(f"DB Mapeo: No se encontró código de cotización para {codigo_operacion}. Usando código de operación como fallback.")
                    
            except Exception as e:
                logger.error(f"Error en el mapeo de códigos de carpeta: {e}")

        # 2. CONSTRUIR RUTA (usando el código que SÍ existe en el disco, sea el de cotización o el fallback)
        ruta_base_operacion = get_ruta_operacion(codigo_folder_system)
        
        logger.info(f"Ruta final de búsqueda: {ruta_base_operacion}") 
        
        # 3. VERIFICAR EXISTENCIA Y LISTAR ARCHIVOS
        if not os.path.isdir(ruta_base_operacion):
            return JSONResponse(status_code=404, content={
                "error": "Carpeta no encontrada o no creada aún en el servidor.",
                "ruta_buscada": ruta_base_operacion,
                "codigo_usado": codigo_folder_system,
                "subcarpetas": {}
            })

        archivos_por_subcarpeta = {}
        subcarpetas = ['Cotizaciones', 'Documentos', 'BLs', 'Facturas', 'Otros']
        
        for subcarpeta_nombre in subcarpetas:
            ruta_subcarpeta = os.path.join(ruta_base_operacion, subcarpeta_nombre)
            
            # Si no existe, no intentamos listar
            if os.path.isdir(ruta_subcarpeta):
                archivos_encontrados = []
                for nombre_archivo in os.listdir(ruta_subcarpeta):
                    ruta_completa_archivo = os.path.join(ruta_subcarpeta, nombre_archivo)
                    
                    if os.path.isfile(ruta_completa_archivo):
                        timestamp = os.path.getmtime(ruta_completa_archivo)
                        fecha_modificacion = datetime.fromtimestamp(timestamp).isoformat()

                        # Usamos el código de la URL para la ruta_relativa, ya que así lo espera el frontend.
                        ruta_relativa_frontend = os.path.join(codigo_operacion, subcarpeta_nombre, nombre_archivo).replace(os.sep, '/')

                        archivos_encontrados.append({
                            "nombre": nombre_archivo,
                            "ruta_relativa": ruta_relativa_frontend, 
                            "fecha_modificacion": fecha_modificacion,
                            "tamano_bytes": os.path.getsize(ruta_completa_archivo)
                        })

                archivos_encontrados.sort(key=lambda x: x['fecha_modificacion'], reverse=True)
                archivos_por_subcarpeta[subcarpeta_nombre] = archivos_encontrados
            else:
                archivos_por_subcarpeta[subcarpeta_nombre] = []
        
        return {
            "mensaje": "Archivos listados exitosamente",
            "ruta_base": ruta_base_operacion,
            "subcarpetas": archivos_por_subcarpeta
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error listando archivos de operación {codigo_operacion}: {e}")
        # Aseguramos que el 500 tenga detalles en la consola
        raise HTTPException(status_code=500, detail=f"Error interno del servidor al listar: {str(e)}")
    
def get_standard_equipo(equipo_cotizacion: Optional[str]) -> Optional[str]:
    """Convierte el nombre legible/de selección del equipo al código estandarizado de la DB."""
    if not equipo_cotizacion: return None
    # Limpieza: Convertir a mayúsculas y normalizar el texto de búsqueda
    equipo_busqueda = equipo_cotizacion.upper().replace("'", "").replace(" ", " ").strip()
    return EQUIPO_MAP.get(equipo_busqueda, None)

# NOTA: Reemplace o elimine su antigua definición de CONTAINER_TYPES si está causando conflictos.
# La validación usará VALID_DB_CONTAINERS.

# Estado mapping
ESTADOS_COTIZACION = {
    'creada': {'color': '#f97316', 'label': '🟠 CREADA', 'dias_alerta': None},
    'aceptada': {'color': '#10b981', 'label': '🟢 ACEPTADA', 'dias_alerta': None},
    'por_vencer': {'color': '#f59e0b', 'label': '🟡 POR VENCER', 'dias_alerta': 2},
    'vencida': {'color': '#ef4444', 'label': '🔴 VENCIDA', 'dias_alerta': 0},
    'enviada': {'color': '#3b82f6', 'label': '🔵 ENVIADA', 'dias_alerta': None},
    'rechazada': {'color': '#6b7280', 'label': '⚫ RECHAZADA', 'dias_alerta': None}
}

# -----------------------
# Pydantic models
# -----------------------

class TrackingUpdate(BaseModel):
    codigo_operacion: str
    etd: str = None
    eta: str = None
    fecha_carga: str = None
    fecha_descarga: str = None
    equipo: str = None
    origen: str = None
    destino: str = None
    referencia: str = None
    volumen_m3: float = None
    peso_total_kg: float = None
    incoterm_origen: str = None
    incoterm_destino: str = None

class ChecklistItem(BaseModel):
    id: Optional[str] = None
    codigo_operacion: str
    tarea: str
    completada: bool = False
    usuario_creacion: Optional[str] = None

class ChecklistItemUpdate(BaseModel):
    tarea: Optional[str] = None
    completada: Optional[bool] = None

class Operacion(BaseModel):
    id: Optional[str] = None
    codigo_operacion: str
    cotizacion_origen: str # <-- Coincide con tu SQL
    cliente: str
    tipo_operacion: str
    estado: str = "en_proceso" # <-- Coincide con tu SQL
    fecha_creacion: Optional[datetime] = None
    fecha_actualizacion: Optional[datetime] = None
    datos_cotizacion: Optional[Dict[str, Any]] = None # <-- Coincide con tu JSONB

# En los modelos Pydantic de Cliente
class Cliente(BaseModel):
    id: Optional[str] = None
    nombre: str
    email: Optional[EmailStr] = None
    telefono: Optional[str] = None
    direccion: Optional[str] = None
    ciudad: Optional[str] = None
    pais: Optional[str] = "Argentina"  # Cambiado a Argentina por defecto
    cuit: Optional[str] = None  # Cambiado de rut a cuit
    giro: Optional[str] = None
    contacto_principal: Optional[str] = None
    activo: bool = True
    fecha_creacion: Optional[datetime] = None
    fecha_actualizacion: Optional[datetime] = None

class ClienteCreate(BaseModel):
    nombre: str
    email: Optional[EmailStr] = None
    telefono: Optional[str] = None
    direccion: Optional[str] = None
    ciudad: Optional[str] = None
    pais: Optional[str] = "Argentina"  # Cambiado a Argentina por defecto
    cuit: Optional[str] = None  # Cambiado de rut a cuit
    giro: Optional[str] = None
    contacto_principal: Optional[str] = None

class ClienteUpdate(BaseModel):
    nombre: Optional[str] = None
    email: Optional[EmailStr] = None
    telefono: Optional[str] = None
    direccion: Optional[str] = None
    ciudad: Optional[str] = None
    pais: Optional[str] = None
    cuit: Optional[str] = None  # Cambiado de rut a cuit
    giro: Optional[str] = None
    contacto_principal: Optional[str] = None
    activo: Optional[bool] = None

class GastoLocalMaritimo(BaseModel):
    id: Optional[str] = None
    tipo_operacion: str
    linea_maritima: str
    equipo: str
    thc: Optional[float] = None
    toll: Optional[float] = None
    gate: Optional[float] = None
    delivery_order: Optional[float] = None
    ccf: Optional[float] = None
    handling: Optional[float] = None
    logistic_fee: Optional[float] = None
    bl_fee: Optional[float] = None
    ingreso_sim: Optional[float] = None
    cert_flete: Optional[float] = None
    cert_fob: Optional[float] = None
    total_locales: Optional[float] = None
    beneficio: Optional[str] = None
    fecha_actualizacion: Optional[date] = None

class LineaMaritima(BaseModel):
    id: int
    nombre: str
    activo: bool

class Aerolinea(BaseModel):
    id: int
    nombre: str
    codigo_iata: Optional[str] = None
    pais: Optional[str] = None
    activo: bool = True    

class CostoLineaMaritima(BaseModel):
    id: int
    linea_maritima_id: int
    equipo: str
    thc: float
    toll: float
    gate: float
    delivery_order: float
    ccf: float
    handling: float
    logistic_fee: float
    bl_fee: float
    ingreso_sim: float
    moneda: str
    activo: bool

class Cotizacion(BaseModel):
    cliente: str
    tipo_operacion: str = Field(..., description="IA/IM/EA/EM/IT/ET")
    modo_transporte: str
    incoterm_origen: Optional[str] = None
    incoterm_destino: Optional[str] = None
    origen: str
    destino: str
    referencia: Optional[str] = None
    validez_dias: Optional[int] = 30
    email_cliente: Optional[EmailStr] = None
    linea_maritima: Optional[str] = None
    aerolinea: Optional[str] = None
    equipo: Optional[str] = None
    cantidad_contenedores: Optional[int] = 1
    tipo_contenedor: Optional[str] = None
    cantidad_bls: Optional[int] = 1
    valor_comercial: Optional[float] = 0.0
    peso_total_kg: Optional[float] = 0.0
    peso_cargable_kg: Optional[float] = 0.0
    volumen_m3: Optional[float] = 0.0
    tipo_embalaje: Optional[str] = None
    cantidad_pallets: Optional[int] = 0
    transit_time_days: Optional[int] = None
    transbordo: Optional[bool] = False
    dias_libres_almacenaje: Optional[int] = 0
    pickup_address: Optional[str] = None
    delivery_address: Optional[str] = None
    pre_carrier: Optional[str] = None
    consolidacion_deconsolidacion: Optional[str] = None
    aplica_alimentos: Optional[bool] = False
    tiene_hielo_seco: Optional[bool] = False
    gastos_locales: Optional[float] = 0.0

class CodigoRequest(BaseModel):
    codigo: str

class CambioEstadoRequest(BaseModel):
    codigo_legible: str
    nuevo_estado: str

class CostoPersonalizado(BaseModel):
    """Representa un costo individual, ya sea predefinido o personalizado."""
    id: Optional[str] = None
    concepto: str
    costo: float
    venta: float
    es_predefinido: bool
    tipo: str
    codigo_cotizacion: str # Para enlazar con la cotización
    detalles: Optional[Dict[str, Any]] = None # Para datos extra como THC, etc.
    moneda: str = "USD"

class SolicitudGuardarCostos(BaseModel):
    """Estructura para guardar todos los costos asociados a una cotización."""
    codigo_cotizacion: str
    costos: List[CostoPersonalizado]

class Notificacion(BaseModel):
    cotizacion_codigo: str
    tipo: str
    mensaje: str
    fecha: Optional[str] = None
    leido: bool = False

# -----------------------
# Helper functions
# -----------------------

# -----------------------
# Carpeta local helper endpoints
# -----------------------

@app.get("/")
def read_root():
    return {
        "message": "🚀 Ganbatte API funcionando (local)",
        "status": "active",
        "database": "Supabase (o local Postgres cuando se configure)",
    }

@app.get("/health")
def health_check():
    try:
        if supabase is None:
            db_status = "not_configured"
        else:
            response = supabase.table('cotizaciones').select('id', count='exact').limit(1).execute()
            db_status = "connected" if response and (response.data is not None) else "error"
    except Exception as e:
        db_status = f"error: {str(e)}"
    return {"status": "healthy", "database": db_status}


@app.post("/api/operaciones/tracking")
async def actualizar_tracking(data: TrackingUpdate):
    # Buscar operación
    op_response = supabase.table("operaciones").select("*").eq("codigo_operacion", data.codigo_operacion).single().execute()
    if op_response.error or not op_response.data:
        raise HTTPException(status_code=404, detail="Operación no encontrada")
    
    # Actualizar datos_cotizacion
    datos_actualizados = op_response.data.get("datos_cotizacion") or {}
    for key in data.dict(exclude={"codigo_operacion"}):
        if data.dict()[key] is not None:
            datos_actualizados[key] = data.dict()[key]

    # Guardar cambios
    update_response = supabase.table("operaciones").update({"datos_cotizacion": datos_actualizados}).eq("codigo_operacion", data.codigo_operacion).execute()
    if update_response.error:
        raise HTTPException(status_code=500, detail="Error al actualizar operación")

    return {"message": "Datos de tracking actualizados", "datos_cotizacion": datos_actualizados}


@app.get("/api/cotizaciones/{codigo_path:path}")
async def obtener_cotizacion_completa(codigo_path: str):
    """Obtener una cotización específica - maneja códigos con barras"""
    try:
        print(f"🔍 Buscando cotización: {codigo_path}")
        
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Buscar por codigo_legible (que puede contener barras)
        response = supabase.table("cotizaciones").select("*").eq("codigo_legible", codigo_path).execute()
        
        print(f"📊 Resultado de búsqueda: {len(response.data)} registros")
        
        if not response.data:
            raise HTTPException(status_code=404, detail=f"Cotización '{codigo_path}' no encontrada")

        cotizacion = response.data[0]
        print(f"✅ Cotización encontrada: {cotizacion['codigo_legible']}")

        # Obtener los costos asociados
        costos_response = supabase.table("costos_cotizacion").select("*").eq("codigo_cotizacion", cotizacion['codigo_legible']).execute()
        print(f"💰 Costos encontrados: {len(costos_response.data or [])}")
        
        # Calcular estado actual
        estado_info = calcular_estado_y_validez(
            cotizacion.get('fecha_validez'), 
            cotizacion.get('validez_dias', 30),
            cotizacion.get('estado')
        )

        # Preparar respuesta completa
        cotizacion_completa = {
            **cotizacion,
            "costos": costos_response.data or [],
            "estado_actual": estado_info['estado'],
            "color": estado_info['color'],
            "dias_restantes": estado_info['dias_restantes'],
            "label_estado": ESTADOS_COTIZACION.get(estado_info['estado'], {'label': '🔵 ENVIADA'})['label']
        }

        return cotizacion_completa

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error obteniendo cotización: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener cotización: {str(e)}")


@app.post("/api/cotizaciones/duplicar")
async def duplicar_cotizacion(cotizacion_duplicada: dict):
    """Duplicar cotización - CORREGIDO para campos específicos"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        print(f"📋 Iniciando duplicación para: {cotizacion_duplicada.get('codigo_legible', 'N/A')}")

        # 🔍 DEBUG ESPECÍFICO PARA CAMPOS PROBLEMÁTICOS
        print("🔍 CAMPOS CRÍTICOS RECIBIDOS:")
        campos_problematicos = ['origen', 'destino', 'peso_total_kg', 'peso_cargable_kg', 'volumen_m3', 'cantidad_pallets']
        for campo in campos_problematicos:
            valor = cotizacion_duplicada.get(campo)
            print(f"   {campo}: '{valor}' (tipo: {type(valor)})")

        # ✅ VALIDACIÓN CRÍTICA: Verificar modo_transporte
        modo_transporte = cotizacion_duplicada.get('modo_transporte', '')
        print(f"🚚 Modo transporte recibido: '{modo_transporte}'")
        
        MODOS_TRANSPORTE_VALIDOS = ["Aerea", "Maritima FCL", "Maritima LCL", "Terrestre", "Courier"]
        
        if modo_transporte not in MODOS_TRANSPORTE_VALIDOS:
            print(f"⚠️ Modo transporte inválido: '{modo_transporte}'. Usando valor por defecto.")
            modo_transporte = "Aerea"

        # 1. Generar nuevo código correlativo
        tipo_operacion = cotizacion_duplicada.get('tipo_operacion', '')
        nuevo_codigo_legible = await generar_proximo_numero(tipo_operacion)
        print(f"🎯 Nuevo código correlativo generado: {nuevo_codigo_legible}")

        # 2. Usar fecha ACTUAL con timezone de Argentina
        from datetime import timezone
        fecha_actual = datetime.now(timezone(timedelta(hours=-3)))
        fecha_validez = fecha_actual + timedelta(days=30)

        # ✅ FUNCIÓN AUXILIAR PARA MANEJAR CAMPOS NUMÉRICOS
        def safe_numeric_value(value, default=0.0):
            """Convierte valores vacíos o inválidos a números seguros"""
            if value is None or value == "":
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        def safe_int_value(value, default=0):
            """Convierte valores vacíos o inválidos a enteros seguros"""
            if value is None or value == "":
                return default
            try:
                return int(value)
            except (ValueError, TypeError):
                return default

        # 3. ✅ CREAR NUEVA COTIZACIÓN - ENFOCADO EN CAMPOS PROBLEMÁTICOS
        nueva_cotizacion_data = {
            "codigo": str(uuid4()),
            "codigo_legible": nuevo_codigo_legible,
            "cliente": cotizacion_duplicada.get('cliente', ''),
            "tipo_operacion": tipo_operacion,
            "modo_transporte": modo_transporte,
            
            # ✅ CAMPOS CRÍTICOS - EXTRAER DIRECTAMENTE CON VALORES POR DEFECTO
            "origen": cotizacion_duplicada.get('origen') or "",  # Forzar string vacío si es None
            "destino": cotizacion_duplicada.get('destino') or "",
            
            # ✅ CAMPOS DE PESOS Y MEDIDAS - CONVERSIÓN EXPLÍCITA
            "peso_total_kg": safe_numeric_value(cotizacion_duplicada.get('peso_total_kg'), 0.0),
            "peso_cargable_kg": safe_numeric_value(cotizacion_duplicada.get('peso_cargable_kg'), 0.0),
            "volumen_m3": safe_numeric_value(cotizacion_duplicada.get('volumen_m3'), 0.0),
            "cantidad_pallets": safe_int_value(cotizacion_duplicada.get('cantidad_pallets'), 0),
            
            "estado": "creada",
            "fecha_creacion": fecha_actual.isoformat(),
            "fecha_actualizacion": fecha_actual.isoformat(),
            "fecha_validez": fecha_validez.date().isoformat(),
            "validez_dias": 30,
            
            # ✅ OTROS CAMPOS IMPORTANTES
            "incoterm_origen": cotizacion_duplicada.get('incoterm_origen'),
            "incoterm_destino": cotizacion_duplicada.get('incoterm_destino'),
            "referencia": cotizacion_duplicada.get('referencia'),
            "email_cliente": cotizacion_duplicada.get('email_cliente'),
            "linea_maritima": cotizacion_duplicada.get('linea_maritima'),
            "aerolinea": cotizacion_duplicada.get('aerolinea'),
            "equipo": cotizacion_duplicada.get('equipo'),
            "cantidad_contenedores": safe_int_value(cotizacion_duplicada.get('cantidad_contenedores'), 1),
            "tipo_contenedor": cotizacion_duplicada.get('tipo_contenedor'),
            "cantidad_bls": safe_int_value(cotizacion_duplicada.get('cantidad_bls'), 1),
            "valor_comercial": safe_numeric_value(cotizacion_duplicada.get('valor_comercial'), 0.0),
            "tipo_embalaje": cotizacion_duplicada.get('tipo_embalaje'),
            "transit_time_days": safe_int_value(cotizacion_duplicada.get('transit_time_days')),
            "transbordo": bool(cotizacion_duplicada.get('transbordo', False)),
            "dias_libres_almacenaje": safe_int_value(cotizacion_duplicada.get('dias_libres_almacenaje'), 0),
            "pickup_address": cotizacion_duplicada.get('pickup_address'),
            "delivery_address": cotizacion_duplicada.get('delivery_address'),
            "pre_carrier": cotizacion_duplicada.get('pre_carrier'),
            "consolidacion_deconsolidacion": cotizacion_duplicada.get('consolidacion_deconsolidacion'),
            "aplica_alimentos": bool(cotizacion_duplicada.get('aplica_alimentos', False)),
            "tiene_hielo_seco": bool(cotizacion_duplicada.get('tiene_hielo_seco', False)),
            "gastos_locales": safe_numeric_value(cotizacion_duplicada.get('gastos_locales'), 0.0)
        }

        # 🔍 VERIFICACIÓN FINAL ANTES DE INSERTAR
        print("✅ DATOS QUE SE INSERTARÁN:")
        for campo in campos_problematicos:
            print(f"   {campo}: {nueva_cotizacion_data[campo]}")

        print(f"✅ Insertando cotización: {nueva_cotizacion_data['codigo_legible']}")

        # 4. Insertar la nueva cotización
        response_cotizacion = supabase.table("cotizaciones").insert(nueva_cotizacion_data).execute()
        
        if not response_cotizacion.data:
            raise HTTPException(status_code=500, detail="Error al crear la cotización duplicada")

        nueva_cotizacion = response_cotizacion.data[0]
        codigo_nuevo = nueva_cotizacion['codigo_legible']
        print(f"✅ Cotización duplicada creada: {codigo_nuevo}")

        # 5. Duplicar costos (mantener tu lógica actual)
        costos_originales = cotizacion_duplicada.get('costos', [])
        costos_duplicados_count = 0
        
        if costos_originales:
            print(f"💰 Procesando {len(costos_originales)} costos para duplicación...")
            
            conceptos_unicos = {}
            nuevos_costos = []
            
            for costo in costos_originales:
                concepto = costo.get('concepto', '').strip()
                if not concepto:
                    continue
                    
                if concepto not in conceptos_unicos:
                    conceptos_unicos[concepto] = True
                    
                    nuevo_costo = {
                        "codigo_cotizacion": codigo_nuevo,
                        "concepto": concepto,
                        "costo": safe_numeric_value(costo.get('costo'), 0),
                        "venta": safe_numeric_value(costo.get('venta'), 0),
                        "es_predefinido": bool(costo.get('es_predefinido', False)),
                        "tipo": costo.get('tipo', 'OTRO'),
                        "fecha_creacion": fecha_actual.isoformat()
                    }
                    
                    if costo.get('detalles'):
                        nuevo_costo["detalles"] = costo['detalles']
                    
                    nuevos_costos.append(nuevo_costo)
            
            print(f"✅ Costos únicos a insertar: {len(nuevos_costos)}")
            
            if nuevos_costos:
                response_costos = supabase.table("costos_cotizacion").insert(nuevos_costos).execute()
                if response_costos.data:
                    costos_duplicados_count = len(response_costos.data)
                    print(f"✅ {costos_duplicados_count} costos duplicados exitosamente")

        # 6. Crear carpeta
        try:
            carpeta_path = os.path.join(BASE_DIR, codigo_nuevo)
            subcarpetas = ['Cotizaciones', 'Documentos', 'BLs', 'Facturas', 'Otros']
            
            os.makedirs(carpeta_path, exist_ok=True)
            for subcarpeta in subcarpetas:
                subcarpeta_path = os.path.join(carpeta_path, subcarpeta)
                os.makedirs(subcarpeta_path, exist_ok=True)
            
            print(f"📁 Carpeta creada: {carpeta_path}")
        except Exception as e:
            print(f"⚠️ Error creando carpeta: {e}")

        print(f"🎉 Duplicación completada exitosamente: {codigo_nuevo}")
        
        return {
            "mensaje": "Cotización duplicada exitosamente",
            "codigo_nuevo": codigo_nuevo,
            "costos_duplicados": costos_duplicados_count,
            "exitoso": True
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error duplicando cotización: %s", e)
        raise HTTPException(status_code=500, detail=f"Error duplicando cotización: {str(e)}")

@app.post("/api/debug/duplicacion-detalle")
async def debug_duplicacion_detalle(cotizacion_duplicada: dict):
    """Endpoint para debug detallado de datos recibidos"""
    print("🔍 DEBUG DETALLADO - Datos recibidos para duplicación:")
    
    # Campos críticos que no se están duplicando
    campos_criticos = [
        'origen', 'destino', 'peso_total_kg', 'peso_cargable_kg', 
        'volumen_m3', 'cantidad_pallets'
    ]
    
    datos_recibidos = {}
    for campo in campos_criticos:
        valor = cotizacion_duplicada.get(campo)
        datos_recibidos[campo] = {
            'valor': valor,
            'tipo': type(valor).__name__,
            'esta_presente': campo in cotizacion_duplicada
        }
        print(f"   {campo}: {valor} (tipo: {type(valor)}, presente: {campo in cotizacion_duplicada})")
    
    # Mostrar todos los campos disponibles
    print(f"📋 Todos los campos recibidos: {list(cotizacion_duplicada.keys())}")
    
    return {
        "mensaje": "Debug completado", 
        "campos_criticos": datos_recibidos,
        "todos_los_campos": list(cotizacion_duplicada.keys())
    }


@app.put("/api/cotizaciones/{codigo_legible}")
async def actualizar_cotizacion(codigo_legible: str, cotizacion: dict):
    """Actualizar una cotización existente - CORREGIDO"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        print(f"✏️ Actualizando cotización: {codigo_legible}")
        print(f"📤 Datos recibidos: {cotizacion}")
        
        # Verificar que la cotización existe
        existing_cot = supabase.table("cotizaciones").select("*").eq("codigo_legible", codigo_legible).execute()
        if not existing_cot.data:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")

        # Preparar datos para actualización (excluir campos que no deben actualizarse)
        campos_no_actualizables = ['codigo', 'codigo_legible', 'fecha_creacion', 'id']
        update_data = {k: v for k, v in cotizacion.items() if k not in campos_no_actualizables and v is not None}
        update_data["fecha_actualizacion"] = datetime.now().isoformat()

        print(f"📝 Campos a actualizar: {list(update_data.keys())}")

        # Actualizar en la base de datos
        response = supabase.table("cotizaciones").update(update_data).eq("codigo_legible", codigo_legible).execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="Error al actualizar cotización")

        logger.info(f"✅ Cotización actualizada: {codigo_legible}")
        return {
            "mensaje": "Cotización actualizada exitosamente",
            "codigo": codigo_legible,
            "data": response.data[0]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error actualizando cotización: %s", e)
        raise HTTPException(status_code=500, detail=f"Error actualizando cotización: {str(e)}")



@app.get("/api/debug/routes")
async def debug_routes():
    """Endpoint para depurar las rutas disponibles"""
    routes = []
    for route in app.routes:
        if hasattr(route, "methods"):
            routes.append({
                "path": route.path,
                "methods": list(route.methods),
                "name": route.name
            })
    return {"routes": routes}

@app.get("/api/debug/cotizaciones")
async def debug_cotizaciones():
    """Endpoint de diagnóstico para ver todas las cotizaciones"""
    try:
        if supabase is None:
            return {"error": "Supabase no configurado"}
            
        response = supabase.table("cotizaciones").select("codigo_legible, cliente, estado, fecha_creacion").order("fecha_creacion", desc=True).execute()
        
        return {
            "total": len(response.data),
            "cotizaciones": response.data
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/guardar-pdf-carpeta")
async def guardar_pdf_carpeta(
    archivo: UploadFile = File(...),
    codigo_cotizacion: str = Form(...),
    tipo_pdf: str = Form("interno")
):
    try:
        # Normalizar código para ruta
        codigo_normalizado = codigo_cotizacion.replace('/', '\\')
        codigo_archivo = codigo_cotizacion.replace('/', '_')
        
        print(f"🔍 GUARDAR PDF - Código: {codigo_cotizacion}")

        # Nombre del archivo - usar el mismo formato que ya existe
        fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"{codigo_archivo}_{tipo_pdf}_{fecha}.pdf"

        # Ruta de la carpeta
        ruta_carpeta = os.path.join(BASE_DIR, codigo_normalizado, "Cotizaciones")
        
        print(f"📁 Ruta de carpeta: {ruta_carpeta}")
        print(f"📄 Nombre archivo: {nombre_archivo}")

        # Crear carpeta si no existe
        os.makedirs(ruta_carpeta, exist_ok=True)

        # Ruta completa del PDF
        ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

        # Guardar PDF
        with open(ruta_archivo, "wb") as f:
            f.write(await archivo.read())

        print(f"✅ PDF guardado exitosamente: {ruta_archivo}")

        return {
            "mensaje": "PDF guardado exitosamente",
            "ruta": ruta_archivo,
            "nombre_archivo": nombre_archivo
        }

    except Exception as e:
        print(f"❌ Error guardando PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error guardando PDF: {str(e)}")
    

@app.get("/api/descargar-pdf")
async def descargar_pdf(codigo_cotizacion: str, tipo_pdf: str = "interno"):
    """
    Devuelve el PDF - con debug extendido
    """
    try:
        print(f"🔍 DESCARGAR PDF - INICIANDO")
        print(f"   Código: {codigo_cotizacion}")
        print(f"   Tipo: {tipo_pdf}")
        
        # Normalizar código para ruta
        codigo_normalizado = codigo_cotizacion.replace('/', '\\')
        ruta_carpeta_cotizaciones = os.path.join(BASE_DIR, codigo_normalizado, "Cotizaciones")
        
        print(f"📁 Ruta construida: {ruta_carpeta_cotizaciones}")
        print(f"📁 BASE_DIR: {BASE_DIR}")

        # Verificar si existe la carpeta
        existe_carpeta = os.path.exists(ruta_carpeta_cotizaciones)
        print(f"📁 Carpeta existe: {existe_carpeta}")

        if not existe_carpeta:
            print(f"❌ Carpeta NO existe: {ruta_carpeta_cotizaciones}")
            raise HTTPException(status_code=404, detail="No se encontró la carpeta de cotizaciones")

        # Listar contenido de la carpeta
        try:
            contenido = os.listdir(ruta_carpeta_cotizaciones)
            print(f"📂 Contenido de la carpeta: {contenido}")
        except Exception as e:
            print(f"❌ Error listando carpeta: {e}")
            contenido = []

        # Buscar archivos PDF
        todos_los_pdf = [f for f in contenido if f.lower().endswith('.pdf')]
        print(f"📊 Archivos PDF encontrados: {todos_los_pdf}")

        if not todos_los_pdf:
            print(f"❌ No hay archivos PDF en la carpeta")
            raise HTTPException(status_code=404, detail="No se encontraron archivos PDF")

        # Parámetros de búsqueda
        codigo_busqueda = codigo_cotizacion.replace('/', '_').lower()
        tipo_busqueda = tipo_pdf.lower()
        
        print(f"🔍 Parámetros búsqueda - Código: '{codigo_busqueda}', Tipo: '{tipo_busqueda}'")

        # Filtrar archivos
        archivos_filtrados = []
        for archivo in todos_los_pdf:
            archivo_lower = archivo.lower()
            tiene_codigo = codigo_busqueda in archivo_lower
            tiene_tipo = tipo_busqueda in archivo_lower
            
            print(f"   📄 {archivo} - código: {tiene_codigo}, tipo: {tiene_tipo}")
            
            if tiene_codigo and tiene_tipo:
                archivos_filtrados.append(archivo)

        print(f"🔍 Archivos que coinciden: {archivos_filtrados}")

        # Si no encuentra con ambos criterios, buscar solo por código
        if not archivos_filtrados:
            print(f"🔍 Búsqueda ampliada - solo por código")
            archivos_filtrados = [f for f in todos_los_pdf if codigo_busqueda in f.lower()]
            print(f"🔍 Archivos con código: {archivos_filtrados}")

        # Si aún no encuentra, usar el primer PDF
        if not archivos_filtrados:
            print(f"🔍 Usando primer PDF disponible")
            archivos_filtrados = [todos_los_pdf[0]]

        # Seleccionar el más reciente
        archivos_filtrados.sort(reverse=True)
        archivo_seleccionado = archivos_filtrados[0]
        ruta_archivo = os.path.join(ruta_carpeta_cotizaciones, archivo_seleccionado)
        
        print(f"✅ Archivo seleccionado: {archivo_seleccionado}")
        print(f"📄 Ruta completa: {ruta_archivo}")

        # Verificar que el archivo existe
        if not os.path.exists(ruta_archivo):
            print(f"❌ Archivo NO existe: {ruta_archivo}")
            raise HTTPException(status_code=404, detail="El archivo PDF no existe")

        print(f"✅ Enviando archivo...")
        
        return FileResponse(
            ruta_archivo,
            media_type="application/pdf",
            filename=archivo_seleccionado
        )

    except HTTPException as he:
        print(f"❌ HTTPException: {he.detail}")
        raise he
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        print(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")
    
@app.post("/api/crear_carpeta/")
def crear_carpeta(request: CodigoRequest):
    """
    Crea la carpeta de operación con subcarpetas organizadas
    """
    try:
        carpeta_path = os.path.join(BASE_DIR, request.codigo)
        subcarpetas = ['Cotizaciones', 'Documentos', 'BLs', 'Facturas', 'Otros']
        
        # Crear carpeta principal si no existe
        carpeta_existia = os.path.exists(carpeta_path)
        os.makedirs(carpeta_path, exist_ok=True)
        
        # Crear todas las subcarpetas
        subcarpetas_creadas = []
        for subcarpeta in subcarpetas:
            subcarpeta_path = os.path.join(carpeta_path, subcarpeta)
            if not os.path.exists(subcarpeta_path):
                os.makedirs(subcarpeta_path, exist_ok=True)
                subcarpetas_creadas.append(subcarpeta)
                logger.info("Subcarpeta creada: %s", subcarpeta_path)
        
        mensaje = (
            f"Carpeta '{request.codigo}' creada exitosamente con {len(subcarpetas_creadas)} subcarpetas." 
            if not carpeta_existia else
            f"Carpeta existente. Se crearon {len(subcarpetas_creadas)} subcarpetas faltantes."
        )
        
        return {
            "mensaje": mensaje,
            "path": carpeta_path,
            "subcarpetas_creadas": subcarpetas_creadas,
            "carpeta_nueva": not carpeta_existia
        }
            
    except Exception as e:
        logger.exception("Error creando carpeta: %s", e)
        return {
            "error": f"No se pudo crear la carpeta: {str(e)}",
            "carpeta_nueva": False
        }

@app.post("/api/abrir_carpeta/")
def abrir_carpeta(request: CodigoRequest):
    carpeta_path = os.path.join(BASE_DIR, request.codigo)
    try:
        if not os.path.exists(carpeta_path):
            return {"error": "La carpeta no existe. Créala primero."}

        sistema = platform.system()
        if sistema == "Windows":
            try:
                os.startfile(carpeta_path)
            except Exception as e:
                logger.exception("Error abriendo carpeta en Windows: %s", e)
                return {"error": "No se pudo abrir la carpeta automáticamente en Windows.", "path": carpeta_path}
        elif sistema == "Darwin":
            subprocess.Popen(["open", carpeta_path])
        else:
            # Linux
            try:
                subprocess.Popen(["xdg-open", carpeta_path])
            except Exception as e:
                logger.exception("Error abriendo carpeta en Linux: %s", e)
                return {"error": "No se pudo abrir la carpeta automáticamente en este sistema.", "path": carpeta_path}

        return {"mensaje": f"Carpeta '{request.codigo}' abierta (intento).", "path": carpeta_path}
    except Exception as e:
        logger.exception("Error abriendo carpeta: %s", e)
        return {"error": f"No se pudo abrir la carpeta: {str(e)}"}

async def generar_proximo_numero(tipo_operacion: str) -> str:
    """
    Genera código legible: PREFIJO-YY/MM/NNN de forma correlativa
    """
    try:
        ahora = datetime.now()
        año = ahora.strftime("%y")
        mes = ahora.strftime("%m")
        prefijo = {
            'IA': 'GAN-IA', 'IM': 'GAN-IM', 'EA': 'GAN-EA',
            'EM': 'GAN-EM', 'IT': 'GAN-IT', 'ET': 'GAN-ET', 'MC': 'GAN-MC', 'CO': 'GAN-CO'
        }.get(tipo_operacion, 'GAN-XX')

        patron_busqueda = f"{prefijo}-{año}/{mes}/"
        if supabase is None:
            # fallback
            return f"{patron_busqueda}001"

        # Buscar códigos con el prefijo en Supabase
        response = supabase.table("cotizaciones").select("codigo_legible").like("codigo_legible", f"{patron_busqueda}%").execute()
        numeros_existentes = []
        for cot in (response.data or []):
            codigo = cot.get("codigo_legible", "")
            match = re.search(r'/(\d+)$', codigo)
            if match:
                try:
                    numeros_existentes.append(int(match.group(1)))
                except ValueError:
                    continue

        proximo_numero = (max(numeros_existentes) + 1) if numeros_existentes else 1
        numero_formateado = f"{proximo_numero:03d}"
        return f"{patron_busqueda}{numero_formateado}"
    except Exception as e:
        logger.exception("Error generando proximo numero: %s", e)
        ahora = datetime.now()
        año = ahora.strftime("%y")
        mes = ahora.strftime("%m")
        prefijo = {
            'IA': 'GAN-IA', 'IM': 'GAN-IM', 'EA': 'GAN-EA',
            'EM': 'GAN-EM', 'IT': 'GAN-IT', 'ET': 'GAN-ET', 'MC': 'GAN-MC', 'CO': 'GAN-CO'
        }.get(tipo_operacion, 'GAN-XX')
        return f"{prefijo}-{año}/{mes}/001"

def calcular_estado_y_validez(fecha_validez: Any, validez_dias: int, estado_actual_db: str = None) -> Dict[str, Any]:
    """
    Calcula el estado considerando la fecha de validez, pero respeta estados manuales
    CORREGIDO: Cuando dias_restantes = 0, estado debe ser 'vencida'
    """
    try:
        print(f"🔍 Calculando estado: estado_db={estado_actual_db}, fecha_validez={fecha_validez}")
        
        # ✅ RESPETAR TODOS LOS ESTADOS MANUALES
        if estado_actual_db in ['creada', 'enviada', 'aceptada', 'rechazada']:
            print(f"✅ Respetando estado manual: {estado_actual_db}")
            dias_restantes = 0
            
            # Solo calcular días restantes si hay fecha de validez
            if fecha_validez:
                if isinstance(fecha_validez, str):
                    fecha_validez = datetime.fromisoformat(fecha_validez.replace('Z', '+00:00'))
                hoy = datetime.now().date()
                fecha_validez_date = fecha_validez.date() if isinstance(fecha_validez, datetime) else fecha_validez
                dias_restantes = (fecha_validez_date - hoy).days
            
            return {
                'estado': estado_actual_db, 
                'dias_restantes': dias_restantes, 
                'color': ESTADOS_COTIZACION.get(estado_actual_db, {'color': '#f97316'})['color']
            }

        # Solo calcular automáticamente para estados que dependen de la fecha
        # o para cotizaciones sin estado definido
        if not fecha_validez:
            return {
                'estado': 'creada',
                'dias_restantes': validez_dias,
                'color': ESTADOS_COTIZACION['creada']['color']
            }

        if isinstance(fecha_validez, str):
            fecha_validez = datetime.fromisoformat(fecha_validez.replace('Z', '+00:00'))
        
        hoy = datetime.now().date()
        fecha_validez_date = fecha_validez.date() if isinstance(fecha_validez, datetime) else fecha_validez
        dias_restantes = (fecha_validez_date - hoy).days

        print(f"📅 Días restantes calculados: {dias_restantes}")

        # ✅ CORRECCIÓN: Si dias_restantes es 0 o negativo, está VENCIDA
        if dias_restantes < 0:
            nuevo_estado = 'vencida'
        elif dias_restantes == 0:  # ¡CORREGIDO! Hoy es el día de vencimiento
            nuevo_estado = 'vencida'
        elif dias_restantes <= 2:
            nuevo_estado = 'por_vencer'
        else:
            # Si no tiene estado y no está vencida, mantener como estaba o poner 'creada'
            nuevo_estado = estado_actual_db or 'creada'

        print(f"🎯 Estado final: {nuevo_estado}")
            
        return {
            'estado': nuevo_estado, 
            'dias_restantes': dias_restantes, 
            'color': ESTADOS_COTIZACION.get(nuevo_estado, {'color': '#f97316'})['color']
        }
        
    except Exception as e:
        logger.exception("Error calculando validez: %s", e)
        return {
            'estado': estado_actual_db or 'creada', 
            'dias_restantes': validez_dias, 
            'color': ESTADOS_COTIZACION['creada']['color']
        }  

async def enviar_notificacion(cotizacion: Dict, tipo_alerta: str, mensaje: Optional[str] = None):
    """
    Guarda una notificación en la tabla 'notificaciones' de Supabase.
    """
    try:
        if supabase is None:
            logger.info("Supabase no configurado - notificacion: %s - %s", tipo_alerta, cotizacion.get('codigo_legible'))
            return

        noti = {
            "cotizacion_codigo": cotizacion.get('codigo_legible', cotizacion.get('codigo')),
            "tipo": tipo_alerta,
            "mensaje": mensaje or f"Alerta {tipo_alerta} para {cotizacion.get('codigo_legible')}",
            "fecha": datetime.now().isoformat(),
            "leido": False
        }
        resp = supabase.table("notificaciones").insert(noti).execute()
        logger.info("Notificación guardada: %s (resp: %s rows)", noti['cotizacion_codigo'], len(resp.data) if resp and resp.data else 0)
    except Exception as e:
        logger.exception("Error enviando notificacion: %s", e)  

def get_linea_id_by_nombre(nombre_linea: str) -> Optional[int]:
    """Busca el ID de una línea marítima por su nombre."""
    try:
        response = supabase.table("lineas_maritimas") \
            .select("id") \
            .eq("nombre", nombre_linea) \
            .single() \
            .execute()
        return response.data["id"]
    except Exception as e:
        logger.warning(f"Línea marítima '{nombre_linea}' no encontrada: {e}")
        return None

# -----------------------
# Scheduler: loop que verifica vencimientos periódicamente
# -----------------------
async def verificar_vencimientos_loop(interval_seconds: int = 300):
    """
    Loop que corre en background (asyncio task) para verificar estados.
    Por defecto corre cada 300s (5 min). Solo activo si ENV == 'development' (configurable).
    """
    logger.info("Iniciando loop de verificación de vencimientos (interval %s s)", interval_seconds)
    while True:
        try:
            hoy = datetime.now().date()
            hasta_iso = (hoy + timedelta(days=2)).isoformat()
            if supabase is None:
                logger.debug("No hay supabase configurado; saltando verificación.")
            else:
                response = supabase.table("cotizaciones")\
                    .select("*")\
                    .lte("fecha_validez", hasta_iso)\
                    .neq("estado", "vencida")\
                    .neq("estado", "aceptada")\
                    .neq("estado", "rechazada")\
                    .execute()

                for cot in (response.data or []):
                    estado_info = calcular_estado_y_validez(cot.get('fecha_validez'), cot.get('validez_dias', 30))
                    if estado_info['estado'] != cot.get('estado'):
                        # actualizar estado en DB
                        supabase.table("cotizaciones").update({"estado": estado_info['estado']}).eq("codigo_legible", cot['codigo_legible']).execute()
                        # crear notificación
                        await enviar_notificacion(cot, f"estado_{estado_info['estado']}", f"Cotización {cot['codigo_legible']} pasó a {estado_info['estado']}")
                        logger.info("Cot %s actualizado a %s", cot.get('codigo_legible'), estado_info['estado'])
        except Exception as e:
            logger.exception("Error en loop de verificación: %s", e)
        await asyncio.sleep(interval_seconds)

# Start loop on startup (only in development by default)
@app.on_event("startup")
async def startup_event():
    logger.info("Iniciando Ganbatte API (ENV=%s)", ENV)
     # if ENV == "development":
    #     # Start background loop
    #     asyncio.create_task(verificar_vencimientos_loop(300))
    #     logger.info("Scheduler de verificación lanzado (cada 5 minutos).")

# -----------------------
# Endpoints CORREGIDOS
# -----------------------

# -----------------------
# Endpoints de Clientes
# -----------------------

@app.get("/api/gastos_locales_maritimos_combinado/{tipo_operacion}/{linea_maritima}/{equipo}")
async def get_gastos_locales_maritimos_combinado(tipo_operacion: str, linea_maritima: str, equipo: str):
    try:
        # Costos de la línea real (ej: COSCO)
        costos_resp = supabase.table("gastos_locales_maritimos") \
            .select("*") \
            .eq("tipo_operacion", tipo_operacion) \
            .eq("linea_maritima", linea_maritima) \
            .eq("equipo", equipo) \
            .execute()
        
        # Venta (línea GANBATTE)
        venta_resp = supabase.table("gastos_locales_maritimos") \
            .select("*") \
            .eq("tipo_operacion", tipo_operacion) \
            .eq("linea_maritima", "GANBATTE") \
            .eq("equipo", equipo) \
            .execute()
        
        costos = costos_resp.data[0] if costos_resp.data else None
        venta = venta_resp.data[0] if venta_resp.data else None

        if not costos:
            return JSONResponse(status_code=404, content={"message": "No se encontraron costos para la línea solicitada."})

        return {
            "costos": costos,
            "venta": venta or {}
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/clientes", response_model=Cliente)
async def crear_cliente(cliente: ClienteCreate):
    """Crear un nuevo cliente"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Verificar si el cliente ya existe (por CUIT o email)
        if cliente.cuit:
            existing_cliente = supabase.table("clientes").select("*").eq("cuit", cliente.cuit).execute()
            if existing_cliente.data:
                raise HTTPException(status_code=400, detail="Ya existe un cliente con este CUIT")

        if cliente.email:
            existing_cliente = supabase.table("clientes").select("*").eq("email", cliente.email).execute()
            if existing_cliente.data:
                raise HTTPException(status_code=400, detail="Ya existe un cliente con este email")

        # Preparar datos para inserción
        cliente_data = cliente.dict()
        cliente_data.update({
            "id": str(uuid4()),
            "fecha_creacion": datetime.now().isoformat(),
            "fecha_actualizacion": datetime.now().isoformat()
        })

        # Insertar en la base de datos
        response = supabase.table("clientes").insert(cliente_data).execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="Error al crear cliente")

        logger.info(f"Cliente creado: {cliente_data['nombre']}")
        return response.data[0]

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error creando cliente: %s", e)
        raise HTTPException(status_code=500, detail=f"Error creando cliente: {str(e)}")
    
    
@app.get("/api/clientes")
async def listar_clientes(activo: Optional[bool] = None, search: Optional[str] = None):
    """Obtener lista de clientes con filtros opcionales"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        query = supabase.table("clientes").select("*")

        # Aplicar filtros
        if activo is not None:
            query = query.eq("activo", activo)
        
        if search:
            query = query.or_(f"nombre.ilike.%{search}%,email.ilike.%{search}%,cuit.ilike.%{search}%")

        # Ordenar por nombre
        query = query.order("nombre", desc=False)

        response = query.execute()
        
        return response.data or []

    except Exception as e:
        logger.exception("Error listando clientes: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener clientes: {str(e)}")

@app.get("/api/clientes/{cliente_id}")
async def obtener_cliente(cliente_id: str):
    """Obtener un cliente específico por ID"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        response = supabase.table("clientes").select("*").eq("id", cliente_id).execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")

        return response.data[0]

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error obteniendo cliente: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener cliente: {str(e)}")

@app.put("/api/clientes/{cliente_id}")
async def actualizar_cliente(cliente_id: str, cliente_update: ClienteUpdate):
    """Actualizar un cliente existente"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Verificar que el cliente existe
        existing_cliente = supabase.table("clientes").select("*").eq("id", cliente_id).execute()
        if not existing_cliente.data:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")

        # Preparar datos para actualización
        update_data = cliente_update.dict(exclude_unset=True)
        update_data["fecha_actualizacion"] = datetime.now().isoformat()

        # Actualizar en la base de datos
        response = supabase.table("clientes").update(update_data).eq("id", cliente_id).execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="Error al actualizar cliente")

        logger.info(f"Cliente actualizado: {cliente_id}")
        return response.data[0]

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error actualizando cliente: %s", e)
        raise HTTPException(status_code=500, detail=f"Error actualizando cliente: {str(e)}")

@app.delete("/api/clientes/{cliente_id}")
async def desactivar_cliente(cliente_id: str):
    """Desactivar un cliente (eliminación lógica)"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Verificar que el cliente existe
        existing_cliente = supabase.table("clientes").select("*").eq("id", cliente_id).execute()
        if not existing_cliente.data:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")

        # Desactivar cliente (eliminación lógica)
        response = supabase.table("clientes").update({
            "activo": False,
            "fecha_actualizacion": datetime.now().isoformat()
        }).eq("id", cliente_id).execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="Error al desactivar cliente")

        logger.info(f"Cliente desactivado: {cliente_id}")
        return {"mensaje": "Cliente desactivado exitosamente"}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error desactivando cliente: %s", e)
        raise HTTPException(status_code=500, detail=f"Error desactivando cliente: {str(e)}")

@app.get("/api/clientes/{cliente_id}/cotizaciones")
async def obtener_cotizaciones_cliente(cliente_id: str):
    """Obtener todas las cotizaciones de un cliente específico"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Verificar que el cliente existe
        cliente = supabase.table("clientes").select("*").eq("id", cliente_id).execute()
        if not cliente.data:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")

        # Obtener cotizaciones del cliente (buscando por nombre del cliente)
        cliente_nombre = cliente.data[0]["nombre"]
        response = supabase.table("cotizaciones").select("*").eq("cliente", cliente_nombre).order("fecha_creacion", desc=True).execute()
        
        # Procesar cotizaciones para incluir información de estado
        cotizaciones_procesadas = []
        for cot in (response.data or []):
            try:
                cot_data = cot.copy()
                
                # Usar codigo legible si existe
                if cot_data.get('codigo_legible'):
                    cot_data['codigo'] = cot_data['codigo_legible']
                else:
                    # fallback retroactivo
                    tipo_op = cot_data.get('tipo_operacion', 'XX')
                    prefijo = {
                        'IA': 'GAN-IA', 'IM': 'GAN-IM', 'EA': 'GAN-EA',
                        'EM': 'GAN-EM', 'IT': 'GAN-IT', 'ET': 'GAN-ET','MC': 'GAN-MC', 'CO': 'GAN-CO'
                    }.get(tipo_op, 'GAN-XX')
                    fecha = cot_data.get('fecha_creacion', datetime.now())
                    if isinstance(fecha, str):
                        try:
                            fecha = datetime.fromisoformat(fecha.replace('Z', '+00:00'))
                        except Exception:
                            fecha = datetime.now()
                    año = fecha.strftime("%y")
                    mes = fecha.strftime("%m")
                    cot_data['codigo'] = f"{prefijo}-{año}/{mes}/R01"

                # Calcular estado
                estado_actual_db = cot_data.get('estado')
                estado_info = calcular_estado_y_validez(
                    cot_data.get('fecha_validez'), 
                    cot_data.get('validez_dias', 30),
                    estado_actual_db
                )
                
                cot_data['estado_actual'] = estado_info['estado']
                cot_data['color'] = estado_info['color']
                cot_data['dias_restantes'] = estado_info['dias_restantes']
                cot_data['label_estado'] = ESTADOS_COTIZACION.get(estado_info['estado'], {'label': '🔵 ENVIADA'})['label']
                
                cotizaciones_procesadas.append(cot_data)
                
            except Exception as e:
                logger.error(f"Error procesando cotización {cot.get('id')}: {e}")
                continue

        return cotizaciones_procesadas

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error obteniendo cotizaciones del cliente: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener cotizaciones del cliente: {str(e)}")

# --- FUNCIÓN DE UTILIDAD ---
# Asegúrese de definir esta función antes del endpoint get_costos_fcl_locales
def map_to_concepts(data: Dict[str, Any], is_costo: bool, tipo_operacion: str) -> List[Dict[str, Any]]:
    """Mapea una fila de gastos_locales_maritimos a una lista de conceptos estructurados (Costo/Venta)."""
    if not data:
        return []
        
    concepts = []
    # Columnas de la DB a mapear
    fields = [
        'thc', 'toll', 'gate', 'delivery_order', 'ccf', 'handling', 
        'logistic_fee', 'bl_fee', 'ingreso_sim', 'cert_flete', 'cert_fob'
    ]
    
    mapeo_conceptos = {
        'thc': 'THC (Terminal Handling Charge)',
        'toll': 'Toll Fee',
        'gate': 'Gate Fee',
        'delivery_order': 'Delivery Order',
        'ccf': 'CCF (Container Cleaning Fee)',
        'handling': 'Handling',
        'logistic_fee': 'Logistic Fee',
        'bl_fee': 'BL Fee',
        'ingreso_sim': 'Ingreso SIM',
        'cert_flete': 'Certificado de Flete',
        'cert_fob': 'Certificado FOB',
    }
    
    for db_key in fields:
        value = data.get(db_key)
        # Solo incluye conceptos con un valor positivo
        if value is not None and float(value) > 0:
            concepts.append({
                "concepto": mapeo_conceptos.get(db_key, db_key.replace('_', ' ').title()),
                "costo": float(value) if is_costo else 0,  # El valor es COSTO si es la consulta de la línea de la cotización
                "venta": 0 if is_costo else float(value),   # El valor es VENTA si es la consulta de GANBATTE
                "es_predefinido": True,
                "tipo": "Maritima FCL",
                "detalles": {
                    "db_campo": db_key,
                    "linea_maritima": data.get('linea_maritima'),
                    "equipo": data.get('equipo'),
                    "tipo_registro": "COSTO" if is_costo else "VENTA"
                }
            })
    return concepts


# --- ENDPOINT CORREGIDO (Reemplazar la versión anterior) ---


@app.get("/api/costos-maritimos-fcl-locales")
def get_costos_maritimos_fcl_locales(tipo_operacion: str, equipo: str, linea_maritima: str):
    """
    Devuelve costos (línea seleccionada) y ventas (GANBATTE) sin importar mayúsculas/minúsculas
    """
    # Normalizar los strings a mayúsculas
    tipo_operacion = tipo_operacion.upper()
    equipo = equipo.upper()
    linea_maritima = linea_maritima.upper()

    # COSTOS base
    response_costos = supabase.table("gastos_locales_maritimos").select("*") \
        .eq("tipo_operacion", tipo_operacion) \
        .eq("equipo", equipo) \
        .eq("linea_maritima", linea_maritima) \
        .execute()
    costos_base = response_costos.data or []

    # VENTAS base (GANBATTE)
    response_ventas = supabase.table("gastos_locales_maritimos").select("*") \
        .eq("tipo_operacion", tipo_operacion) \
        .eq("equipo", equipo) \
        .eq("linea_maritima", "GANBATTE") \
        .execute()
    ventas_base = response_ventas.data or []

    return {"costos_base": costos_base, "ventas_base": ventas_base}

@app.post("/api/costos_personalizados/guardar")
async def guardar_costos_personalizados(solicitud: dict):
    """Guarda o actualiza costos personalizados para una cotización."""
    if supabase is None:
        raise HTTPException(status_code=503, detail="Base de datos no disponible.")

    try:
        codigo_cotizacion = solicitud.get("codigo_cotizacion")
        costos = solicitud.get("costos", [])
        
        print(f"💾 [GUARDAR_COSTOS] INICIANDO - Código: {codigo_cotizacion}")
        print(f"📦 [GUARDAR_COSTOS] Costos a guardar: {len(costos)}")
        
        if not codigo_cotizacion:
            raise HTTPException(status_code=400, detail="Código de cotización requerido")
        
        if not costos:
            raise HTTPException(status_code=400, detail="Lista de costos vacía")

        # 1. VERIFICAR CONEXIÓN A LA BD
        print("🔍 [GUARDAR_COSTOS] Verificando conexión a Supabase...")
        try:
            test_response = supabase.table("costos_cotizacion").select("count", count="exact").limit(1).execute()
            print(f"✅ [GUARDAR_COSTOS] Conexión OK. Tabla existe.")
        except Exception as e:
            print(f"❌ [GUARDAR_COSTOS] Error conectando a BD: {e}")
            raise

        # 2. ELIMINAR COSTOS EXISTENTES
        print("🗑️ [GUARDAR_COSTOS] Eliminando costos existentes...")
        try:
            delete_response = supabase.table("costos_cotizacion").delete().eq(
                "codigo_cotizacion", codigo_cotizacion
            ).execute()
            deleted_count = len(delete_response.data) if delete_response.data else 0
            print(f"✅ [GUARDAR_COSTOS] Costos eliminados: {deleted_count}")
        except Exception as e:
            print(f"❌ [GUARDAR_COSTOS] Error eliminando costos: {e}")
            # Continuar de todos modos

        # 3. PREPARAR DATOS PARA INSERTAR
        costos_con_fecha = []
        for i, costo in enumerate(costos):
            costo_data = {
                "codigo_cotizacion": codigo_cotizacion,
                "concepto": costo.get("concepto", f"Concepto {i+1}"),
                "costo": float(costo.get("costo", 0)),
                "venta": float(costo.get("venta", 0)),
                "es_predefinido": bool(costo.get("es_predefinido", False)),
                "tipo": costo.get("tipo", "Otro"),
                "detalles": costo.get("detalles", {}),
                "fecha_creacion": datetime.now().isoformat()
            }
            costos_con_fecha.append(costo_data)

        # 4. INSERTAR NUEVOS COSTOS
        print(f"💾 [GUARDAR_COSTOS] Insertando {len(costos_con_fecha)} costos...")
        try:
            insert_response = supabase.table("costos_cotizacion").insert(costos_con_fecha).execute()
            
            if hasattr(insert_response, 'data') and insert_response.data:
                inserted_count = len(insert_response.data)
                print(f"✅ [GUARDAR_COSTOS] INSERT EXITOSO: {inserted_count} costos insertados")
                
                # Mostrar los IDs generados
                for i, costo in enumerate(insert_response.data):
                    print(f"   📝 [{i+1}] ID: {costo.get('id')} - {costo.get('concepto')}")
            else:
                print(f"❌ [GUARDAR_COSTOS] INSERT FALLIDO. Respuesta: {insert_response}")
                return {"mensaje": "No se pudieron insertar los costos", "count": 0}
                
        except Exception as e:
            print(f"❌ [GUARDAR_COSTOS] Error en INSERT: {e}")
            raise

        # 5. VERIFICACIÓN INMEDIATA
        print("🔍 [GUARDAR_COSTOS] Verificación INMEDIATA en BD...")
        try:
            verify_response = supabase.table("costos_cotizacion").select("*").eq(
                "codigo_cotizacion", codigo_cotizacion
            ).execute()
            
            verified_count = len(verify_response.data) if verify_response.data else 0
            print(f"📊 [GUARDAR_COSTOS] Verificación: {verified_count} costos en BD")
            
            if verified_count > 0:
                for i, costo in enumerate(verify_response.data):
                    print(f"   ✅ [{i+1}] {costo.get('concepto')} - ID: {costo.get('id')}")
            else:
                print("❌ [GUARDAR_COSTOS] VERIFICACIÓN FALLIDA: No hay costos en BD")
                
        except Exception as e:
            print(f"❌ [GUARDAR_COSTOS] Error en verificación: {e}")

        # 6. VERIFICACIÓN FINAL CON COUNT
        print("🔍 [GUARDAR_COSTOS] Verificación FINAL con COUNT...")
        try:
            count_response = supabase.table("costos_cotizacion").select("id", count="exact").eq(
                "codigo_cotizacion", codigo_cotizacion
            ).execute()
            
            final_count = count_response.count if hasattr(count_response, 'count') else 0
            print(f"🎯 [GUARDAR_COSTOS] COUNT FINAL: {final_count} costos")
            
        except Exception as e:
            print(f"❌ [GUARDAR_COSTOS] Error en COUNT: {e}")
            final_count = 0

        return {
            "mensaje": f"Costos guardados exitosamente: {inserted_count} registros",
            "count": inserted_count,
            "verificados_en_bd": verified_count,
            "count_final": final_count,
            "codigo_cotizacion": codigo_cotizacion
        }
        
    except Exception as e:
        print(f"❌ [GUARDAR_COSTOS] ERROR GENERAL: {e}")
        import traceback
        print(f"🔍 [GUARDAR_COSTOS] Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error al guardar costos: {str(e)}")

@app.get("/api/costos_personalizados/{codigo_cotizacion:path}") # <--- ¡CAMBIO AQUÍ!
async def get_costos_personalizados(codigo_cotizacion: str):
    """Obtiene los costos personalizados guardados para una cotización específica."""
    if supabase is None:
        raise HTTPException(status_code=503, detail="Base de datos no disponible.")

    try:
        # DECODIFICAR EL CÓDIGO (puede venir doblemente codificado)
        from urllib.parse import unquote
        codigo_decodificado = unquote(codigo_cotizacion)
        
        # Si todavía tiene %2F, decodificar otra vez
        if '%2F' in codigo_decodificado:
            codigo_decodificado = unquote(codigo_decodificado)
            
        print(f"🔍 [COSTOS_PERSONALIZADOS] Código recibido: '{codigo_cotizacion}'")
        print(f"🔍 [COSTOS_PERSONALIZADOS] Código decodificado: '{codigo_decodificado}'")
        
        # Buscar en la base de datos con el código decodificado
        response = supabase.table("costos_cotizacion").select("*").eq(
            "codigo_cotizacion", codigo_decodificado
        ).order("fecha_creacion", desc=False).execute()

        print(f"📊 [COSTOS_PERSONALIZADOS] Resultados de la consulta: {len(response.data)}")
        
        if not response.data:
            print(f"❌ [COSTOS_PERSONALIZADOS] No se encontraron costos para: {codigo_decodificado}")
            # En lugar de 404, devolver array vacío
            return []
        
        print(f"✅ [COSTOS_PERSONALIZADOS] Costos encontrados: {len(response.data)}")
        
        # Formatear los datos para el frontend
        costos_formateados = [
            {
                **costo, 
                'id': str(costo['id']),
                'detalles': costo.get('detalles', {}) or {}
            } 
            for costo in response.data
        ]

        return costos_formateados
        
    except Exception as e:
        print(f"❌ [COSTOS_PERSONALIZADOS] Error: {e}")
        # En caso de error, devolver array vacío
        return []
    
# -----------------------
# Endpoints para Costos (CORREGIDOS - solo GET)
# -----------------------

@app.get("/api/tasas_cambio")
async def obtener_tasas_cambio():
    """
    Obtener tasas de cambio actualizadas desde una API externa
    """
    try:
        logger.info("📊 Solicitando tasas de cambio desde API externa")
        
        # Obtener fecha actual ANTES de cualquier operación
        fecha_actual = datetime.now().isoformat()
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.frankfurter.app/latest?from=USD",
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                tasas_cambio = {
                    "USD": 1.0,
                    "ARS": data["rates"].get("ARS", 1473.17),
                    "EUR": data["rates"].get("EUR", 0.87),
                    "GBP": data["rates"].get("GBP", 0.77),
                    "BRL": data["rates"].get("BRL", 5.40),
                    "fecha_actualizacion": fecha_actual,  # Usar la variable ya calculada
                    "fuente": "Frankfurter API"
                }
            else:
                # Fallback
                tasas_cambio = {
                    "USD": 1.0,
                    "ARS": 1473.17,
                    "EUR": 0.87,
                    "GBP": 0.77,
                    "BRL": 5.40,
                    "fecha_actualizacion": fecha_actual,  # Usar la variable ya calculada
                    "fuente": "Fallback - Error API"
                }
        
        logger.info("✅ Tasas de cambio obtenidas exitosamente")
        return tasas_cambio
        
    except Exception as e:
        logger.warning("⚠️ Error obteniendo tasas externas, usando valores por defecto: %s", e)
        # Fallback seguro con fecha actual
        fecha_actual = datetime.now().isoformat()
        return {
            "USD": 1.0,
            "ARS": 1473.17,
            "EUR": 0.87,
            "GBP": 0.77,
            "BRL": 5.40,
            "fecha_actualizacion": fecha_actual,  # Usar la variable ya calculada
            "fuente": f"Fallback por error: {str(e)}"
        }

@app.get("/api/costos-predefinidos")
async def get_costos_predefinidos(
    tipo_operacion: str, 
    incoterm: Optional[str] = None, 
    modo_transporte: Optional[str] = None
):
    """Obtener costos predefinidos según tipo de operación e incoterm (GET)"""
    try:
        logger.info(f"📦 Solicitando costos predefinidos: {tipo_operacion}, {incoterm}, {modo_transporte}")
        
        # Datos de ejemplo - reemplaza con tu lógica real
        conceptos_ejemplo = []
        
        if tipo_operacion in ['IM', 'EM']:  # Marítimo
            conceptos_ejemplo = [
                {
                    "id": 1,
                    "concepto": "Flete Marítimo Internacional",
                    "costo_base": 0,
                    "precio_sugerido": 0,
                    "tipo_operacion": tipo_operacion,
                    "incoterm": incoterm,
                    "modo_transporte": modo_transporte
                },
                {
                    "id": 2, 
                    "concepto": "Agencia Marítima",
                    "costo_base": 150,
                    "precio_sugerido": 250,
                    "tipo_operacion": tipo_operacion,
                    "incoterm": incoterm,
                    "modo_transporte": modo_transporte
                },
                {
                    "id": 3,
                    "concepto": "Despacho de Aduana",
                    "costo_base": 100,
                    "precio_sugerido": 180,
                    "tipo_operacion": tipo_operacion,
                    "incoterm": incoterm,
                    "modo_transporte": modo_transporte
                }
            ]
        elif tipo_operacion in ['IA', 'EA']:  # Aéreo
            conceptos_ejemplo = [
                {
                    "id": 4,
                    "concepto": "Flete Aéreo Internacional", 
                    "costo_base": 0,
                    "precio_sugerido": 0,
                    "tipo_operacion": tipo_operacion,
                    "incoterm": incoterm,
                    "modo_transporte": modo_transporte
                },
                {
                    "id": 5,
                    "concepto": "Agencia de Carga Aérea",
                    "costo_base": 120,
                    "precio_sugerido": 200,
                    "tipo_operacion": tipo_operacion,
                    "incoterm": incoterm,
                    "modo_transporte": modo_transporte
                }
            ]
        else:  # Terrestre u otros
            conceptos_ejemplo = [
                {
                    "id": 6,
                    "concepto": "Flete Terrestre",
                    "costo_base": 0,
                    "precio_sugerido": 0,
                    "tipo_operacion": tipo_operacion,
                    "incoterm": incoterm,
                    "modo_transporte": modo_transporte
                }
            ]
        
        logger.info(f"✅ Retornando {len(conceptos_ejemplo)} conceptos predefinidos")
        return conceptos_ejemplo
        
    except Exception as e:
        logger.exception("Error obteniendo costos predefinidos: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/costos-ganbatte")
async def get_costos_ganbatte(tipo_operacion: str, equipo: str):
    """Obtener costos de Ganbatte como referencia para Marítima FCL (GET)"""
    try:
        logger.info(f"📦 Solicitando costos Ganbatte: {tipo_operacion}, {equipo}")
        
        # Mapear tipos de operación a los valores de la BD
        
        tipo_op_bd = op_map.get(tipo_operacion)
        if not tipo_op_bd:
            logger.warning(f"Tipo de operación no válido para costos marítimos: {tipo_operacion}")
            return {
                "thc": 0, "toll": 0, "gate": 0, "delivery_order": 0, "ccf": 0,
                "handling": 0, "logistic_fee": 0, "bl_fee": 0, "ingreso_sim": 0,
                "cert_flete": 0, "cert_fob": 0, "total_locales": 0
            }

        if supabase is None:
            logger.warning("Supabase no configurado - retornando datos de ejemplo")
            # Datos de ejemplo para desarrollo
            return {
                "thc": 450, "toll": 120, "gate": 80, "delivery_order": 45, "ccf": 25,
                "handling": 150, "logistic_fee": 100, "bl_fee": 75, "ingreso_sim": 30,
                "cert_flete": 40, "cert_fob": 35, "total_locales": 1150
            }

        # Consultar costos de Ganbatte en la BD
        response = supabase.table("gastos_locales_maritimos")\
            .select("*")\
            .eq("linea_maritima", "GANBATTE")\
            .eq("tipo_operacion", tipo_op_bd)\
            .eq("equipo", equipo)\
            .execute()

        if not response.data:
            logger.warning(f"No se encontraron costos Ganbatte para {tipo_op_bd}/{equipo}")
            return {
                "thc": 0, "toll": 0, "gate": 0, "delivery_order": 0, "ccf": 0,
                "handling": 0, "logistic_fee": 0, "bl_fee": 0, "ingreso_sim": 0,
                "cert_flete": 0, "cert_fob": 0, "total_locales": 0
            }

        # Retornar el primer resultado (debería ser único)
        costo_ganbatte = response.data[0]
        logger.info(f"✅ Costos Ganbatte encontrados: {len(costo_ganbatte)} campos")
        
        return {
            "thc": costo_ganbatte.get("thc", 0),
            "toll": costo_ganbatte.get("toll", 0),
            "gate": costo_ganbatte.get("gate", 0),
            "delivery_order": costo_ganbatte.get("delivery_order", 0),
            "ccf": costo_ganbatte.get("ccf", 0),
            "handling": costo_ganbatte.get("handling", 0),
            "logistic_fee": costo_ganbatte.get("logistic_fee", 0),
            "bl_fee": costo_ganbatte.get("bl_fee", 0),
            "ingreso_sim": costo_ganbatte.get("ingreso_sim", 0),
            "cert_flete": costo_ganbatte.get("cert_flete", 0),
            "cert_fob": costo_ganbatte.get("cert_fob", 0),
            "total_locales": costo_ganbatte.get("total_locales", 0)
        }
        
    except Exception as e:
        logger.exception("Error obteniendo costos Ganbatte: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    
# Agregar estos endpoints a tu FastAPI

@app.get("/api/ia/alertas-proactivas/{codigo_operacion:path}")
async def obtener_alertas_proactivas(codigo_operacion: str):
    """Genera alertas proactivas basadas en el estado de la operación"""
    try:
        # Obtener datos de la operación
        op_resp = supabase.table("operaciones").select("*").eq("codigo_operacion", codigo_operacion).execute()
        if not op_resp.data:
            return {"alertas": []}
        
        operacion = op_resp.data[0]
        datos = operacion.get("datos_cotizacion", {})
        
        alertas = []
        
        # Alertas por datos faltantes críticos
        if not datos.get('etd') or not datos.get('eta'):
            alertas.append({
                "id": 1,
                "titulo": "Fechas críticas faltantes",
                "mensaje": "Complete ETD y ETA para habilitar seguimiento automático",
                "nivel": "critico",
                "timestamp": datetime.now().isoformat(),
                "accion": "Completar Fechas"
            })
        
        # Alertas por proximidad de ETD
        if datos.get('etd'):
            etd = datetime.fromisoformat(datos['etd'].replace('Z', '+00:00'))
            dias_hasta_etd = (etd - datetime.now()).days
            if dias_hasta_etd <= 2:
                alertas.append({
                    "id": 2,
                    "titulo": "ETD próximo",
                    "mensaje": f"ETD en {dias_hasta_etd} día(s). Confirme documentación",
                    "nivel": "advertencia",
                    "timestamp": datetime.now().isoformat(),
                    "accion": "Verificar Documentación"
                })
        
        return {"alertas": alertas}
        
    except Exception as e:
        logger.exception(f"Error generando alertas: {e}")
        return {"alertas": []}

@app.get("/api/operaciones/{codigo_operacion:path}/estadisticas")
async def obtener_estadisticas_operacion(codigo_operacion: str):
    """Calcula estadísticas de la operación"""
    try:
        # Lógica para calcular progreso, tareas pendientes, etc.
        return {
            "progreso": 65,
            "tareasPendientes": 2,
            "documentosFaltantes": 1
        }
    except Exception as e:
        logger.exception(f"Error obteniendo estadísticas: {e}")
        return {"progreso": 0, "tareasPendientes": 0, "documentosFaltantes": 0}

@app.get("/api/costos-linea-maritima")
async def get_costos_linea_maritima(
    tipo_operacion: str, 
    linea_maritima: str, 
    equipo: str
):
    """Obtener costos por línea marítima específica (GET)"""
    try:
        logger.info(f"📦 Solicitando costos línea: {tipo_operacion}, {linea_maritima}, {equipo}")
        
        tipo_op_bd = op_map.get(tipo_operacion)
        if not tipo_op_bd:
            raise HTTPException(status_code=400, detail="Tipo de operación no válido")

        if supabase is None:
            logger.warning("Supabase no configurado - retornando lista vacía")
            return []

        # Consultar costos en la BD
        response = supabase.table("gastos_locales_maritimos")\
            .select("*")\
            .eq("linea_maritima", linea_maritima)\
            .eq("tipo_operacion", tipo_op_bd)\
            .eq("equipo", equipo)\
            .execute()

        if not response.data:
            logger.warning(f"No se encontraron costos para {linea_maritima}/{tipo_op_bd}/{equipo}")
            return []
            
        logger.info(f"✅ Costos línea encontrados: {len(response.data)} registros")
        return response.data
        
    except Exception as e:
        logger.exception("Error obteniendo costos línea marítima: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/costos-automaticos")
async def get_costos_automaticos(
    tipo_operacion: str, 
    modo_transporte: str, 
    equipo: str, 
    linea_maritima: str = None
):
    """Endpoint principal para cargar costos automáticamente según el tipo de operación"""
    try:
        logger.info(f"🚀 Cargando costos automáticos: {tipo_operacion}, {modo_transporte}, {equipo}, {linea_maritima}")
        
        # Si es Marítima FCL, cargar costos de Ganbatte
        if modo_transporte == 'Maritima FCL' and tipo_operacion in ['IM', 'EM']:
            if not equipo:
                raise HTTPException(status_code=400, detail="Se requiere especificar el equipo para operaciones Marítima FCL")
            
            # Obtener costos de Ganbatte
            costos_ganbatte = await get_costos_ganbatte(tipo_operacion, equipo)
            
            # Si se especifica una línea marítima, obtener también sus costos
            costos_linea = None
            if linea_maritima and linea_maritima != "GANBATTE":
                costos_linea_data = await get_costos_linea_maritima(tipo_operacion, linea_maritima, equipo)
                costos_linea = costos_linea_data[0] if costos_linea_data else None
            
            return {
                "tipo": "maritima_fcl",
                "costos_ganbatte": costos_ganbatte,
                "costos_linea": costos_linea,
                "linea_seleccionada": linea_maritima
            }
        
        # Para otros tipos de operación (aérea, terrestre, etc.)
        else:
            return {
                "tipo": "otros",
                "mensaje": "Cargar costos predefinidos según el modo de transporte",
                "modo_transporte": modo_transporte,
                "tipo_operacion": tipo_operacion
            }
            
    except Exception as e:
        logger.exception("Error obteniendo costos automáticos: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------
# Endpoints de Configuración
# -----------------------

@app.get("/api/lineas-maritimas")
async def get_lineas_maritimas():
    """Obtener todas las líneas marítimas únicas"""
    try:
        if supabase is None:
            # Fallback con datos hardcodeados
            lineas = ["CMA CGM", "LOG-IN", "COSCO", "MSC", "MAERSK", "EVERGREEN", 
                     "HAPAG LLOYD", "ZIM", "ONE", "PIL", "HMM", "YANG MING", "GANBATTE"]
            return sorted(lineas)
        
        # Consulta para obtener líneas marítimas únicas
        response = supabase.table('gastos_locales_maritimos')\
            .select('linea_maritima')\
            .execute()
        
        lineas = list(set([item['linea_maritima'] for item in response.data]))
        return sorted(lineas)
        
    except Exception as e:
        logger.exception("Error obteniendo líneas marítimas: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tipos-equipo")
async def get_tipos_equipo():
    """Obtener tipos de equipo únicos"""
    try:
        if supabase is None:
            # Fallback con datos hardcodeados
            equipos = ["20DV'", "40DV'", "40HC'", 
                      "20TK'", "20OT'", "20FR'", "20RE'","40OT'","40FR'","40NOR'",]
            return sorted(equipos)
        
        response = supabase.table('gastos_locales_maritimos')\
            .select('equipo')\
            .execute()
        
        equipos = list(set([item['equipo'] for item in response.data]))
        return sorted(equipos)
        
    except Exception as e:
        logger.exception("Error obteniendo tipos de equipo: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/configuracion-costos")
async def get_configuracion_costos():
    """Obtener configuración general de costos"""
    try:
        config = {
            "moneda_base": "USD",
            "margen_por_defecto": 0.2,
            "conceptos_habilitados": True,
            "actualizado": datetime.now().isoformat()
        }
        return config
    except Exception as e:
        logger.exception("Error obteniendo configuración de costos: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------
# Endpoints para Aerolíneas
# -----------------------

@app.get("/api/aerolineas")
async def get_aerolineas():
    """Obtener todas las aerolíneas activas"""
    try:
        if supabase is None:
            # Fallback con datos estáticos
            aerolineas_estaticas = [
                {"id": 1, "nombre": "LATAM Airlines", "codigo_iata": "LA", "pais": "Chile"},
                {"id": 2, "nombre": "Aerolíneas Argentinas", "codigo_iata": "AR", "pais": "Argentina"},
                {"id": 3, "nombre": "American Airlines", "codigo_iata": "AA", "pais": "Estados Unidos"},
                {"id": 4, "nombre": "Delta Air Lines", "codigo_iata": "DL", "pais": "Estados Unidos"},
                {"id": 5, "nombre": "United Airlines", "codigo_iata": "UA", "pais": "Estados Unidos"},
            ]
            return aerolineas_estaticas
        
        response = supabase.table("aerolineas").select("*").eq("activo", True).order("nombre").execute()
        return response.data or []
        
    except Exception as e:
        logger.exception("Error cargando aerolíneas: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------
# Endpoints de Cotizaciones (EXISTENTES)
# -----------------------

@app.delete("/api/cotizaciones/{codigo_legible}")
async def eliminar_cotizacion(codigo_legible: str):
    """Eliminar una cotización y sus costos asociados"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Verificar que la cotización existe
        existing_cot = supabase.table("cotizaciones").select("*").eq("codigo_legible", codigo_legible).execute()
        if not existing_cot.data:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")

        # Eliminar costos asociados primero
        supabase.table("costos_cotizacion").delete().eq("codigo_cotizacion", codigo_legible).execute()

        # Eliminar la cotización
        response = supabase.table("cotizaciones").delete().eq("codigo_legible", codigo_legible).execute()
        
        logger.info(f"Cotización eliminada: {codigo_legible}")
        return {"mensaje": "Cotización eliminada exitosamente"}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error eliminando cotización: %s", e)
        raise HTTPException(status_code=500, detail=f"Error eliminando cotización: {str(e)}")


# ✅ ENDPOINTS ESPECÍFICOS CON PATH PARAMETER (para manejar códigos con /)
@app.get("/api/cotizaciones/{codigo_path:path}")
async def obtener_cotizacion_completa(codigo_path: str):
    """Obtener una cotización específica - maneja códigos con barras"""
    try:
        print(f"🔍 [GET] Buscando cotización: {codigo_path}")
        
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Buscar por codigo_legible (que puede contener barras)
        response = supabase.table("cotizaciones").select("*").eq("codigo_legible", codigo_path).execute()
        
        print(f"📊 Resultado de búsqueda: {len(response.data)} registros")
        
        if not response.data:
            raise HTTPException(status_code=404, detail=f"Cotización '{codigo_path}' no encontrada")

        cotizacion = response.data[0]
        print(f"✅ Cotización encontrada: {cotizacion['codigo_legible']}")

        # Obtener los costos asociados
        costos_response = supabase.table("costos_cotizacion").select("*").eq("codigo_cotizacion", cotizacion['codigo_legible']).execute()
        print(f"💰 Costos encontrados: {len(costos_response.data or [])}")
        
        # Calcular estado actual
        estado_info = calcular_estado_y_validez(
            cotizacion.get('fecha_validez'), 
            cotizacion.get('validez_dias', 30),
            cotizacion.get('estado')
        )

        # Preparar respuesta completa
        cotizacion_completa = {
            **cotizacion,
            "costos": costos_response.data or [],
            "estado_actual": estado_info['estado'],
            "color": estado_info['color'],
            "dias_restantes": estado_info['dias_restantes'],
            "label_estado": ESTADOS_COTIZACION.get(estado_info['estado'], {'label': '🔵 ENVIADA'})['label']
        }

        return cotizacion_completa

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error obteniendo cotización: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener cotización: {str(e)}")

# ✅ PUT también debe usar path parameter
@app.put("/api/cotizaciones/{codigo_path:path}")
async def actualizar_cotizacion(codigo_path: str, cotizacion: dict):
    """Actualizar una cotización existente - maneja códigos con barras"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        print(f"✏️ [PUT] Actualizando cotización: {codigo_path}")
        print(f"📤 Datos recibidos: {cotizacion}")
        
        # Verificar que la cotización existe
        existing_cot = supabase.table("cotizaciones").select("*").eq("codigo_legible", codigo_path).execute()
        if not existing_cot.data:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")

        # Preparar datos para actualización
        campos_no_actualizables = ['codigo', 'codigo_legible', 'fecha_creacion', 'id']
        update_data = {k: v for k, v in cotizacion.items() if k not in campos_no_actualizables and v is not None}
        update_data["fecha_actualizacion"] = datetime.now().isoformat()

        print(f"📝 Campos a actualizar: {list(update_data.keys())}")

        # Actualizar en la base de datos
        response = supabase.table("cotizaciones").update(update_data).eq("codigo_legible", codigo_path).execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="Error al actualizar cotización")

        logger.info(f"✅ Cotización actualizada: {codigo_path}")
        return {
            "mensaje": "Cotización actualizada exitosamente",
            "codigo": codigo_path,
            "data": response.data[0]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error actualizando cotización: %s", e)
        raise HTTPException(status_code=500, detail=f"Error actualizando cotización: {str(e)}")

# ✅ DELETE también con path parameter
@app.delete("/api/cotizaciones/{codigo_path:path}")
async def eliminar_cotizacion(codigo_path: str):
    """Eliminar una cotización - maneja códigos con barras"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Verificar que la cotización existe
        existing_cot = supabase.table("cotizaciones").select("*").eq("codigo_legible", codigo_path).execute()
        if not existing_cot.data:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")

        # Eliminar costos asociados primero
        supabase.table("costos_cotizacion").delete().eq("codigo_cotizacion", codigo_path).execute()

        # Eliminar la cotización
        response = supabase.table("cotizaciones").delete().eq("codigo_legible", codigo_path).execute()
        
        logger.info(f"Cotización eliminada: {codigo_path}")
        return {"mensaje": "Cotización eliminada exitosamente"}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error eliminando cotización: %s", e)
        raise HTTPException(status_code=500, detail=f"Error eliminando cotización: {str(e)}")

@app.post("/api/cotizaciones")
async def crear_cotizacion(cotizacion: Cotizacion, background_tasks: BackgroundTasks):
    # Validaciones básicas
    try:
        # ✅ NUEVA VALIDACIÓN: Verificar que el cliente existe
        if supabase is not None:
            cliente_existente = supabase.table("clientes").select("nombre").eq("nombre", cotizacion.cliente).eq("activo", True).execute()
            if not cliente_existente.data:
                raise HTTPException(
                    status_code=400, 
                    detail=f"El cliente '{cotizacion.cliente}' no existe en el sistema. Por favor, créelo primero en el módulo de clientes."
                )
        if cotizacion.modo_transporte not in TRANSPORT_MODES:
            raise HTTPException(status_code=400, detail="Modo de transporte inválido.")

        if cotizacion.incoterm_origen and cotizacion.incoterm_origen not in INCOTERMS:
            raise HTTPException(status_code=400, detail="Incoterm origen inválido.")

        if cotizacion.incoterm_destino and cotizacion.incoterm_destino not in INCOTERMS:
            raise HTTPException(status_code=400, detail="Incoterm destino inválido.")

        # ⭐️ LÓGICA CORREGIDA DE ESTANDARIZACIÓN Y VALIDACIÓN MARÍTIMA ⭐️
        if "Maritima" in cotizacion.modo_transporte and cotizacion.tipo_contenedor:
            nombre_equipo_original = cotizacion.tipo_contenedor or cotizacion.equipo
            
            # 1. Estandarizar el nombre del equipo
            equipo_estandarizado = get_standard_equipo(nombre_equipo_original)

            # 2. Validar contra el set de contenedores estandarizados (VALID_DB_CONTAINERS)
            # Esto corrige el error 500 al usar 'in' en un set, en lugar de '.values()'.
            if equipo_estandarizado and equipo_estandarizado in VALID_DB_CONTAINERS:
                # 3. CRÍTICO: Reemplazar el nombre en el objeto Pydantic ANTES de guardar
                cotizacion.tipo_contenedor = equipo_estandarizado
                cotizacion.equipo = equipo_estandarizado 
            else:
                logger.error(f"Equipo inválido: Original='{nombre_equipo_original}', Estandarizado='{equipo_estandarizado}'")
                raise HTTPException(status_code=400, detail="Tipo de contenedor inválido para transporte marítimo.")
        # ----------------------------------------------------------------------------

        # Generar IDs y fechas
        codigo_uuid = str(uuid4())
        codigo_legible = await generar_proximo_numero(cotizacion.tipo_operacion)
        fecha_validez = datetime.now() + timedelta(days=cotizacion.validez_dias or 30)

        # Usar .dict() (o .model_dump() si usa Pydantic v2)
        payload = cotizacion.dict() 
        payload.update({
            "codigo": codigo_uuid,
            "codigo_legible": codigo_legible,
            "fecha_validez": fecha_validez.isoformat(),
            "estado": "creada",
            "notificaciones_enviadas": [],
            # ✅ Asegurar que estos campos se guarden
            "peso_cargable_kg": cotizacion.peso_cargable_kg or 0.0,
            "tiene_hielo_seco": cotizacion.tiene_hielo_seco or False,
            "gastos_locales": cotizacion.gastos_locales or 0.0
        })

        if supabase is None:
            logger.warning("Supabase no configurado. No se insertará la cotización en DB.")
            return {
                "mensaje": "Cotización (simulada) creada", 
                "codigo": codigo_legible, 
                "data": payload,
                "peso_cargable_kg": cotizacion.peso_cargable_kg
            }

        response = supabase.table("cotizaciones").insert(payload).execute()
        if not response or not response.data:
            logger.error("Fallo la inserción en Supabase. Respuesta: %s", response)
            raise HTTPException(status_code=500, detail="Error al crear cotización en la base de datos.")

        # schedule a check (background task)
        background_tasks.add_task(enviar_notificacion, response.data[0], "creada", f"Cotización {codigo_legible} creada")

        return {
            "mensaje": "Cotización creada exitosamente",
            "codigo": codigo_legible,
            "validez_hasta": fecha_validez.strftime("%Y-%m-%d"),
            "estado": "creada",
            "peso_cargable_kg": cotizacion.peso_cargable_kg,
            "data": response.data[0]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error creando cotizacion: %s", e)
        raise HTTPException(status_code=500, detail=f"Error creando cotización: {str(e)}")


@app.get("/api/cotizaciones")
def listar_cotizaciones():
    try:
        if supabase is None:
            logger.warning("Supabase no configurado. Retornando lista vacía.")
            return []

        response = supabase.table("cotizaciones").select("*").order("fecha_creacion", desc=True).execute()
        
        print(f"🔍 Respuesta de Supabase: {len(response.data) if response.data else 0} cotizaciones")
        
        cotizaciones = []
        for cot in (response.data or []):
            try:
                cot_data = cot.copy()
                
                # Usar codigo legible si existe
                if cot_data.get('codigo_legible'):
                    cot_data['codigo'] = cot_data['codigo_legible']
                else:
                    # fallback retroactivo
                    tipo_op = cot_data.get('tipo_operacion', 'XX')
                    prefijo = {
                        'IA': 'GAN-IA', 'IM': 'GAN-IM', 'EA': 'GAN-EA',
                        'EM': 'GAN-EM', 'IT': 'GAN-IT', 'ET': 'GAN-ET','MC': 'GAN-MC', 'CO': 'GAN-CO'
                    }.get(tipo_op, 'GAN-XX')
                    fecha = cot_data.get('fecha_creacion', datetime.now())
                    if isinstance(fecha, str):
                        try:
                            fecha = datetime.fromisoformat(fecha.replace('Z', '+00:00'))
                        except Exception:
                            fecha = datetime.now()
                    año = fecha.strftime("%y")
                    mes = fecha.strftime("%m")
                    cot_data['codigo'] = f"{prefijo}-{año}/{mes}/R01"

                # ✅ Pasar el estado ACTUAL de la base de datos
                estado_actual_db = cot_data.get('estado')
                estado_info = calcular_estado_y_validez(
                    cot_data.get('fecha_validez'), 
                    cot_data.get('validez_dias', 30),
                    estado_actual_db
                )
                
                cot_data['estado_actual'] = estado_info['estado']
                cot_data['color'] = estado_info['color']
                cot_data['dias_restantes'] = estado_info['dias_restantes']
                cot_data['label_estado'] = ESTADOS_COTIZACION.get(estado_info['estado'], {'label': '🔵 ENVIADA'})['label']
                
                cotizaciones.append(cot_data)
                
            except Exception as e:
                print(f"❌ Error procesando cotización {cot.get('id')}: {e}")
                continue
                
        print(f"✅ Total de cotizaciones procesadas: {len(cotizaciones)}")
        return cotizaciones
        
    except Exception as e:
        logger.exception("Error listando cotizaciones: %s", e)
        print(f"❌ Error crítico en listar_cotizaciones: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener cotizaciones: {str(e)}")
# -----------------------
# Endpoints RESTANTES (compatibilidad)
# -----------------------

# Función helper común

async def generar_proximo_numero_operacion() -> str:
    """
    Genera código legible para Operaciones: GAN-OP-YY/MM/NNN
    """
    try:
        ahora = datetime.now()
        año = ahora.strftime("%y")
        mes = ahora.strftime("%m")
        prefijo = "GAN-OP" # Prefijo para Operaciones

        patron_busqueda = f"{prefijo}-{año}/{mes}/"
        if supabase is None:
            return f"{patron_busqueda}001"

        # Buscar códigos en la nueva tabla 'operaciones'
        response = supabase.table("operaciones").select("codigo_operacion").like("codigo_operacion", f"{patron_busqueda}%").execute()
        numeros_existentes = []
        for op in (response.data or []):
            codigo = op.get("codigo_operacion", "")
            match = re.search(r'/(\d+)$', codigo)
            if match:
                try:
                    numeros_existentes.append(int(match.group(1)))
                except ValueError:
                    continue

        proximo_numero = (max(numeros_existentes) + 1) if numeros_existentes else 1
        numero_formateado = f"{proximo_numero:03d}"
        return f"{patron_busqueda}{numero_formateado}"
    except Exception as e:
        logger.exception("Error generando proximo numero de operacion: %s", e)
        ahora = datetime.now()
        año = ahora.strftime("%y")
        mes = ahora.strftime("%m")
        return f"GAN-OP-{año}/{mes}/001"

async def crear_operacion_automatica(codigo_cotizacion: str):
    """
    Crea una operación en la DB basada en una cotización aceptada.
    AJUSTADA para la nueva estructura de tabla con JSONB.
    """
    try:
        if supabase is None:
            logger.warning("Supabase no configurado. No se puede crear operación.")
            return

        # 1. Verificar si la operación ya existe (usando el campo correcto)
        op_existente = supabase.table("operaciones").select("id").eq("cotizacion_origen", codigo_cotizacion).execute()
        if op_existente.data:
            logger.info(f"Operación ya existe para {codigo_cotizacion}. No se crea duplicado.")
            return

        # 2. Obtener datos de la cotización
        cot_response = supabase.table("cotizaciones").select("*").eq("codigo_legible", codigo_cotizacion).single().execute()
        if not cot_response.data:
            logger.error(f"No se encontró la cotización {codigo_cotizacion} para crear operación.")
            return

        cot = cot_response.data
        nuevo_codigo_op = await generar_proximo_numero_operacion()

        # 3. Crear payload de la operación (¡Ajustado a tu schema!)
        operacion_data = {
            "id": str(uuid4()),
            "codigo_operacion": nuevo_codigo_op,
            "cotizacion_origen": codigo_cotizacion, # <-- Coincide con tu SQL
            "cliente": cot.get("cliente"),
            "tipo_operacion": cot.get("tipo_operacion"),
            "estado": "en_proceso", # <-- Coincide con el default 'en_proceso' de tu SQL
            "fecha_creacion": datetime.now().isoformat(),
            "fecha_actualizacion": datetime.now().isoformat(),
            
            # ✅ Aquí guardamos los detalles extra en el JSONB
            "datos_cotizacion": {
                "referencia": cot.get("referencia"),
                "modo_transporte": cot.get("modo_transporte"),
                "origen": cot.get("origen"),
                "destino": cot.get("destino"),
                "equipo": cot.get("equipo"),
                "incoterm_origen": cot.get("incoterm_origen"),
                "incoterm_destino": cot.get("incoterm_destino"),
                "peso_total_kg": cot.get("peso_total_kg"),
                "volumen_m3": cot.get("volumen_m3")
            }
        }

        # 4. Insertar la nueva operación
        insert_response = supabase.table("operaciones").insert(operacion_data).execute()
        if insert_response.data:
            logger.info(f"✅ Operación {nuevo_codigo_op} creada exitosamente desde {codigo_cotizacion}")
        else:
            logger.error(f"Error al insertar operación para {codigo_cotizacion}")

    except Exception as e:
        logger.exception(f"Error crítico en crear_operacion_automatica para {codigo_cotizacion}: {e}")


async def generar_costos_predefinidos(tipo_operacion: str, incoterm: str, modo_transporte: str):
    """Generar costos predefinidos (lógica común)"""
    conceptos_ejemplo = []
    
    if tipo_operacion in ['IM', 'EM']:  # Marítimo
        conceptos_ejemplo = [
            {
                "id": 1,
                "concepto": "Flete Marítimo Internacional",
                "costo_base": 0,
                "precio_sugerido": 0,
                "tipo_operacion": tipo_operacion,
                "incoterm": incoterm,
                "modo_transporte": modo_transporte
            },
            {
                "id": 2, 
                "concepto": "Agencia Marítima",
                "costo_base": 150,
                "precio_sugerido": 250,
                "tipo_operacion": tipo_operacion,
                "incoterm": incoterm,
                "modo_transporte": modo_transporte
            }
        ]
    elif tipo_operacion in ['IA', 'EA']:  # Aéreo
        conceptos_ejemplo = [
            {
                "id": 4,
                "concepto": "Flete Aéreo Internacional", 
                "costo_base": 0,
                "precio_sugerido": 0,
                "tipo_operacion": tipo_operacion,
                "incoterm": incoterm,
                "modo_transporte": modo_transporte
            }
        ]
    
    logger.info(f"✅ Retornando {len(conceptos_ejemplo)} conceptos predefinidos")
    return conceptos_ejemplo

@app.get("/api/puertos_aeropuertos")
def listar_puertos(tipo: Optional[str] = None, pais: Optional[str] = None):
    try:
        q = supabase.table("puertos_aeropuertos").select("*").eq("activo", True)
        if tipo:
            q = q.eq("tipo", tipo)
        if pais:
            q = q.ilike("pais", f"%{pais}%")
        
        q = q.order("nombre")
        data = q.execute()
        return data.data or []
    except Exception as e:
        logger.exception("Error listando puertos/aeropuertos: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/operaciones/{codigo_operacion:path}/checklist")
async def get_checklist_operacion(codigo_operacion: str):
    """Obtener todos los checklist items para una operación"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
        
        response = supabase.table("operacion_checklist").select("*") \
            .eq("codigo_operacion", codigo_operacion) \
            .order("fecha_creacion", desc=False) \
            .execute()
        
        return response.data or []
    except Exception as e:
        logger.exception("Error obteniendo checklist: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener checklist: {str(e)}")

@app.post("/api/operaciones/{codigo_operacion:path}/checklist")
async def add_checklist_item(codigo_operacion: str, item: ChecklistItem):
    """Añadir una nueva tarea al checklist"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Asegurarnos que el código es el del path
        item.codigo_operacion = codigo_operacion
        item_data = item.dict()
        item_data["id"] = str(uuid4())
        item_data["fecha_creacion"] = datetime.now().isoformat()
        
        response = supabase.table("operacion_checklist").insert(item_data).execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="No se pudo crear la tarea")
            
        return response.data[0]
    except Exception as e:
        logger.exception("Error creando tarea checklist: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al crear tarea: {str(e)}")


# ------------------------------------------------
# 🧠 Endpoint de Inteligencia Operativa
# ------------------------------------------------

@app.get("/api/debug/operacion/{codigo_operacion:path}")
async def debug_operacion(codigo_operacion: str):
    """Endpoint de diagnóstico para operaciones"""
    try:
        logger.info(f"🔧 Debug operación: {codigo_operacion}")
        
        if supabase is None:
            return {"error": "Supabase no configurado"}
        
        # Verificar operación
        op_resp = supabase.table("operaciones").select("*").eq("codigo_operacion", codigo_operacion).execute()
        
        if not op_resp.data:
            return {"error": "Operación no encontrada", "codigo": codigo_operacion}
        
        operacion = op_resp.data[0]
        
        # Verificar cotización origen
        cotizacion_origen = operacion.get("cotizacion_origen")
        cotizacion_data = {}
        if cotizacion_origen:
            cot_resp = supabase.table("cotizaciones").select("*").eq("codigo_legible", cotizacion_origen).execute()
            if cot_resp.data:
                cotizacion_data = cot_resp.data[0]
        
        return {
            "operacion": operacion,
            "cotizacion_origen": cotizacion_origen,
            "cotizacion_data": cotizacion_data,
            "datos_cotizacion": operacion.get("datos_cotizacion", {}),
            "existe_cotizacion": bool(cotizacion_data)
        }
        
    except Exception as e:
        logger.exception(f"Error en debug: {e}")
        return {"error": str(e)}

@app.get("/api/ia/recomendaciones/{codigo_operacion:path}")
async def obtener_recomendaciones(codigo_operacion: str):
    """
    Devuelve recomendaciones IA basadas en la operación y su cotización.
    """
    try:
        logger.info(f"🤖 Solicitando recomendaciones IA para: {codigo_operacion}")
        
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # 1️⃣ Obtener operación
        op_resp = supabase.table("operaciones").select("*").eq("codigo_operacion", codigo_operacion).execute()
        logger.info(f"📊 Operación encontrada: {len(op_resp.data) if op_resp.data else 0}")
        
        if not op_resp.data:
            raise HTTPException(status_code=404, detail="Operación no encontrada")
        
        operacion = op_resp.data[0]
        datos = operacion.get("datos_cotizacion", {})
        logger.info(f"📦 Datos operación: {datos}")

        # 2️⃣ Obtener cotización origen (para datos faltantes o duplicados)
        cotizacion_origen = operacion.get("cotizacion_origen")
        cotizacion = {}
        
        if cotizacion_origen:
            cot_resp = supabase.table("cotizaciones").select("*").eq("codigo_legible", cotizacion_origen).execute()
            if cot_resp.data:
                cotizacion = cot_resp.data[0]
                logger.info(f"✅ Cotización origen encontrada: {cotizacion_origen}")
            else:
                logger.warning(f"⚠️ Cotización origen no encontrada: {cotizacion_origen}")

        # 3️⃣ Completar datos para IA
        equipo = datos.get("equipo") or cotizacion.get("equipo")
        origen = datos.get("origen") or cotizacion.get("origen")
        destino = datos.get("destino") or cotizacion.get("destino")
        modo_transporte = datos.get("modo_transporte") or cotizacion.get("modo_transporte")
        volumen_m3 = datos.get("volumen_m3") or cotizacion.get("volumen_m3") or 0
        peso_total_kg = datos.get("peso_total_kg") or cotizacion.get("peso_total_kg") or 0
        incoterm_origen = datos.get("incoterm_origen") or cotizacion.get("incoterm_origen")
        incoterm_destino = datos.get("incoterm_destino") or cotizacion.get("incoterm_destino")
        etd = datos.get("etd")
        eta = datos.get("eta")
        fecha_carga = datos.get("fecha_carga")
        fecha_descarga = datos.get("fecha_descarga")

        logger.info(f"🔍 Datos completados - Origen: {origen}, Destino: {destino}, Equipo: {equipo}")

        # 4️⃣ Simulación de IA (aquí se puede reemplazar con modelo real)
        tareas = []
        if not fecha_carga:
            tareas.append("Registrar fecha efectiva de carga")
        if not fecha_descarga:
            tareas.append("Registrar fecha efectiva de descarga")
        if not etd:
            tareas.append("Confirmar ETD con transportista")
        if not eta:
            tareas.append("Estimación de ETA pendiente")

        recomendaciones = [
            f"💡 Operación con transporte {modo_transporte or 'No especificado'} de {origen or 'Origen no definido'} a {destino or 'Destino no definido'}.",
            f"📦 Volumen: {volumen_m3} m³, Peso total: {peso_total_kg} kg",
            f"📝 Tareas pendientes: {', '.join(tareas) if tareas else 'Ninguna'}",
            f"🤖 Predicción de entrega a tiempo: 90-95%",
            f"⚠️ Nivel de riesgo: Bajo"
        ]

        # Agregar recomendaciones específicas basadas en datos disponibles
        if not etd and not eta:
            recomendaciones.append("⏰ Complete las fechas ETD/ETA para mejorar la precisión de las predicciones")
        
        if equipo:
            recomendaciones.append(f"🚚 Equipo asignado: {equipo}")

        logger.info(f"✅ Recomendaciones generadas: {len(recomendaciones)}")

        return {
            "operacion": operacion,
            "cotizacion": cotizacion,
            "recomendaciones": recomendaciones,
            "tareas": tareas,
            "riesgo": "Bajo",
            "fecha_estimada": eta or "Pendiente"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ ERROR en obtener_recomendaciones: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando recomendaciones: {str(e)}")
    


@app.get("/api/ia/datos-faltantes/{codigo_operacion:path}")
async def obtener_datos_faltantes(codigo_operacion: str):
    """
    Detecta datos faltantes críticos para las predicciones de IA
    """
    try:
        logger.info(f"🔍 Analizando datos faltantes para: {codigo_operacion}")
        
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # 1. Obtener operación
        op_resp = supabase.table("operaciones").select("*").eq("codigo_operacion", codigo_operacion).execute()
        logger.info(f"📊 Respuesta operación: {len(op_resp.data) if op_resp.data else 0} registros")
        
        if not op_resp.data:
            raise HTTPException(status_code=404, detail="Operación no encontrada")
        
        operacion = op_resp.data[0]
        datos = operacion.get("datos_cotizacion", {})
        logger.info(f"📦 Datos cotización: {datos}")
        
        # 2. Obtener cotización origen para completar datos
        cotizacion_origen = operacion.get("cotizacion_origen")
        logger.info(f"🔗 Cotización origen: {cotizacion_origen}")
        
        cotizacion = {}
        if cotizacion_origen:
            cot_resp = supabase.table("cotizaciones").select("*").eq("codigo_legible", cotizacion_origen).execute()
            if cot_resp.data:
                cotizacion = cot_resp.data[0]
                logger.info(f"✅ Cotización encontrada: {cotizacion_origen}")
            else:
                logger.warning(f"⚠️ Cotización origen no encontrada: {cotizacion_origen}")

        # 3. Definir campos críticos para IA
        campos_criticos = {
            'etd': {'nombre': 'ETD (Estimated Time of Departure)', 'tipo': 'date'},
            'eta': {'nombre': 'ETA (Estimated Time of Arrival)', 'tipo': 'date'},
            'fecha_carga': {'nombre': 'Fecha efectiva de carga', 'tipo': 'date'},
            'fecha_descarga': {'nombre': 'Fecha efectiva de descarga', 'tipo': 'date'},
            'equipo': {'nombre': 'Equipo/Contenedor', 'tipo': 'text'},
            'origen': {'nombre': 'Origen', 'tipo': 'text'},
            'destino': {'nombre': 'Destino', 'tipo': 'text'},
            'volumen_m3': {'nombre': 'Volumen (m³)', 'tipo': 'number'},
            'peso_total_kg': {'nombre': 'Peso total (kg)', 'tipo': 'number'},
            'incoterm_origen': {'nombre': 'Incoterm Origen', 'tipo': 'text'},
            'incoterm_destino': {'nombre': 'Incoterm Destino', 'tipo': 'text'}
        }
        
        # 4. Verificar datos faltantes
        datos_faltantes = []
        datos_completados = []
        
        for campo, info in campos_criticos.items():
            valor_actual = datos.get(campo)
            
            # Si no está en datos_cotizacion, buscar en cotización
            if not valor_actual and campo in ['equipo', 'origen', 'destino', 'volumen_m3', 'peso_total_kg', 'incoterm_origen', 'incoterm_destino']:
                valor_actual = cotizacion.get(campo)
                if valor_actual:
                    datos_completados.append({
                        'campo': campo,
                        'nombre': info['nombre'],
                        'valor': valor_actual,
                        'fuente': 'cotizacion'
                    })
                    logger.info(f"✅ Campo {campo} completado desde cotización: {valor_actual}")
            
            # Verificar si el campo sigue vacío
            if not valor_actual:
                datos_faltantes.append({
                    'campo': campo,
                    'nombre': info['nombre'],
                    'tipo': info['tipo'],
                    'critico': campo in ['etd', 'eta', 'fecha_carga', 'origen', 'destino']  # Campos más críticos
                })
                logger.info(f"❌ Campo {campo} faltante")
            else:
                logger.info(f"✅ Campo {campo} presente: {valor_actual}")
        
        # 5. Calcular porcentaje de completitud
        total_campos = len(campos_criticos)
        campos_completos = total_campos - len(datos_faltantes)
        porcentaje_completitud = (campos_completos / total_campos) * 100 if total_campos > 0 else 0
        
        # 6. Determinar nivel de alerta
        if porcentaje_completitud < 50:
            nivel_alerta = "alto"
            mensaje_alerta = "⚠️ Datos críticos faltantes. Complete la información para habilitar predicciones de IA."
        elif porcentaje_completitud < 80:
            nivel_alerta = "medio" 
            mensaje_alerta = "ℹ️ Algunos datos importantes faltan. Mejore la precisión de las predicciones completando la información."
        else:
            nivel_alerta = "bajo"
            mensaje_alerta = "✅ Datos suficientes para predicciones básicas."

        logger.info(f"📈 Análisis completado: {porcentaje_completitud}% - {nivel_alerta}")

        return {
            "operacion_codigo": codigo_operacion,
            "porcentaje_completitud": round(porcentaje_completitud, 1),
            "nivel_alerta": nivel_alerta,
            "mensaje_alerta": mensaje_alerta,
            "datos_faltantes": datos_faltantes,
            "datos_completados": datos_completados,
            "total_campos": total_campos,
            "campos_completos": campos_completos
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ ERROR CRÍTICO en obtener_datos_faltantes: {e}")
        raise HTTPException(status_code=500, detail=f"Error analizando datos: {str(e)}")
    



@app.put("/api/checklist/{item_id}")
async def update_checklist_item(item_id: str, item_update: ChecklistItemUpdate):
    """Actualizar una tarea (marcar como completada)"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
            
        update_data = item_update.dict(exclude_unset=True)
        
        response = supabase.table("operacion_checklist").update(update_data).eq("id", item_id).execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Tarea no encontrada")
            
        return response.data[0]
    except Exception as e:
        logger.exception("Error actualizando tarea: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al actualizar tarea: {str(e)}")

@app.delete("/api/checklist/{item_id}")
async def delete_checklist_item(item_id: str):
    """Eliminar una tarea del checklist"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
        
        response = supabase.table("operacion_checklist").delete().eq("id", item_id).execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Tarea no encontrada")
            
        return {"mensaje": "Tarea eliminada"}
    except Exception as e:
        logger.exception("Error eliminando tarea: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al eliminar tarea: {str(e)}")

@app.post("/api/cotizaciones/cambiar-estado")
async def cambiar_estado(request: CambioEstadoRequest, background_tasks: BackgroundTasks):
    try:
        if request.nuevo_estado not in ESTADOS_COTIZACION:
            raise HTTPException(status_code=400, detail="Estado no válido.")
        if supabase is None:
            return {"mensaje": "Supabase no configurado. Cambio simulado.", "estado": request.nuevo_estado}

        response = supabase.table("cotizaciones").update({
            "estado": request.nuevo_estado,
            "fecha_estado": datetime.now().isoformat()
        }).eq("codigo_legible", request.codigo_legible).execute()

        if not response or not response.data:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")

        # ✅ ¡AQUÍ ESTÁ EL TRIGGER!
        if request.nuevo_estado == "aceptada":
            logger.info(f"Disparando creación de operación para: {request.codigo_legible}")
            # Usamos background_tasks para no retrasar la respuesta al usuario
            background_tasks.add_task(crear_operacion_automatica, request.codigo_legible)

        background_tasks.add_task(enviar_notificacion, response.data[0], f"cambio_estado_{request.nuevo_estado}", f"Cambio de estado a {request.nuevo_estado}")

        return {"mensaje": f"Estado cambiado a {request.nuevo_estado}", "estado": request.nuevo_estado, "color": ESTADOS_COTIZACION[request.nuevo_estado]['color']}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error cambiando estado: %s", e)
        raise HTTPException(status_code=500, detail=f"Error cambiando estado: {str(e)}")

@app.put("/api/operaciones/{codigo_operacion:path}")
async def actualizar_operacion(codigo_operacion: str, datos_actualizados: dict):
    """
    Actualizar los datos de una operación existente
    """
    try:
        logger.info(f"🔄 Actualizando operación: {codigo_operacion}")
        
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        # Verificar que la operación existe
        op_resp = supabase.table("operaciones").select("*").eq("codigo_operacion", codigo_operacion).execute()
        
        if not op_resp.data:
            raise HTTPException(status_code=404, detail="Operación no encontrada")
        
        operacion_actual = op_resp.data[0]
        logger.info(f"📋 Operación actual: {operacion_actual.get('codigo_operacion')}")
        
        # Preparar datos para actualizar
        update_data = {}
        
        # Actualizar datos_cotizacion si se proporciona
        if 'datos_cotizacion' in datos_actualizados:
            datos_actuales = operacion_actual.get('datos_cotizacion', {}) or {}
            nuevos_datos = datos_actualizados['datos_cotizacion']
            
            # Fusionar los datos existentes con los nuevos
            datos_fusionados = {**datos_actuales, **nuevos_datos}
            update_data['datos_cotizacion'] = datos_fusionados
            
            logger.info(f"📦 Datos cotización actualizados: {list(nuevos_datos.keys())}")
            logger.info(f"🔍 Valores actualizados: {nuevos_datos}")
        
        # Actualizar fecha_actualizacion
        update_data['fecha_actualizacion'] = datetime.now().isoformat()
        
        logger.info(f"💾 Ejecutando UPDATE en Supabase: {update_data}")
        
        # Realizar la actualización en Supabase
        response = supabase.table("operaciones").update(update_data).eq("codigo_operacion", codigo_operacion).execute()
        
        if not response.data:
            logger.error("❌ No se recibieron datos en la respuesta de Supabase")
            raise HTTPException(status_code=500, detail="Error al actualizar operación")
        
        operacion_actualizada = response.data[0]
        
        logger.info(f"✅ Operación actualizada exitosamente: {codigo_operacion}")
        logger.info(f"📊 Nuevos datos_cotizacion: {operacion_actualizada.get('datos_cotizacion', {})}")
        
        return {
            "mensaje": "Operación actualizada exitosamente",
            "operacion": operacion_actualizada,
            "campos_actualizados": list(datos_actualizados.get('datos_cotizacion', {}).keys())
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ ERROR actualizando operación: {e}")
        raise HTTPException(status_code=500, detail=f"Error actualizando operación: {str(e)}")


# -----------------------
# Endpoints de Operaciones
# -----------------------

@app.get("/api/operaciones")
async def listar_operaciones():
    """Obtener lista de todas las operaciones"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
        
        response = supabase.table("operaciones").select("*").order("fecha_creacion", desc=True).execute()
        return response.data or []
    except Exception as e:
        logger.exception("Error listando operaciones: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener operaciones: {str(e)}")

@app.get("/api/operaciones/{codigo_operacion:path}")
async def obtener_operacion(codigo_operacion: str):
    """Obtener una operación específica por su codigo_operacion"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
            
        response = supabase.table("operaciones").select("*").eq("codigo_operacion", codigo_operacion).single().execute()
        if not response.data:
            raise HTTPException(status_code=404, detail="Operación no encontrada")
        return response.data
    except Exception as e:
        logger.exception("Error obteniendo operación: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al obtener operación: {str(e)}")

@app.put("/api/operaciones/{codigo_operacion:path}")
async def actualizar_operacion(codigo_operacion: str, update_data: dict):
    """Actualizar el estado u otros datos de una operación"""
    try:
        if supabase is None:
            raise HTTPException(status_code=503, detail="Base de datos no disponible")

        update_data["fecha_actualizacion"] = datetime.now().isoformat()
        
        response = supabase.table("operaciones").update(update_data).eq("codigo_operacion", codigo_operacion).execute()
        if not response.data:
            raise HTTPException(status_code=404, detail="Operación no encontrada para actualizar")
            
        logger.info(f"Operación actualizada: {codigo_operacion}")
        return response.data[0]
    except Exception as e:
        logger.exception("Error actualizando operación: %s", e)
        raise HTTPException(status_code=500, detail=f"Error al actualizar operación: {str(e)}")

# -----------------------
# Main (for local run)
# -----------------------
if __name__ == "__main__":
    import uvicorn
    logger.info("Ejecutando main.py directamente (uvicorn) - host 0.0.0.0:8000")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=(ENV=="development"))