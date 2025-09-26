from flask import Flask, request, jsonify
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import tempfile
from datetime import datetime
import logging

app = Flask(__name__)

# Configurar logging
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Configuración
GEMINI_API_KEY = "AIzaSyCbNt5deM5N9zRbaSZAFkGmlbjHvuOuRgk"
PROJECT_ID = "esval-435215"
TABLE_ID = "esval-435215.webhooks.Adereso_WebhookTests"

# Configurar credenciales
creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
if creds_json:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name

genai.configure(api_key=GEMINI_API_KEY)
bq_client = bigquery.Client(project=PROJECT_ID)

# TODOS LOS CAMPOS DISPONIBLES
ALL_FIELDS = """
ID, Fecha_de_inicio, Hora_de_inicio, Fecha_de_actualizacion, Hora_de_actualizacion, 
Fecha_de_abordaje, Hora_de_abordaje, Fecha_de_termino, Hora_de_termino, Estado, 
Detalles_del_estado, Canal, Cuenta_s, Sentimiento_Inicial, Sentimiento_de_Termino, 
Tiene_mensajes_publicos, Tiene_mensajes_privados, Tiene_ticket_previo, Respondido, 
Nick_del_Cliente, Asignacion_actual, Primera_asignacion, Ultima_asignacion, 
Cantidad_de_asignaciones, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, 
Texto_del_Primer_Mensaje, Texto_del_ultimo_Mensaje, Importante, Abordado, 
Abordado_en_SLA, Tipificado, Escalado, Tiempo_de_Abordaje__Segundos_, 
Segundos_Sin_Asignar, Proactivo, Departamento, Cerrado_Por, Abordado_Por, 
Fecha_de_primer_asignacion_humana, Hora_de_primer_asignacion_humana, 
Fecha_de_asignacion, Hora_de_asignacion, Creado_en_horario_habil, 
Tickets_fusionados, Tiempo_asignado_sin_abordaje__Segundos_, 
Tiempo_de_abordaje_ejecutivo__Segundos_, Abordado_en_SLA_ejecutivo, 
BOT_DERIVATION_Date, BOT_DERIVATION_Time, Primer_departamento, 
Ultimo_departamento, Prioridad, Cliente_Principal, Primera_asignacion_humana, 
Empresa, Grupo, Menu_inicial, Numero_de_servicio, Tipificaciones, 
Tipificaciones_Anidado_1, Tipificacion_Bot, Tipificacion_Menu_Clarita, 
Tipificacion_Sub_Menu_Clarita, Identifier
"""

# CORS
@app.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.route('/api/query', methods=['OPTIONS'])
def handle_options():
    response = jsonify({})
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/')
def home():
    return {"status": "Backend Bruno - Modelo Correcto"}

def get_available_model():
    """Encuentra el modelo Gemini disponible"""
    models_to_try = [
        'gemini-pro',
        'gemini-1.5-pro', 
        'models/gemini-pro',
        'models/gemini-1.5-pro'
    ]
    
    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            app.logger.info(f"MODELO: Usando {model_name}")
            return model
        except Exception as e:
            app.logger.warning(f"MODELO: {model_name} no disponible: {e}")
            continue
    
    raise Exception("Ningún modelo Gemini disponible")

def generate_dynamic_sql(user_query):
    """Genera SQL dinámicamente usando Gemini con modelo correcto"""
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sql_prompt = f"""
    Eres experto en SQL y BigQuery. Genera una consulta SQL para la tabla `{TABLE_ID}` basada en: "{user_query}"

    CONTEXTO ACTUAL:
    - Fecha/Hora actual: {current_time}
    - Tabla: {TABLE_ID}
    
    TODOS LOS CAMPOS DISPONIBLES:
    {ALL_FIELDS}

    EJEMPLOS DE CONSULTAS INTELIGENTES:
    - "mensajes iniciales" → SELECT Identifier, Texto_del_Primer_Mensaje FROM `{TABLE_ID}` LIMIT 20
    - "tipificaciones bot" → SELECT Tipificacion_Bot, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Tipificacion_Bot
    - "tickets por canal" → SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Canal ORDER BY cantidad DESC
    - "whatsapp" → SELECT Identifier, Estado, Canal FROM `{TABLE_ID}` WHERE LOWER(Canal) LIKE '%whatsapp%' LIMIT 20
    - "escalados" → SELECT Identifier, Estado FROM `{TABLE_ID}` WHERE Escalado = 'true' LIMIT 20

    REGLAS IMPORTANTES:
    1. Para "hoy" usa: WHERE DATE(Fecha_de_inicio) = CURRENT_DATE()
    2. Para conteos usa COUNT(*) as cantidad
    3. Para búsquedas de texto usa LOWER() y LIKE '%texto%'
    4. Incluye siempre Identifier cuando sea posible
    5. LIMIT 20 máximo para memoria

    IMPORTANTE: Solo devuelve la consulta SQL, sin explicaciones ni markdown.

    SQL:
    """
    
    try:
        app.logger.info(f"GEMINI: Generando SQL para: {user_query}")
        
        model = get_available_model()
        response = model.generate_content(sql_prompt)
        
        if not response or not response.text:
            app.logger.error("GEMINI: No devolvió respuesta")
            return None
            
        sql = response.text.strip()
        app.logger.info(f"GEMINI: Respuesta: {sql[:100]}...")
        
        # Limpiar respuesta
        sql = sql.replace('```sql', '').replace('```', '').strip()
        
        # Validación de seguridad
        dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(word in sql.upper() for word in dangerous):
            app.logger.error(f"GEMINI: SQL peligroso: {sql}")
            return None
        
        # Validar SQL
        if not sql or len(sql) < 10 or 'SELECT' not in sql.upper():
            app.logger.error(f"GEMINI: SQL inválido: {sql}")
            return None
        
        app.logger.info(f"GEMINI: SQL válido: {sql}")
        return sql
        
    except Exception as e:
        app.logger.error(f"GEMINI: Error: {str(e)}")
        return None

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        app.logger.info(f"QUERY: {user_query}")
        
        # Generar SQL con Gemini
        sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "Error: No se pudo generar SQL válida",
                "chart": None,
                "tickets": []
            }), 400
        
        app.logger.info(f"SQL: {sql}")
        
        # Ejecutar consulta
        query_job = bq_client.query(sql)
        results = query_job.result(max_results=25)
        results = results.to_dataframe()
        
        app.logger.info(f"RESULTADOS: {len(results)} registros")
        
        # Generar gráfico
        if 'cantidad' in results.columns:
            chart = {
                "labels": results.iloc[:, 0].astype(str).tolist()[:10],
                "values": results['cantidad'].tolist()[:10]
            }
        elif 'total' in results.columns:
            chart = {
                "labels": ["Total"],
                "values": [int(results['total'].iloc[0])]
            }
        else:
            chart = {
                "labels": ["Registros"],
                "values": [len(results)]
            }
        
        # Generar tickets
        tickets = []
        if len(results) <= 25:
            for _, row in results.head(10).iterrows():
                ticket = {
                    "id": str(row.get('Identifier', 'N/A')),
                    "estado": str(row.get('Estado', 'N/A')),
                    "canal": str(row.get('Canal', 'N/A')),
                    "fecha": str(row.get('Fecha_de_inicio', 'N/A'))[:10] if row.get('Fecha_de_inicio') else 'N/A'
                }
                tickets.append(ticket)
        
        # Generar respuesta con Gemini
        try:
            model = get_available_model()
            response_prompt = f"""
            Usuario: "{user_query}"
            Registros: {len(results)}
            
            Responde como Bruno, analista experto. Sé específico. Usa emojis. Máximo 2 líneas.
            """
            
            response = model.generate_content(response_prompt)
            response_text = response.text if response and response.text else f"📊 Se encontraron {len(results)} registros"
            
        except:
            if 'cantidad' in results.columns:
                total = results['cantidad'].sum()
                response_text = f"📈 Análisis completado: {len(results)} categorías con {total} registros totales"
            elif 'total' in results.columns:
                total = results['total'].iloc[0]
                response_text = f"📊 Total de registros: {total:,}"
            else:
                response_text = f"🎫 Se encontraron {len(results)} registros para tu consulta"
        
        return jsonify({
            "text": response_text,
            "chart": chart,
            "tickets": tickets,
            "data_count": len(results),
            "timestamp": current_time
        })
        
    except Exception as e:
        app.logger.error(f"ERROR: {str(e)}")
        return jsonify({
            "text": f"Error: {str(e)}",
            "chart": None,
            "tickets": []
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
