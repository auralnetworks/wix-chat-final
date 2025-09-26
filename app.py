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
GEMINI_API_KEY = "AIzaSyC7OceU-fwISiyihJsDDv51kMQEAkzEQ0k"
PROJECT_ID = "esval-435215"
TABLE_ID = "esval-435215.webhooks.Adereso_WebhookTests"

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

# Configurar credenciales
creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
if creds_json:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name

genai.configure(api_key=GEMINI_API_KEY)
bq_client = bigquery.Client(project=PROJECT_ID)

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
    return {"status": "Backend Adereso - Siempre con Gemini + Tiempo Real + Todos los Campos"}

def get_working_model():
    """Encuentra un modelo que funcione"""
    models = ['gemini-pro', 'models/gemini-2.5-flash', 'models/gemini-1.5-flash']
    
    for model_name in models:
        try:
            app.logger.info(f"MODELO: Probando {model_name}")
            model = genai.GenerativeModel(model_name)
            test_response = model.generate_content("Di 'hola'")
            
            if test_response and test_response.text:
                app.logger.info(f"MODELO: ✅ {model_name} FUNCIONA")
                return model, model_name
                
        except Exception as e:
            app.logger.warning(f"MODELO: {model_name} error: {str(e)[:50]}")
            continue
    
    raise Exception("Ningún modelo Gemini disponible")

def generate_dynamic_sql(user_query):
    """Genera SQL dinámicamente usando Gemini"""
    
    sql_prompt = f"""
    Genera SQL para BigQuery tabla `{TABLE_ID}` basada en: "{user_query}"

    CAMPOS DISPONIBLES:
    {ALL_FIELDS}

    EJEMPLOS:
    - "tickets por canal" → SELECT Canal, COUNT(*) as cantidad FROM `{TABLE_ID}` GROUP BY Canal ORDER BY cantidad DESC
    - "últimos tickets" → SELECT Identifier, Estado, Canal, Fecha_de_inicio FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC LIMIT 20
    - "últimos usuarios" → SELECT Identifier, Nick_del_Cliente, Canal, Fecha_de_inicio FROM `{TABLE_ID}` ORDER BY Fecha_de_inicio DESC LIMIT 20
    - "detalles usuarios" → SELECT Nick_del_Cliente, Canal, Estado, Mensajes FROM `{TABLE_ID}` WHERE Nick_del_Cliente IS NOT NULL LIMIT 20

    REGLAS:
    1. USA SOLO los campos de la lista disponible
    2. Para usuarios usa Nick_del_Cliente (NO Client_Name ni UserID)
    3. Para conteos usa COUNT(*) as cantidad
    4. LIMIT 20 máximo
    5. Solo SQL, sin explicaciones

    SQL:
    """
    
    try:
        app.logger.info(f"GEMINI: Generando SQL para: {user_query}")
        
        model, model_name = get_working_model()
        app.logger.info(f"GEMINI: Usando modelo: {model_name}")
        
        response = model.generate_content(sql_prompt)
        
        if not response or not response.text:
            return None
            
        sql = response.text.strip().replace('```sql', '').replace('```', '').strip()
        
        # Validación básica
        if not sql or 'SELECT' not in sql.upper():
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
        app.logger.info(f"QUERY: {user_query}")
        
        # Generar SQL con Gemini
        sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "❌ Error: No se pudo generar SQL válida",
                "chart": {"labels": ["Error"], "values": [0]},
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
        else:
            chart = {
                "labels": ["Registros"],
                "values": [len(results)]
            }
        
        # Generar tickets
        tickets = []
        for _, row in results.iterrows():
            ticket = {}
            for col in results.columns:
                value = row[col]
                if pd.isna(value):
                    ticket[col] = None
                else:
                    ticket[col] = str(value)
            tickets.append(ticket)
        
        # Generar respuesta con Gemini
        try:
            model, model_name = get_working_model()
            
            response_prompt = f"""
            Usuario preguntó: "{user_query}"
            Encontrados: {len(results)} registros
            
            {results.to_string()}
            
            Responde en español, claro y con emojis:
            """
            
            response = model.generate_content(response_prompt)
            text_response = response.text if response and response.text else f"📊 Encontré {len(results)} registros."
        except:
            text_response = f"📊 Encontré {len(results)} registros."
        
        return jsonify({
            "text": text_response,
            "chart": chart,
            "tickets": tickets[:20]
        })
        
    except Exception as e:
        app.logger.error(f"ERROR: {str(e)}")
        return jsonify({
            "text": f"❌ Error: {str(e)}",
            "chart": {"labels": ["Error"], "values": [0]},
            "tickets": []
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
