from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
from google.cloud import bigquery
import os
import pandas as pd
import json
import tempfile
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuración
GEMINI_API_KEY = "AIzaSyC7OceU-fwISiyihJsDDv51kMQEAkzEQ0k"
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

@app.route('/')
def home():
    return {"status": "Backend Adereso - Gráficos Inteligentes + Tarjetas Condicionales"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Backend funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Test"], "values": [100]}
    })

def generate_dynamic_sql(user_query):
    """Genera SQL dinámicamente usando Gemini SIEMPRE"""
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sql_prompt = f"""
    Eres experto en SQL y BigQuery. Genera una consulta SQL para la tabla `{TABLE_ID}` basada en: "{user_query}"

    CONTEXTO ACTUAL:
    - Fecha/Hora actual: {current_time}
    - Tabla: {TABLE_ID}
    
    CAMPOS DISPONIBLES:
    ID, Fecha_de_inicio, Hora_de_inicio, Fecha_de_actualizacion, Hora_de_actualizacion, Estado, Canal, 
    Nick_del_Cliente, Mensajes, Mensajes_Enviados, Mensajes_Recibidos, Departamento, Empresa, 
    Sentimiento_Inicial, Abordado_en_SLA, Tipificaciones, Identifier, Prioridad, etc.

    REGLAS IMPORTANTES:
    1. Para "hoy" usa: WHERE DATE(Fecha_de_inicio) = CURRENT_DATE()
    2. Para "ayer" usa: WHERE DATE(Fecha_de_inicio) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    3. Para comparativos usa UNION ALL
    4. Para conteos usa COUNT(*) sin LIMIT
    5. Para búsquedas de texto usa LOWER() y LIKE '%texto%'
    6. Incluye siempre Identifier cuando sea posible
    7. NO uses LIMIT a menos que el usuario pida números específicos

    EJEMPLOS:
    - "tickets de hoy vs ayer" → Usar UNION ALL con dos consultas separadas
    - "cuántos tickets hay" → SELECT COUNT(*) FROM tabla
    - "tickets de WhatsApp" → WHERE LOWER(Canal) LIKE '%whatsapp%'

    IMPORTANTE: Solo devuelve la consulta SQL, sin explicaciones ni markdown.

    SQL:
    """
    
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(sql_prompt)
        sql = response.text.strip()
        
        # Limpiar respuesta
        sql = sql.replace('```sql', '').replace('```', '').strip()
        
        # Validación de seguridad
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in sql.upper() for keyword in dangerous_keywords):
            return None
        
        return sql
        
    except Exception as e:
        print(f"Error generando SQL: {e}")
        return None

def generate_chart_with_identifiers(results):
    """Genera gráfico inteligente según el tipo de datos"""
    if len(results) == 0:
        return None
    
    # 1. COMPARATIVOS (Hoy/Ayer, etc.) - Detectar por estructura
    if len(results.columns) == 2 and any(word in str(results.iloc[:, 0].tolist()).lower() for word in ['hoy', 'ayer', 'today', 'yesterday']):
        return {
            "labels": results.iloc[:, 0].astype(str).tolist(),
            "values": results.iloc[:, 1].tolist()
        }
    
    # 2. CONTEOS AGREGADOS (Canal, Estado, Departamento, etc.)
    if 'cantidad' in results.columns:
        return {
            "labels": results.iloc[:, 0].astype(str).tolist()[:10],
            "values": results['cantidad'].tolist()[:10]
        }
    
    # 3. TOTALES SIMPLES
    if 'total' in results.columns:
        return {
            "labels": ["Total de Tickets"],
            "values": [int(results['total'].iloc[0])]
        }
    
    # 4. ANÁLISIS POR HORA
    if 'hora' in results.columns:
        return {
            "labels": [f"{int(h)}:00" for h in results['hora'].tolist()],
            "values": results['cantidad'].tolist() if 'cantidad' in results.columns else results['hora'].tolist()
        }
    
    # 5. ANÁLISIS POR FECHA
    if 'fecha' in results.columns:
        return {
            "labels": results['fecha'].astype(str).tolist()[:15],
            "values": results['cantidad'].tolist()[:15] if 'cantidad' in results.columns else list(range(1, len(results) + 1))
        }
    
    # 6. TICKETS INDIVIDUALES - Solo si hay pocos registros
    if len(results) <= 20 and 'Identifier' in results.columns:
        identifiers = results['Identifier'].head(15).fillna('Sin ID').tolist()
        
        if 'Mensajes' in results.columns:
            return {
                "labels": identifiers,
                "values": results['Mensajes'].head(15).fillna(0).tolist()
            }
        else:
            return {
                "labels": identifiers,
                "values": list(range(1, len(identifiers) + 1))
            }
    
    # 7. FALLBACK - No mostrar gráfico para muchos registros individuales
    return None

def should_show_ticket_cards(user_query, results):
    """Determina si mostrar tarjetas de tickets individuales"""
    query = user_query.lower()
    
    # Mostrar tarjetas solo cuando se preguntan tickets específicos
    show_keywords = [
        'tickets', 'ticket', 'últimos', 'recientes', 'clarita', 
        'whatsapp', 'chat', 'cliente', 'hoy', 'ayer'
    ]
    
    # NO mostrar para análisis agregados
    hide_keywords = [
        'total', 'cuántos', 'cantidad', 'count', 'canal', 'canales',
        'estado', 'estados', 'departamento', 'empresa', 'hora', 'horas',
        'vs', 'versus', 'comparar'
    ]
    
    # Si es análisis agregado, no mostrar tarjetas
    if any(word in query for word in hide_keywords):
        return False
    
    # Si pregunta por tickets específicos y hay pocos resultados, mostrar
    if any(word in query for word in show_keywords) and len(results) <= 100:
        return True
    
    return False

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"[{current_time}] Consulta: {user_query}")
        
        # SIEMPRE generar SQL con Gemini para respuestas dinámicas
        sql = generate_dynamic_sql(user_query)
        
        if not sql:
            return jsonify({
                "text": "No pude generar una consulta SQL válida. ¿Puedes reformular tu pregunta?",
                "chart": None
            }), 400
        
        print(f"SQL generado: {sql}")
        
        # Ejecutar consulta en tiempo real
        results = bq_client.query(sql).to_dataframe()
        
        print(f"Registros obtenidos: {len(results)} a las {current_time}")
        
        # Generar gráfico inteligente
        chart_data = generate_chart_with_identifiers(results)
        
        # SIEMPRE usar Gemini para respuestas dinámicas y específicas
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Preparar contexto rico para Gemini
        data_sample = results.head(10).to_string() if len(results) > 0 else "No hay datos"
        
        # Información adicional del contexto
        context_info = []
        if len(results) > 0:
            if 'Canal' in results.columns and len(results) > 1:
                canales = results['Canal'].value_counts().head(3).to_dict()
                context_info.append(f"Canales principales: {canales}")
            
            if 'Estado' in results.columns and len(results) > 1:
                estados = results['Estado'].value_counts().head(3).to_dict()
                context_info.append(f"Estados principales: {estados}")
            
            if 'Mensajes' in results.columns:
                avg_mensajes = results['Mensajes'].mean()
                context_info.append(f"Promedio de mensajes: {avg_mensajes:.1f}")
        
        context_text = " | ".join(context_info) if context_info else ""
        
        response_prompt = f"""
        CONSULTA DEL USUARIO: "{user_query}"
        TIMESTAMP ACTUAL: {current_time}
        SQL EJECUTADO: {sql}
        TOTAL DE REGISTROS: {len(results)}
        
        MUESTRA DE DATOS:
        {data_sample}
        
        CONTEXTO ADICIONAL:
        {context_text}
        
        INSTRUCCIONES:
        - Responde como analista experto de Adereso en tiempo real
        - Menciona la hora/fecha actual si es relevante
        - Sé específico con los números y datos encontrados
        - Si es un comparativo, calcula diferencias y porcentajes
        - Si hay patrones interesantes en los datos, menciónalos
        - Usa emojis para hacer la respuesta más visual
        - Si encontraste muchos registros, menciona que hay más datos disponibles
        - Responde en español de forma conversacional y profesional
        
        RESPUESTA:
        """
        
        response = model.generate_content(response_prompt)
        
        # Determinar si mostrar tarjetas de tickets
        show_cards = should_show_ticket_cards(user_query, results)
        raw_data = results.head(20).to_dict('records') if show_cards and len(results) > 0 else []
        
        return jsonify({
            "text": response.text,
            "chart": chart_data,
            "data_count": len(results),
            "raw_data": raw_data,
            "sql_executed": sql,
            "timestamp": current_time,
            "show_ticket_cards": show_cards
        })
        
    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_time}] Error: {str(e)}")
        return jsonify({
            "text": f"Error consultando datos en tiempo real: {str(e)}",
            "chart": None,
            "timestamp": error_time
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
