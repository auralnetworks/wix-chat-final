from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai
import os

app = Flask(__name__)
CORS(app)

genai.configure(api_key="AIzaSyC7OceU-fwISiyihJsDDv51kMQEAkzEQ0k")

@app.route('/')
def home():
    return {"status": "Backend funcionando"}

@app.route('/api/test', methods=['POST'])
def test():
    data = request.get_json()
    return jsonify({
        "text": f"✅ Funcionando! Recibí: {data.get('query', 'sin query')}",
        "chart": {"labels": ["Test1", "Test2"], "values": [10, 20]}
    })

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        user_query = request.json['query']
        model = genai.GenerativeModel('gemini-pro')
        prompt = f"Eres asistente de Adereso. Pregunta: {user_query}. Responde en español sobre datos de webhooks."
        response = model.generate_content(prompt)
        
        return jsonify({
            "text": response.text,
            "chart": {"labels": ["Exitosos", "Fallidos"], "values": [85, 15]}
        })
    except Exception as e:
        return jsonify({"text": f"Error: {str(e)}", "chart": None}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
