from flask import Flask, request, jsonify
from Adarsh.utils.database import Database
from Adarsh.vars import Var

app = Flask(__name__)

# Initialize MongoDB database
db = Database(Var.DATABASE_URL, "tokens")

@app.route('/validate_token', methods=['POST'])
def validate_token():
    data = request.get_json()
    token = data.get('token')

    if not token:
        return jsonify({"success": False, "message": "No token provided."}), 400

    # Fetch the daily token from the database
    token_data = db.tokens.find_one({"type": "daily"})
    
    if token_data and token_data.get('token') == token:
        return jsonify({"success": True, "message": "Access granted."})
    else:
        return jsonify({"success": False, "message": "Invalid token."}), 403

if __name__ == '__main__':
    app.run(debug=True)
