from flask import Flask, render_template, request, jsonify
from Adarsh.bot.plugins.generate_token import get_current_token

app = Flask(__name__, template_folder="template")

@app.route("/")
def index():
    return render_template("req.html")

@app.route("/validate", methods=["POST"])
def validate_token():
    user_token = request.form.get("token")
    current_token = get_current_token()
    
    if user_token == current_token:
        return jsonify({"status": "success", "message": "✅ Access Granted!"})
    else:
        return jsonify({"status": "error", "message": "❌ Invalid Token. Please try again."})

if __name__ == "__main__":
    app.run(debug=True)
