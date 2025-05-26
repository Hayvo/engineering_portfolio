from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from config import Config


app = Flask(__name__)
CORS(app, supports_credentials=True, origins=Config.allowed_origins)

app.config["JWT_SECRET_KEY"] = Config.JWT_SECRET_KEY
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = Config.JWT_ACCESS_TOKEN_EXPIRES
app.config["JWT_TOKEN_LOCATION"] = Config.JWT_TOKEN_LOCATION

jwt = JWTManager(app)

# from routes.ETLs_routes import etls_routes
from routes.login_routes import login_routes
from routes.api_routes import api_routes

app.register_blueprint(login_routes)
app.register_blueprint(api_routes)

@app.after_request
def add_security_headers(response):
    # Remove COEP if issues persist
    origin = request.headers.get("Origin")
    print(f"Origin: {origin}")
    if origin in Config.allowed_origins:
        response.headers["Cross-Origin-Opener-Policy"] = "same-origin-allow-popups"
        response.headers["Access-Control-Allow-Origin"] = origin  
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

if __name__ == '__main__':    
    app.run(port=5000, debug=True)