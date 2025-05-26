import re
from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from config import Config

login_routes = Blueprint('login_routes', __name__)

GOOGLE_CLIENT_ID = "YOUR_GOOGLE_CLIENT_ID_HERE"  # Replace with your actual Google Client ID

def verify_google_token(token):
    try:
        # Verify the token
        idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)

        # Ensure the token is from Google
        if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
            raise ValueError('Invalid token issuer.')

        # Ensure the token is intended for this client
        if idinfo['aud'] != GOOGLE_CLIENT_ID:
            raise ValueError('Token not meant for this app.')

        # Extract user information
        return {
            "success": True,
            "email": idinfo.get("email"),
            "email_verified": idinfo.get("email_verified"),
            "name": idinfo.get("name"),
            "picture": idinfo.get("picture"),
            "google_id": idinfo.get("sub"),
            "domain": idinfo.get("hd")  # This is "adoreme.com" in your case
        }

    except Exception as e:
        return {"success": False, "error": str(e)}

@login_routes.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"})

@login_routes.route('/login', methods=['POST', 'OPTIONS'])
def login():
    
    if request.method == 'OPTIONS':
        # Return CORS headers for preflight requests
        origin = request.headers.get("Origin")
        print(f"Origin: {origin}")
        if origin not in Config.allowed_origins:
            return jsonify({"message": "CORS preflight failed"}), 403
        else:
            response = jsonify({"message": "CORS preflight OK"})
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, Content-Disposition"
            response.headers["Access-Control-Allow-Credentials"] = "true"
        return response, 200

    try:
        data = request.get_json()
        google_token = data.get("id_token")

        if not google_token:
            return jsonify({"success": False, "error": "Missing Google ID token"}), 400

        user_info = verify_google_token(google_token)

        if not user_info["success"]:
            return jsonify({"status": "error", "message": user_info["error"]}), 401

        email = user_info["email"]
        email_verified = user_info["email_verified"]

        # Validate email
        if re.match(r".*@adoreme.com", email) and email_verified:
            # Generate JWT token
            access_token = create_access_token(identity=email)
            user_info["accessToken"] = access_token
            return jsonify({'status': 'success', 'user': user_info,  'message': 'Login successful!'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Invalid credentials!'}), 401

    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error logging in: {e}'}), 500
