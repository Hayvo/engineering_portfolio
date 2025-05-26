from flask import request, jsonify, Blueprint, send_file, Response
from config import Config
from flask_cors import cross_origin 
import json
import json
from utils.excelWorkbookGenerator import ExcelWorkbookGenerator
from flask_jwt_extended import jwt_required 
import os 
import traceback
import datetime 

api_routes = Blueprint('apiRoutes', __name__)
config = Config()

@api_routes.route('/journals', methods=['GET', 'OPTIONS'])
@jwt_required()
def journals():
    if request.method == 'OPTIONS':
        # Return CORS headers
        origin = request.headers.get("Origin")
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
        try:
            journal_event_mapping = open("utils/journal_event_mapping.json", 'r')
            journal_event_mapping = json.load(journal_event_mapping)
        except:
            journal_event_mapping = ExcelWorkbookGenerator().getJournalEventMapping()
        journal_tables = list(set([journal for journal in journal_event_mapping.keys()]))
        journal_tables.sort()
        return jsonify({'status': 'success', 'journals': journal_tables}), 200
    except Exception as e:
        print(e)
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': f'Error getting journals: {e}'}), 500
    
@api_routes.route('/api', methods=['GET', 'OPTIONS'])
@jwt_required()
def api():
    if request.method == 'OPTIONS':
        # Return CORS headers
        origin = request.headers.get("Origin")
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
        journal = request.args.get('journal')
        dateFrom = request.args.get('dateFrom')
        dateTo = request.args.get('dateTo')

        dateFrom = datetime.datetime.strptime(dateFrom, "%Y_%m_%d").date()
        dateTo = datetime.datetime.strptime(dateTo, "%Y_%m_%d").date()

        try:
            excelWorkbookGenerator = ExcelWorkbookGenerator()
            excelFile, workbookPath = excelWorkbookGenerator.createExcelWorkbook(journal=journal, dateFrom=dateFrom, dateTo=dateTo)
            workbookName = workbookPath.split('/')[-1]
            del excelWorkbookGenerator # Kill class
        
            response = send_file(
                excelFile,
                as_attachment=True,
                download_name=f"{workbookName}",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response.headers["Content-Disposition"] = f'attachment; filename="{workbookName}"'
            response.headers["Access-Control-Expose-Headers"] = "Content-Disposition"
            return response
        except Exception as e:
            traceback.print_exc()
            return jsonify({'status': 'error', 'message': f'Error creating Excel workbook: {e}'}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': f'Error getting params: {e}'}), 500
    

@api_routes.route('/delete_file', methods=['POST', 'OPTIONS'])
@jwt_required()
def delete_file():
    if request.method == 'OPTIONS':
        # Return CORS headers
        origin = request.headers.get("Origin")
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
        fileName = data['fileName']
        filePath = 'temp/' + fileName
        try:
            os.remove(filePath)
            return jsonify({'status': 'success', 'message': f'File deleted successfully'}), 200
        except Exception as e:
            return jsonify({'status': 'error', 'message': f'Error deleting file: {e}'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error deleting file: {e}'}), 500