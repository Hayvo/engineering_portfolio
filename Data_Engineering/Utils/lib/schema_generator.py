from google.cloud import bigquery
from genson import SchemaBuilder

class SchemaGenerator:
    def __init__(self):
        pass
    
    def transformBigquerySchemaToJsonSchema(self,bq_schema):
        # print(bq_schema)
        def jsonType(field_type):
            json_type = None
            if field_type == "STRING":
                json_type = "string"
            elif field_type == "FLOAT":
                json_type = "number"
            elif field_type == "INTEGER":
                json_type = "integer"
            elif field_type == "BOOLEAN":
                json_type = "boolean"
            else:
                json_type = "string"
            return json_type
        
        """Transforms a BigQuery schema to JSON schema."""
        def processField(field):
            """Process a single BigQuery field recursively."""
            field_type = field.field_type
            mode = field.mode
            if mode == "REPEATED":
                if field_type == "RECORD":
                    record_fields = [{"name":sub_field.name.replace('-','_'), "content":processField(sub_field)} for sub_field in field.fields]
                    return {'type':'array','items':{"type": "object",
                                                    "properties": {sub_field["name"].replace('-','_'): sub_field["content"] for sub_field in record_fields},
                                                    "required": [sub_field["name"].replace('-','_') for sub_field in record_fields if sub_field.get("required") == True]}}
                else:
                    return {"type": "array", "items": {"type": jsonType(field_type)}}
            elif field_type == "RECORD" and mode != "REPEATED":
                record_fields = [{"name":sub_field.name.replace('-','_'), "content":processField(sub_field)} for sub_field in field.fields]
                return {
                    "type": "object",
                    "properties": {sub_field["name"].replace('-','_'): sub_field["content"] for sub_field in record_fields},
                    "required": [sub_field["name"].replace('-','_') for sub_field in record_fields if sub_field.get("required") == True]
                }
            else:
                
                return {"type" :jsonType(field_type)}

        return {'$schema': 'http://json-schema.org/schema#',
            "type": "object",
            "properties": {field.name.replace('-','_'): processField(field) for field in bq_schema},
            "required": [field.name.replace('-','_') for field in bq_schema if field.mode == "REQUIRED"]
        } 
    
    def generateSchema(self,data,base_schema=[]):
        schemaBuilder = SchemaBuilder()
        base_schema = self.transformBigquerySchemaToJsonSchema(base_schema)
        schemaBuilder.add_schema(base_schema)
        for input in data:
            schemaBuilder.add_object(input)
        json_schema = schemaBuilder.to_schema()
        def mapJsonTypeToBigquery(json_type,field_name):
            """Map JSON schema types to BigQuery types."""
            type_mapping = {
                "string": "STRING",
                "number": "FLOAT",
                "integer": "INTEGER",
                "boolean": "BOOLEAN",
                "object": "RECORD"
            }
            if field_name.endswith('_at') or 'timestamp' in field_name.lower():
                return 'TIMESTAMP'
            else:
                return type_mapping.get(json_type, "STRING") 

        def getFirstNonNullType(json_type):
            """Get the first non-null type from a list or single type."""
            if isinstance(json_type, list):
                for t in json_type:
                    if t != "null":
                        return t
            return json_type

        def processJsonField(field_name, field_info):
            """Process a single JSON field recursively."""
            json_type = field_info.get("type")
            mode = "NULLABLE"
            if "anyOf" in field_info:
                any_of = field_info.get("anyOf")
                if any_of:
                    for schema in any_of:
                        if schema.get("type") != "null":
                            if schema["type"] == "array":
                                mode = "REPEATED"
                                # print(field_name,schema)
                                if "anyOf" in schema["items"]:
                                    for sub_schema in schema["items"]["anyOf"]:
                                        if sub_schema["type"] != "null":
                                            json_type = sub_schema["type"]
                                            field_info = sub_schema
                                            break
                                else:
                                    json_type = schema["items"]["type"]
                                    # print(schema,json_type)
                                    field_info = schema["items"]
                                break
                            else:
                                json_type = schema.get("type")
                                field_info = schema
                                break
            if isinstance(json_type, list):
                json_type = getFirstNonNullType(json_type)
            if json_type == 'array':
                mode = "REPEATED"
                json_type = field_info.get("items").get("type")
                field_info = field_info.get("items")
            bq_type = mapJsonTypeToBigquery(json_type,field_name)
            if bq_type == "RECORD":
                record_fields = [
                    processJsonField(sub_field_name, sub_field_info)
                    for sub_field_name, sub_field_info in field_info.get("properties", {}).items()]
                if record_fields == []:
                    return bigquery.SchemaField(field_name.replace('-','_'), bq_type, mode="NULLABLE")
                return  bigquery.SchemaField(field_name.replace('-','_'), bq_type, mode=mode, fields=record_fields)
            else:
                return bigquery.SchemaField(field_name.replace('-','_'), bq_type, mode=mode)

        def transformJsonToBigquerySchema(json_schema):
            """Transforms a JSON schema to BigQuery schema."""
            return [
                processJsonField(field_name.replace('-','_') , json_schema["properties"][field_name]) for field_name in json_schema["properties"]]

        return transformJsonToBigquerySchema(json_schema)