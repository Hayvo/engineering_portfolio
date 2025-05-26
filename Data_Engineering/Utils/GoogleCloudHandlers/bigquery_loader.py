from google.cloud import bigquery
import copy
import traceback
from lib.schema_generator import SchemaGenerator
import json

class BigQueryLoader:
    def __init__(self,adminServiceAccount,debug = False):
        self.BQproject = adminServiceAccount['project_id']
        self.storageServiceAccountCredential = adminServiceAccount
        self.SchemaGenerator = SchemaGenerator()
        self.debug = debug

    def removeDuplicatesFields(self,data : list) -> list:
        def recRemoveDuplicatesFields(json : dict) -> dict:
            if isinstance(json,dict):
                new_json = {}
                visited = {}
                for (field) in json:
                    if visited.get(str(field).lower(),False):
                        continue
                    else:
                        new_json[field.replace('-','_').replace('\r','').replace(' ','').replace('$','').replace('\ufeff','').replace('\"','')] = recRemoveDuplicatesFields(json[field])
                        visited[str(field).lower()] = True
                return new_json
            elif isinstance(json,list):
                return [recRemoveDuplicatesFields(unit) for unit in json]
            elif isinstance(json,str):
                return json.replace('\r','')
            else:
                return json
        return recRemoveDuplicatesFields(data)
    
    def formatJSON(self, data : list, platform : str) -> list:
        new_data = copy.deepcopy(data)
        def recFunc(dictio):
            if not isinstance(dictio,dict):
                return dictio
            for field in dictio:
                if platform not in ('masonhub','shopify') and field == 'version':
                    dictio[field] = int(eval(dictio[field]))
                if isinstance(dictio[field],dict):
                    if dictio[field] == {}:
                        dictio[field] = None
                    else:
                        dictio[field] = recFunc(dictio[field])
                if isinstance(dictio[field],list):
                    if dictio[field] == [{}]:
                        dictio[field] = None
                    elif len(dictio[field]) != 0:
                        if all(isinstance(unit,dict) for unit in dictio[field]):
                            dictio[field] = [recFunc(unit) for unit in dictio[field]]
                        else:
                            dictio[field] = dictio[field]
                    else:
                        dictio[field] = None
            return dictio
        for i,dictio in enumerate(new_data):
            new_data[i] = recFunc(dictio)
        return new_data

    def getSchema(self,new_data : list,client : str,dataset_ref : str,BQtable : str ,base_table : str  = None,force_schema :bool = False) -> list:
        table_ref = dataset_ref.table(BQtable)
        if force_schema:
                print('Forcing schema...')
                schema = self.SchemaGenerator.generateSchema(new_data)
        else:   
            print('Getting schema...')
            if base_table is not None:
                try:
                    base_table_ref = dataset_ref.table(base_table)
                    client_base_table = client.get_table(base_table_ref)
                    print('Provided base table found, getting schema...')
                    schema = client_base_table.schema
                    return schema
                except Exception as e:      
                    print('Provided base table not found, generating schema...')
                    schema = self.SchemaGenerator.generateSchema(new_data)
                    # traceback.print_exc()
            else:
                if BQtable.endswith('_temp'):
                    new_BQtable = BQtable[:-5]
                    new_table_ref = dataset_ref.table(new_BQtable)
                    try:
                        client_new_table = client.get_table(new_table_ref)
                        print('Base table found, getting schema...')
                        schema = client_new_table.schema
                    except Exception as e:
                        print('No base table found, looking for temp table...')
                        try:
                            client_table = client.get_table(table_ref)
                            print('Temp table found, getting schema...')
                            schema = client_table.schema
                        except Exception as e:
                            schema = self.SchemaGenerator.generateSchema(new_data)
                            print('Temp table not found, generating schema...')
                else:
                    try:
                        client_table = client.get_table(table_ref)
                        print('Already a base table, getting schema...')
                        schema = client_table.schema
                    except Exception as e:
                        # traceback.print_exc()
                        schema = self.SchemaGenerator.generateSchema(new_data)
                        print('Table not found, generating schema...')
                
        return schema
    

    def enforce_schema_types(self, data : list, schema : list) -> list:
        """
        Preprocesses the data to ensure that all fields match their types in the schema.
        If a field is defined as STRING in the schema, non-string values are converted to strings.
        """

        def cast_value(value, field_type):
            try:   
                if field_type == "STRING":
                    return str(value) 
                elif field_type == "FLOAT":
                    return float(value)
                elif field_type == "INTEGER":
                    return int(value)
                elif field_type == "BOOLEAN":
                    return bool(value)
                else:
                    return value
            except Exception as e:
                print(f"Error casting value {value} to type {field_type}")
                return None

        def convert_value(value, field):
            if field.field_type == "RECORD" and not isinstance(value, dict):
                # Convert non-records to empty records
                return {key.name : cast_value(value,key.field_type) for key in field.fields}

            if field.field_type == "STRING" :
                if field.mode == "REPEATED":
                    # Convert each value in the list
                    return value if isinstance(value, list) else [str(item) for item in value]
                else:
                    # Convert non-string values to strings
                    return json.dumps(value) if isinstance(value, (dict)) else str(value)
            return value

        def process_record(record, schema_fields):
            processed_record = {}
            for field in schema_fields:
                field_name = field.name
                field_type = field.field_type  # Example: STRING, INTEGER, etc.
            
                if field_name in record:
                    value = record[field_name]
                    if value is not None:  # Only process non-null values
                        if field_type == "RECORD" and isinstance(value, dict):
                            # Handle nested RECORDs (recursively process subfields)
                            processed_record[field_name] = process_record(value, field.fields)
                        elif field_type == "RECORD" and isinstance(value, list):
                            # Handle repeated RECORDs (process each sub-record)
                            processed_record[field_name] = [
                                process_record(item, field.fields) for item in value
                            ]
                        else:
                            # Convert or validate the value
                            processed_record[field_name] = convert_value(value, field)
                    else:
                        # Preserve null values
                        processed_record[field_name] = None
                else:
                    # If the field is not present, set it to None
                    processed_record[field_name] = None
            return processed_record

        # Process each record in the data
        return [process_record(record, schema) for record in data]


    def loadDataToBQ(self,data :list ,BQdataset : list ,BQtable : list ,platform : str ,base_table : str = None,force_schema : bool = False,WRITE_DISPOSITION : str ='WRITE_TRUNCATE') -> None: 
        # client = bigquery.Client(project=self.BQproject)
        client = bigquery.Client.from_service_account_info(self.storageServiceAccountCredential)
        dataset_ref = client.dataset(BQdataset, project = self.BQproject)
        table_ref = dataset_ref.table(BQtable)  
        try:
            dataset = client.get_dataset(dataset_ref)
            print('Dataset found')
        except Exception as e:
            print('Dataset not found, creating dataset...')
            dataset = bigquery.Dataset(dataset_ref)
            dataset = client.create_dataset(dataset)
            print('Dataset created')
        print('Removing duplicates fields and formatting JSON...')
        
        new_data = self.removeDuplicatesFields(self.formatJSON(data,platform))
        schema = self.getSchema(new_data,client,dataset_ref,BQtable,base_table,force_schema)
         
        
        new_data = self.enforce_schema_types(new_data, schema)
        
        # with open('schema.json','w') as f:
        #     f.write(json.dumps(self.SchemaGenerator.transformBigquerySchemaToJsonSchema(schema),indent=6))
        # with open('data.json','w') as f:
        #     f.write(json.dumps(new_data,indent=6))

        if not(self.debug):
            try:
                print('Loading data to BigQuery...')
                job_config = bigquery.job.LoadJobConfig(schema = schema, autodetect = False)
                if WRITE_DISPOSITION == "WRITE_TRUNCATE":
                    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                elif WRITE_DISPOSITION == 'WRITE_APPEND':
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
                job = client.load_table_from_json(new_data,table_ref,job_config = job_config,num_retries=10)
                print(job.result()) 
            except Exception as e:
                print('Error loading data to BigQuery, trying to force schema...')
                traceback.print_exc()
                schema = self.SchemaGenerator.generateSchema(new_data,schema)
                if WRITE_DISPOSITION == "WRITE_APPEND":
                    print("Updating schema of temp table...")
                    table_ref = dataset_ref.table(BQtable)
                    client_table = client.get_table(table_ref)
                    client_table.schema = schema
                    client.update_table(client_table,['schema'])
                    print('Schema updated')
                job_config = bigquery.job.LoadJobConfig(schema = schema, autodetect = False)
                if WRITE_DISPOSITION == "WRITE_TRUNCATE":
                    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                elif WRITE_DISPOSITION == 'WRITE_APPEND':
                    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
                job = client.load_table_from_json(new_data,table_ref,job_config = job_config,num_retries=10)
                print(job.result()) 
                try:
                    if base_table is not None:
                        print('Updating schema of base table...')
                        base_table_ref = dataset_ref.table(base_table)
                        client_base_table = client.get_table(base_table_ref)
                        client_base_table.schema = schema
                        try:
                            client.update_table(client_base_table,['schema'])
                            print('Schema updated')
                        except Exception as e:
                            traceback.print_exc()
                            print('Error updating schema of base table')
                    elif BQtable.endswith('_temp'):
                            new_BQtable = BQtable[:-5]
                            new_table_ref = dataset_ref.table(new_BQtable)
                            try:
                                client_new_table = client.get_table(new_table_ref)
                                client_new_table.schema = schema
                                client.update_table(client_new_table,['schema'])
                                print('Schema updated')
                            except Exception as e:
                                print('Error updating schema of base table')
                                traceback.print_exc()
                except Exception as e:
                    print('No base table found')
                    traceback.print_exc
        else:
            print('Debug mode, not loading data to BigQuery')
            schema = self.SchemaGenerator.generateSchema(new_data,schema)
            print(schema)

