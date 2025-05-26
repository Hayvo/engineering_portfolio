import regex as re
import traceback
import json
import sqlglot
from collections import defaultdict
from sqlglot import exp

class QueryFormatter:

    def __init__(self):
        self.sql_keywords = [
            "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT", 
            "BETWEEN", "BY", "CASE", "CAST", "COLLATE", "CONTAINS", "CREATE", "CROSS", 
            "CUBE", "CURRENT", "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END", 
            "ENUM", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE", "FETCH", 
            "FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPING", "GROUPS", "HASH", 
            "HAVING", "IF", "IGNORE", "IN", "INNER", "INTERSECT", "INTERVAL", "INTO", 
            "IS", "JOIN", "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE", "NATURAL", 
            "NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "OUTER", 
            "OVER", "PARTITION", "PRECEDING", "PROTO", "QUALIFY", "RANGE", "RECURSIVE", 
            "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME", "STRUCT", 
            "TABLESAMPLE", "THEN", "TREAT", "TRUE", "UNBOUNDED", "UNION", "UNNEST", 
            "USING", "WHEN", "WHERE", "WINDOW", "WITH", "WITHIN","DATE", "TIME", "TIMESTAMP",
            "DATE_ADD", "DATE_SUB", "DATE_DIFF", "DATE_TRUNC", "DATE_ADD", "DATE_SUB",
            "SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV", "STDDEV_POP", "STDDEV_SAMP",
            "VAR_POP", "VAR_SAMP", "VARIANCE", "ARRAY_AGG", "ARRAY_CONCAT", "ARRAY_LENGTH",
            "COALESCE", "CONCAT", "CONCAT_WS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "EXTRACT", "FORMAT_TIMESTAMP", "FORMAT_DATE", "FORMAT_TIME", "FORMAT_DATETIME",
            "TIMESTAMP_ADD", "TIMESTAMP_SUB", "TIMESTAMP_DIFF", "TIMESTAMP_TRUNC",
            "UNION ALL", "UNION DISTINCT", "INTERSECT ALL", "INTERSECT DISTINCT",
            "DISTINCT", "UNIQUE", "LIMIT", "OFFSET", "ORDER BY", "GROUP BY",
        ]

    def formatKeyWords(self, sql: str) -> str:
        """Format SQL keywords.
        
        Args:
            sql (str): The SQL to format.
        
        Returns:
            str: The formatted SQL.
        """
        sql = sql.lower() + "\n"
        try:
            for keyword in self.sql_keywords:
                # Match only full keywords, ensuring they are not inside placeholders
                sql = re.sub(
                    rf"(?<!{{)(?<!\w)([\(\t\n\s]*){keyword.lower()}([\(\t\n\s]*)\b", 
                    rf"\1{keyword}\2", 
                    sql, 
                    flags=re.IGNORECASE
                )
        
            # Replace now() with CURRENT_TIMESTAMP()
            sql = re.sub(r"\bnow\(\)\b", "CURRENT_TIMESTAMP()", sql, flags=re.IGNORECASE)

            return sql

        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting SQL keywords: {e}")
        
        
    def formatGroupOrderBy(self, sql : str) -> str:
        """Format GROUP BY.
        Args:
            sql (str): The SQL to format.
        Returns:
            str: The formatted SQL."""
        try:
            sql = re.sub(r"GROUP BY\s+((?:\w+\((?:[^()]*|\w+\([^()]*\))*\)|\w+\.\w+|\w+)(?:\s*,\s*(?:\w+\((?:[^()]*|\>\<\w+\([^()]*\))*\)|\w+\.\w+|\w+))*)\s*(UNION ALL|\n|\)|;)?", r"GROUP BY ALL \2", sql, flags=re.IGNORECASE)
            sql = re.sub(r"ORDER BY\s+((?:\((?:[^()]+|\([^()]*\))*\)|[^,;\n\)]+)(?:\s*,\s*(?:\((?:[^()]+|\([^()]*\))*\)|[^,;\n\)]+))*)", r"", sql, flags=re.IGNORECASE)
            
            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting GROUP BY: {e}")

    def formatLineBreak(self, sql : str) -> str:
        """Format line breaks.
        Args:
            sql (str): The SQL to format.
        Returns:
            str: The formatted SQL."""
        try:
            sql =  re.sub(r"\n", " ", sql)
            sql = re.sub(r"\t", " ", sql)
            sql = re.sub(r"      ", " ", sql)
            # sql = re.sub(r"[\([\w+\.]?\w+]!(,) ", ",\n\t", sql)
            sql = re.sub(r"\bSELECT ", " \nSELECT\n\t", sql)
            sql = re.sub(r"\bFROM", " \nFROM", sql)
            sql = re.sub(r"\bWHERE", " \nWHERE", sql)
            sql = re.sub(r"\s*\bAND\b", " \n\tAND", sql)
            sql = re.sub(r"\bOR", " \n\tOR", sql)
            sql = re.sub(r"\bUNION ALL", " \nUNION ALL\n", sql)
            sql = re.sub(r"\bLEFT JOIN", " \nLEFT JOIN", sql)
            sql = re.sub(r"\bRIGHT JOIN", " \nRIGHT JOIN", sql)
            sql = re.sub(r"\bINNER JOIN", " \nINNER JOIN", sql)
            sql = re.sub(r"\bFULL JOIN", " \nFULL JOIN", sql)
            sql = re.sub(r"\bORDER BY", " \nORDER BY", sql)
            sql = re.sub(r"\bHAVING", " \nHAVING", sql)
            sql = re.sub(r"\bGROUP BY", " \nGROUP BY", sql)
            sql = re.sub(r"\bLIMIT", " \nLIMIT", sql)
            sql = re.sub(r"\bQUALIFY", " \nQUALIFY", sql)
            sql = re.sub(r"\bSET", " \nSET", sql)
            sql = re.sub(r"\bUPDATE", " \nUPDATE", sql)
            sql = re.sub(r"\bINSERT INTO", " \nINSERT INTO", sql)
            sql = re.sub(r"\bVALUES", " \nVALUES", sql)
            sql = re.sub(r"\bDELETE FROM", " \nDELETE FROM", sql)
            sql = re.sub(r"\s+ON", " \n\tON", sql)
            sql = re.sub(r";", "", sql)
            sql = re.sub(r"\bchar\b", "STRING", sql)
            sql = re.sub(r"\bsigned\b", "INT64", sql)
            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting line breaks: {e}")
        
    def formatProjectDataset(self, projectId : str, datasetId : str, sql : str) -> str:
        """Format project and dataset.
        Args:
            sql (str): The SQL to format.
        Returns:
            str: The formatted SQL."""
        try:
            sql = re.sub(r"\b(clover|cloverevent)\.", f"", sql)

            pattern = r"\b(?:FROM|JOIN)\n?\s+(?!\()((?:\w+\s+\w*)(?:\s*,\s*\w+\s+\w*)*)"

            def replacer(match):
                print(match.group(0))
                fromJoin = re.search(r"(FROM|JOIN)", match.group(0)).group(0)  # Captures FROM or JOIN
                tables = match.group(1)  # Captures all tables in FROM/JOIN
                transformed_tables = []
                
                for table in tables.split(","):
                    table = table.strip()
                    table_name, alias = table.split()  # Splitting table name and alias
                    transformed_tables.append(f"`{projectId}.{datasetId}.dl_{table_name}` {alias}")
                return f"{fromJoin} {', '.join(transformed_tables)}"

            sql = re.sub(pattern, replacer, sql, flags=re.IGNORECASE)
            
            sql = re.sub(r"(FROM|JOIN)\n*\s+(\w+)\s*(\n|\))", rf"\1 `{projectId}.{datasetId}.dl_\2` \3", sql, flags=re.IGNORECASE)

            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting project and dataset: {e}")

    def formatDateTimeFunction(self, sql : str) -> str:
        try:
            sql = re.sub(r"\b(curdate\(\))", 'CURRENT_DATE()', sql, flags=re.IGNORECASE)
            sql = re.sub(r"\b(now\(\))", 'CURRENT_TIMESTAMP()', sql, flags=re.IGNORECASE)
            sql = re.sub(r"DATE_FORMAT\(\s*((?:[^(),]+|\([^()]*\))*)\s*,\s*['\"].*?['\"]\s*\)", r"DATE(\1)", sql, flags=re.IGNORECASE)
            sql = re.sub(r"timestampdiff\(\s*((?:[^(),]+|\([^()]*\))*)\s*,\s*((?:[^(),]+|\([^()]*\))*)\s*,\s*((?:[^(),]+|\([^()]*\))*)\)", r"TIMESTAMP_DIFF(\3, \2, \1)", sql, flags=re.IGNORECASE)
            sql = re.sub(r"(?<!\([\s\S]*?)curdate\(\)(?![\s\S]*?\))", 'CURRENT_TIMESTAMP()', sql, flags=re.IGNORECASE)
            sql = re.sub(r"datediff\(\s*((?:[^(),]+|\([^()]*\))*)\s*,\s*([\w\.\`]+)\s*\)", r'DATE_DIFF(\1, \2, DAY)', sql, flags=re.IGNORECASE)
            sql = re.sub(r"(?<!\([\s\S]*?)DATE_DIFF\(CURRENT_TIMESTAMP\(\),\s*([\w\.\`]+)\)(?![\s\S]*?\))", r'DATE_DIFF(CURRENT_DATE(), \1)', sql, flags=re.IGNORECASE)
            sql = re.sub(r"dayofweek\(\s*((?:[^(),]+|\([^()]*\))*)\s*\)", r"EXTRACT(DAYOFWEEK FROM \1)", sql, flags=re.IGNORECASE)
            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting timestamp diff: {e}")

    def formatParam(self, params : list[dict], sql : str) -> str:
        """Format a parameter.
        Args:
            param (dict): The parameter to format.
            sql (str) : The query to format.
        Returns:
            str: The formatted SQL."""   
        try:
            regexParam = re.findall(r"'?{{\s*([^{}]+?)\s*}}'?", sql)
            # print(regexParam)
            for param in params:
                for regex in regexParam:
                    if param["Name"].lower() == regex.lower():
                        newParam = param['Name'].replace(" ","_")
                        newType = param['Type']
                        sql = re.sub(rf"'?\{{{{(\s*{regex}\s*)}}}}'?", f"{newParam}", sql)
                        sql = re.sub(rf"(\b[\w.]+\b)\s*([><=]+)\s*(\w+\(.*{newParam}.*\)|\s*{newParam}\s*)(\n|\))?", rf"SAFE_CAST(\1 AS {newType}) \2 \3 \4", sql)
            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting parameter: {e}")
        
    def formatComments(self, sql : str) -> str:
        try:
            sql = re.sub(r"--.*\n", "", sql)
            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting comments: {e}")
        
    def formatOtherFunction(self, sql : str) -> str:
        """
        Format other functions
        Args:
            sql (str): The SQL to format.
        Returns:
            str: The formatted SQL.
        """
        try:
            sql = re.sub(r"objectjson->>'\$\.(\w+)'", r"JSON_EXTRACT_SCALAR(objectjson, '$.\1')", sql)
            sql = re.sub(r"substring_index\(([^,]+),\s*'([^']+)',\s*(\d+)\)", r"SPLIT(\1, '\2')[SAFE_OFFSET(0)]", sql)
            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting JSON functions: {e}")

    def formatUserDefinedFunction(self, sql : str, projectId : str) -> str:
        """Format user-defined functions.
        Args:
            sql (str): The SQL to format.
        Returns:
            str: The formatted SQL."""
        try:
            functionList = ["dateDiffBusiness","ageBandValues","boxNumberGroupKPIReporting","boxNumberGroupReporting",
                            "customerBoxNumberGroup","customerLTVRange","customerSpendLevel","customerSpendLevelLTVScoreOnly",
                            "customerTotalSpendBrackets","homeValueRange","productBoxNumberAvailabilityInfoID",
                            "productBoxNumberAvailabilityInfoIDSubcategoryID","productBoxNumberAvailabilityProductID",
                            "productBuyTypeDynamicProductID","receiveingAgeBuckets","seasonForDate"]
            
            for function in functionList:
                sql = re.sub(rf"\b{function}\s*\(", rf"`{projectId}.DL_Redash.DL_Func_{function}`(", sql, flags=re.IGNORECASE)
            return sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting user-defined functions: {e}")

    def translateSQL(self, sql: str, projectId : str, datasetId : str) -> str:
        """Translate SQL to BigQuery syntax.
        
        Args:
            sql (str): The SQL to translate.
        
        Returns:
            str: The translated SQL.
        """
        try:
            expression = sqlglot.parse_one(re.sub(r"\b(clover|cloverevent)\.", f"", sql), read="mysql")
            for order in expression.find_all(sqlglot.exp.Order):
                order.parent.set("order", None)
            for group in expression.find_all(sqlglot.exp.Group):
                group.parent.set("group", " GROUP BY ALL")
            
            for expr in expression.find_all(sqlglot.exp.Table):
                table_name = expr.name
                if table_name == "membernotes":
                    table_name = "P_TD_DL_Membernotes_History"
                elif table_name == "inventoryadjustment":
                    table_name = "P_TD_DL_Inventoryadjustment_History"
                full_name = f"`{projectId}.{datasetId}.dl_{table_name}`"
                expr.set("this", full_name)

            translated_sql = sqlglot.transpile(
                expression.sql(dialect="mysql"), 
                write="bigquery",
                read="mysql")[0]
            return translated_sql
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error translating SQL: {e}")

    def formatQuery(self, projectId : str, datasetId : str, sql : str, params : list[dict]) -> str:
        """Format a query.
        Args:
            sql (str): The query to format.
            projectId (str): The project ID.
            datasetId (str): The dataset ID.
        Returns:
            str: The formatted query."""
        try:
            return  re.sub('\u0002','\n',
                    self.formatUserDefinedFunction(projectId= projectId, sql= 
                    # self.formatGroupOrderBy(
                    self.formatDateTimeFunction(
                    self.formatOtherFunction(
                    self.formatParam(params = params, sql =
                    # self.formatProjectDataset(projectId= projectId, datasetId= datasetId, sql= 
                    self.formatLineBreak(
                    self.formatComments(
                    self.translateSQL(datasetId= datasetId, projectId= projectId, sql=
                    self.formatKeyWords(
                    sql)))))))))
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error formatting query: {e}")