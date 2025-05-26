import regex as re

class ErrorAnalyzer():
    def __init__(self):
        """Initialize the ErrorAnalyzer class."""
        pass

    def analyze_error(self, error_message):
        """Analyze the error message and return the error type."""
        
        # Check for Type not found
        if re.search(r"Type not found", error_message):
            return "TypeError", ""
        
        # Check for Not found: Table with regex to capture the table name
        elif re.search(r"Not found: Table", error_message):
            match = re.search(r"Not found: Table [\w-]+.[\w-_]+.dl_([\w-_]+)", error_message)
            if match:
                return "TableNotFoundError", match.group(1)
            else:
                return "TableNotFoundError", ""
        
        # Check for Syntax error with regex to capture the identifier
        elif re.search(r"Syntax error", error_message):
            match = re.search(r'identifier \\"([\w-]+)\\"', error_message)
            if match:
                return "SyntaxError", match.group(1)
            else:
                return "SyntaxError", ""
        
        # Check for Aggregation error
        elif re.search(r"neither grouped nor aggregated", error_message):
            return "AggregationError", ""
        
        elif re.search(r"unction not found", error_message):
            return "FunctionNotFoundError", ""

        elif re.search(r"uplicate table alias", error_message):
            return "DuplicateTableAlias", ""

        elif re.search(r"Unrecognized name", error_message) or re.search("not found inside",error_message):
            return "UnrecognizedColumn", ""
        
        elif re.search(r"atching signature", error_message):
            return "TypeMismatch", ""

        else:
            return "UnknownError", ""