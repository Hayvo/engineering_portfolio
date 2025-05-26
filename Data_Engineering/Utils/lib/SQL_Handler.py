import regex as re

def input_project_id(sql,project_id):
    sql = re.sub(r'{{project_id}}',f'{project_id}',sql)
    return sql

def refactor_var_reference(line):
    return re.sub("./src/var/login_credentials","./var/login_credentials",line)