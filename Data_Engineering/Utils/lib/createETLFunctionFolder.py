import shutil
import os
import glob 
import traceback
from SQL_Handler import input_project_id,refactor_var_reference

currentOsUser = os.getlogin()

class FunctionFolderMaker():
    def __init__(self):
        pass

    def deleteZippedFolder(self, folder):
        try:
            os.remove(f"./src/temp/{folder}.zip")
        except:
            pass

    def createFunctionFolder(self, project, platform):

        destFolder = f"{project}_{platform}".replace("-","_")

        if os.path.exists(f"./src/temp/{destFolder}"):
            shutil.rmtree(f"./src/temp/{destFolder}")
        try:
            os.mkdir(f"./src/temp/"+destFolder)
            ETLs = glob.glob(f"./src/ETLs/{platform}/etl_*")
            main = glob.glob(f"./src/ETLs/{platform}/main*")
            lib = glob.glob("./src/lib/*")
            lib = [x for x in lib if "__pycache__" not in x]
            var = glob.glob(f"./src/var/login_credentials/{project}/{platform}*")+glob.glob(f"./src/var/login_credentials/{project}/admin_service_account.json")
            req = glob.glob(f"./src/requirements.txt")
            # print(ETLs, lib, main, var)
            for file in ETLs:
                if not os.path.exists(f"./src/temp/{destFolder}"):
                    os.mkdir(f"./src/temp/{destFolder}")
                shutil.copy(file, f"./src/temp/{destFolder}")
            for file in lib:
                if not os.path.exists(f"./src/temp/{destFolder}/lib"):
                    os.mkdir(f"./src/temp/{destFolder}/lib")
                shutil.copy(file, f"./src/temp/{destFolder}/lib")
            for file in main:
                shutil.copy(file, f"./src/temp/{destFolder}")
                fileName = file.replace("\\", "/").split("/")[-1]
                os.rename(f"./src/temp/{destFolder}/{fileName}", f"./src/temp/{destFolder}/main.py")
                
                with open(f"./src/temp/{destFolder}/main.py", "r") as f:
                    lines = f.readlines()
                
                # Refactor and filter lines
                new_lines = []
                for line in lines:
                    # Remove lines containing "if __name__ == '__main__':" or "main()"
                    if not (line.strip() == "if __name__ == '__main__':" 
                            or line.strip() == "main()"
                            or line.strip() == "if project_id == '{{project_id}}':"
                            or line.strip() == 'project_id = input("Please enter the project_id: ")'):
                        new_lines.append(refactor_var_reference(input_project_id(line, project)))
                
                with open(f"./src/temp/{destFolder}/main.py", "w") as f:
                    f.writelines(new_lines)
            for file in var:
                if not os.path.exists(f"./src/temp/{destFolder}/var/login_credentials/{project}"):
                    os.mkdir(f"./src/temp/{destFolder}/var")
                    os.mkdir(f"./src/temp/{destFolder}/var/login_credentials")
                    os.mkdir(f"./src/temp/{destFolder}/var/login_credentials/{project}")
                shutil.copy(file, f"./src/temp/{destFolder}/var/login_credentials/{project}")
            for file in req:
                shutil.copy(file, f"./src/temp/{destFolder}")

            # zip the folder
            if os.path.exists(f"./src/temp/{destFolder}.zip"):
                os.remove(f"./src/temp/{destFolder}.zip")
            try:
                shutil.make_archive(f"./src/temp/{destFolder}", 'zip', f"./src/temp/{destFolder}")
                shutil.rmtree(f"./src/temp/{destFolder}")
            except:
                traceback.print_exc()
                pass
        except:
            traceback.print_exc()
            shutil.rmtree(destFolder)
            os.mkdir(destFolder)
            pass
        

if __name__ == "__main__":
    FunctionFolderMaker().createFunctionFolder("bonjout-shopify","klaviyo")