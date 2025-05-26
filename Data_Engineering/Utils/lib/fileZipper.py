from zipfile import ZipFile
import os 
import glob 

class fileZipper():
    def __init__(self):
        pass

    def zipFiles(self, files, zipFileName):
        with ZipFile(zipFileName, 'w') as zip:
            for file in files:
                zip.write(file)
        return zipFileName

    def unzipFiles(self, zipFileName, unzipFolder):
        with ZipFile(zipFileName, 'r') as zip:
            zip.extractall(unzipFolder)
        return unzipFolder
    
    def zipFolder(self, folder, zipFileName):
        with ZipFile(zipFileName, 'w') as zip:
            files = glob.glob(folder+"/**", recursive=True)
            files = [x for x in files if os.path.isfile(x)]
            print(files)
            for file in files:
                zip.write(file)
        return zipFileName