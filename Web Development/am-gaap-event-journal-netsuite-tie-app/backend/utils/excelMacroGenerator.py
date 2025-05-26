import os
import xlwings as xw
import subprocess

class ExcelMacroGenerator:
    def __init__(self):
        os.makedirs('temp', exist_ok=True)  # Ensure temp folder exists
        pass

    def listFields(self, fields):
        '''Convert list of fields to string'''
        return '", "'.join(fields)

    def createPivotMacro(self, dataSourceSheet: str, destinationSheet: str, destinationCell: tuple,
                         pivotName: str, rowFields: list, columnFields: list, valueFields: list, macroName: str):
        '''Generate pivot table macro'''
        pivotMacro = f'''
        Sub {macroName}()
            'Declare Variables
            Dim PSheet As Worksheet
            Dim DSheet As Worksheet
            Dim PCache As PivotCache
            Dim PTable As PivotTable
            Dim PRange As Range
            Dim LastRow As Long
            Dim LastCol As Long

            'Check if sheet exists, if not, create it
            On Error Resume Next
            Set PSheet = Worksheets("{destinationSheet}")
            If PSheet Is Nothing Then
                Sheets.Add(After:=ActiveSheet).Name = "{destinationSheet}"
                Set PSheet = Worksheets("{destinationSheet}")
            End If
            On Error GoTo 0

            Set DSheet = Worksheets("{dataSourceSheet}")

            'Define Data Range
            LastRow = DSheet.Cells(Rows.Count, 1).End(xlUp).Row
            LastCol = DSheet.Cells(1, Columns.Count).End(xlToLeft).Column
            Set PRange = DSheet.Cells(1, 1).Resize(LastRow, LastCol)

            'Define Pivot Cache
            Set PCache = ActiveWorkbook.PivotCaches.Create( _
                SourceType:=xlDatabase, SourceData:=PRange)

            'Create Pivot Table
            Set PTable = PCache.CreatePivotTable( _
                TableDestination:=PSheet.Cells({destinationCell[0]}, {destinationCell[1]}), _
                TableName:="{pivotName}")

            'Insert Row Fields
            Dim i As Integer
            i = 1
            For Each field In Array("{self.listFields(rowFields)}")
                With PTable.PivotFields(field)
                    .Orientation = xlRowField
                    .Position = i
                End With
                i = i + 1
            Next

            'Insert Column Fields
            Dim j As Integer
            j = 1
            For Each field In Array("{self.listFields(columnFields)}")
                With PTable.PivotFields(field)
                    .Orientation = xlColumnField
                    .Position = j
                End With
                j = j + 1
            Next

            'Insert Value Fields
            For Each field In Array("{self.listFields(valueFields)}")
                With PTable.PivotFields(field)
                    .Orientation = xlDataField
                    .Function = xlSum
                    .NumberFormat = "$#,##0.00"
                    .Name = "Sum of " & field
                End With
            Next

            'Format Pivot
            PTable.ShowTableStyleRowStripes = True
            PTable.TableStyle2 = "PivotStyleMedium9"

        End Sub
        '''
        return pivotMacro

    def saveMacro(self, macro, fileName):
        '''Save macro to file'''
        with open(fileName, 'w') as f:
            f.write(macro)
        return f"Macro saved to {fileName}"

    def embedMacroInExcel(self, wb: xw.Book, macro: str):
        '''Embed a VBA macro into an Excel workbook'''
        import pywintypes

        macro_file = os.path.abspath("temp/temp_macro.bas")
        self.saveMacro(macro, macro_file)

        # Import macro
        try:
            wb.api.VBProject.VBComponents.Import(macro_file)
        except pywintypes.com_error:
            print("Error importing macro. Enabling programmatic access to VBA project and trying again")
            self.enableVBAProgramaticAccess()
            wb.api.VBProject.VBComponents.Import(macro_file)
        
        os.remove(macro_file)

        return f"Macro added and saved"

    def executeMacro(self, wb: xw.Book, macroName: str):
        '''Execute a VBA macro in an Excel workbook'''
        wb.macro(macroName).run()

        return f"Macro {macroName} executed successfully"

    def enableVBAProgramaticAccess(self):
        """Enable programmatic access to the VBA project in Excel by creating AccessVBOM if missing."""
        try:
            import win32api
            import win32con

            key = win32api.RegOpenKeyEx(win32con.HKEY_CURRENT_USER,
                                        "Software\\Microsoft\\Office\\16.0\\Excel"
                                        + "\\Security", 0, win32con.KEY_ALL_ACCESS)
            win32api.RegSetValueEx(key, "AccessVBOM", 0, win32con.REG_DWORD, 1)
                        
            print("AccessVBOM added and set to 1. Restart Excel for changes to apply.")

        except Exception as e:
            print("Failed to modify registry:", e)
