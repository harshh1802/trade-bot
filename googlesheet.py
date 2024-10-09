#Importing libraries
import sys
import path
dir = path.Path(__file__).abspath()
sys.path.append(dir.parent.parent)


import gspread
from oauth2client.service_account import ServiceAccountCredentials


#Scopes for google api account
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(r"creds.json", scope)
client = gspread.authorize(creds)

class Sheet:

	def __init__(self,sheetName):
		self.sheetName = sheetName

	#Get worksheet
	def getWorksheet(self,worksheetName):
		sheet = client.open(self.sheetName)
		main = sheet.worksheet(worksheetName)
		return main.get_all_records()

	#get value of cell
	def getCell(self,worksheetName,cell):
		sheet = client.open(self.sheetName)
		main = sheet.worksheet(worksheetName)
		return main.acell(cell).value

	#Update value of cell
	def updateCell(self,worksheetName,cell,value):
		sheet = client.open(self.sheetName)
		main = sheet.worksheet(worksheetName)
		main.update(cell,value)

	#Get worksheet object
	def getWorksheetObject(self,worksheetName):
		sheet = client.open(self.sheetName)
		main = sheet.worksheet(worksheetName)
		return main

	#Add/Create other worksheet
	def addWorksheet(self,newWorksheetName):
		sheet = client.open(self.sheetName)
		sheet.add_worksheet(title=newWorksheetName, rows="1000", cols="50")


if __name__ == "__main__":
	a = Sheet("NOCD")
	print(a.getWorksheet("api"))

	