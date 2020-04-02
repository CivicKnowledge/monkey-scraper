import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials\
    .from_json_keyfile_name('../../uwsd-covid-19-data-48c99e07e2c8.json', scope)

gc = gspread.authorize(credentials)

wks = gc.open("UWSD Covid-19 Application Files").sheet1

df = gd.get_as_dataframe(wks)

print(df.head())
