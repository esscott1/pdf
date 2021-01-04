import csv
import pandas as pd
import pylightxl as load_workbook
import io
import pandas as pd
import json




wb = load_workbook.readxl(fn='c:/SampleData/pandatest.xlsx')
dict = {}
all_data = []
print(wb.ws_names)
print(wb.ws('Sheet1').row(1))
columns = wb.ws('Sheet1').row(1)
for i in range(len(columns)):
    dict[columns[i]] = wb.ws('Sheet1').row(2)[i]

print(f"json is: {json.dumps(dict)}")


print(type(wb.ws('Sheet1').row(1)))
print(json.dumps(wb.ws('Sheet1').row(1)))
#sheet_ranges =  wb['Sheet1']

#print(sheet_ranges['A1'].value)
#for sheet in wb:
#    print(f'----printing first row of sheet {sheet.title}all sheets ----')
#    for row in sheet.iter_rows(min_row=1,max_row=2):
#        for cell in row:
#            print(cell.value)

