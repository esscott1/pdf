import csv
import pandas as pd
import pylightxl as load_workbook
import io




wb = load_workbook.readxl(fn='c:/SampleData/pandatest.xlsx')

print(wb.ws_names)
#sheet_ranges =  wb['Sheet1']

#print(sheet_ranges['A1'].value)
#for sheet in wb:
#    print(f'----printing first row of sheet {sheet.title}all sheets ----')
#    for row in sheet.iter_rows(min_row=1,max_row=2):
#        for cell in row:
#            print(cell.value)

