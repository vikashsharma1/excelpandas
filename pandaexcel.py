import pandas as pd


# Create a Pandas dataframe from some data.
df = pd.DataFrame({'Data': [10, 20, 30, 20, 15, 30, 45]})

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('pandas_simple.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
df.to_excel(writer, sheet_name='Sheet1')

# Close the Pandas Excel writer and output the Excel file.
writer.save()

# Reading Excel# load the sheet into the Pandas Dataframe structure
df = pd.read_excel('Pandas-Example.xlsx', sheetname='Sheet1')

print("The list of row indicies")
print(df.index)
print("The column headings")
print(df.columns)

print("The 'Patient' column information:")
print(df['Patient'])

# print each row of the patient column
for i in df.index:
    print(df['Patient'][i])

# compute a new column as the product of two other columns
for i in df.index:
    df['BP*SO2'][i] = df['BP'][i] * df['SO2'][i]

print("results of column multiplication:")
print(df['BP*SO2'])

# write the dataframe back out with the new column data included
writer = ExcelWriter('Pandas-Example-Out.xlsx')
df.to_excel(writer,'Sheet1',index=False)
writer.save()


# Adding New Sheet to Eisting Excel File
import pandas
from openpyxl import load_workbook

book = load_workbook('Masterfile.xlsx')
writer = pandas.ExcelWriter('Masterfile.xlsx', engine='openpyxl') 
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
#  Essentially these steps are just loading the existing data from 'Masterfile.xlsx' and populating your writer with them.

data.to_excel(writer, "Testsheetname", cols=['Diff1', 'Diff2'])
writer.save()
