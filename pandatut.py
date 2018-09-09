# Series  one-dimensional array-like object containing an array of data

# Creating Series
import pandas as pd

ds = pd.Series([2, 4, 6, 8, 10])
print(ds)
"""
0     2
1     4
2     6
3     8
4    10
dtype: int64
"""
print(type(ds))
# <class 'pandas.core.series.Series'>

# convert a Panda module Series to Python list and it's type
print(ds.tolist())
print(type(ds.tolist()))
"""
[2L, 4L, 6L, 8L, 10L]
<type 'list'>
"""
#  add, subtract, multiple and divide two Pandas Series.
ds1 = pd.Series([2, 4, 6, 8, 10])
ds2 = pd.Series([1, 3, 5, 7, 9])
ds = ds1 + ds2
print("Add two Series:")
print(ds)
print("Subtract two Series:")
ds = ds1 - ds2
print(ds)
print("Multiply two Series:")
ds = ds1 * ds2
print(ds)
print("Divide Series1 by Series2:")
ds = ds1 / ds2
print(ds)
"""
Divide Series1 by Series2:
0    2.000000
1    1.333333
2    1.200000
3    1.142857
4    1.111111
dtype: float64
"""

# Comparison
print("Compare the elements of the said Series:")
print("Equals:")
print(ds1 == ds2)
print("Greater than:")
print(ds1 > ds2)
print("Less than:")
print(ds1 < ds2)
"""
Less than:
0    False
1    False
2    False
3    False
4    False
dtype: bool
"""

# DateFrame - Collection of Pandas Series
# From Dictionary
df = pd.DataFrame({'X':[78,85,96,80,86], 'Y':[84,94,89,83,86],'Z':[86,97,96,72,83]});
print(df)
"""
    X   Y   Z                                                          
0  78  84  86                                                          
1  85  94  97                                                          
2  96  89  96                                                          
3  80  83  72                                                          
4  86  86  83 
"""

import numpy as np
# Create and display a DataFrame from a specified dictionary data which has the index labels
exam_data = {'name': ['Anastasia', 'Dima', 'Katherine', 'James', 'Emily', 'Michael', 'Matthew', 'Laura', 'Kevin', 'Jonas'],
'score': [12.5, 9, 16.5, np.nan, 9, 20, 14.5, np.nan, 8, 19],
'attempts': [1, 3, 2, 3, 2, 3, 1, 1, 2, 1],
'qualify': ['yes', 'no', 'yes', 'no', 'no', 'yes', 'yes', 'no', 'no', 'yes']}

labels = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

df = pd.DataFrame(exam_data , index=labels)
print(df)

print("Summary of the basic information about this DataFrame and its data:")
print(df.info())

print("First three rows of the data frame:")
print(df.iloc[:3])

print("Select specific columns:")
print(df[['name', 'score']])

# Select 'name' and 'score' columns in rows 1, 3, 5, 6
print("Select specific columns and rows:")
print(df.ix[[1, 3, 5, 6], ['name', 'score']])

# Select the rows where the score is missing
print("Rows where score is missing:")
print(df[df['score'].isnull()])

# select the rows the score is between 15 and 20 (inclusive).
print("Rows where score between 15 and 20 (inclusive):")
print(df[df['score'].between(15, 20)])

# select the rows where number of attempts in the examination is less than 2 and score greater than 15.
print("Rows where score between 15 and 20 (inclusive):")
print(df[(df['attempts'] < 3) & (df['score'] > 15)])

# change the score in row 'd' to 11.5.
print("\nChange the score in row 'd' to 11.5:")
df.loc['d', 'score'] = 11.5

# sum of the examination attempts by the students
print("\nSum of the examination attempts by the students:")
print(df['attempts'].sum())

print("\nMean score for each different student in data frame:")
print(df['score'].mean())

print("\nAppend a new row:")
df.loc['k'] = [1, 'Suresh', 'yes', 15.5]

print("\nDelete the new row and display the original  rows:")
df = df.drop('k')

print("Sort the data frame first by 'name' in descending order, then by 'score' in ascending order:")
df.sort_values(by=['name', 'score'], ascending=[False, True])

print("\nReplace the 'qualify' column contains the values 'yes' and 'no'  with True and  False:")
df['qualify'] = df['qualify'].map({'yes': True, 'no': False})

print("\nChange the name 'James' to 'Suresh':")
df['name'] = df['name'].replace('James', 'Suresh')

print("\nDelete the 'attempts' column from the data frame:")
df.pop('attempts')

# insert a new column in existing DataFrame.
print("\nInserting the 'color' column")
color = ['Red','Blue','Orange','Red','White','White','Blue','Green','Green','Red']
df['color'] = color

#  iterate over rows in a DataFrame.
exam_data = [{'name':'Anastasia', 'score':12.5}, {'name':'Dima','score':9}, {'name':'Katherine','score':16.5}]
df = pd.DataFrame(exam_data)
for index, row in df.iterrows():
    print(row['name'], row['score'])

# Common Excel Tasks Demonstrated in Pandas
df = pd.read_excel("excel-comp-data.xlsx")
df.head()

# Adding sales of Jan, Feb and March to get total Sale for each row
df["total"] = df["Jan"] + df["Feb"] + df["Mar"]
df.head()

# column level analysis 
print(df["Jan"].sum(), df["Jan"].mean(),df["Jan"].min(),df["Jan"].max())

sum_row=df[["Jan","Feb","Mar","total"]].sum()
df_sum=pd.DataFrame(data=sum_row).T
df_sum=df_sum.reindex(columns=df.columns)
df_final=df.append(df_sum,ignore_index=True)
df_final.tail()


# filter values of a column of all females who are not graduate and got a loan
data.loc[(data["Gender"]=="Female") & (data["Education"]=="Not Graduate") & (data["Loan_Status"]=="Y"), ["Gender","Education","Loan_Status"]]

#Create a new function:
def num_missing(x):
  return sum(x.isnull())

#Applying per column:
print "Missing values per column:"
print data.apply(num_missing, axis=0) #axis=0 defines that function is to be applied on each column

#Applying per row:
print "\nMissing values per row:"
print data.apply(num_missing, axis=1).head() #axis=1 defines that function is to be applied on each row

# updating missing values with the overall mean/mode/median of the colum
#First we import a function to determine the mode
from scipy.stats import mode
mode(data['Gender'])

# Output: ModeResult(mode=array([‘Male’], dtype=object), count=array([489]))
# This returns both mode and count.There can be multiple values with high frequency. We will take the first one by default always using:
mode(data['Gender']).mode[0]
# 'Male

#  fill the missing values 
#Impute the values:
data['Gender'].fillna(mode(data['Gender']).mode[0], inplace=True)
data['Married'].fillna(mode(data['Married']).mode[0], inplace=True)
data['Self_Employed'].fillna(mode(data['Self_Employed']).mode[0], inplace=True)

#Now check the #missing values again to confirm:
print data.apply(num_missing, axis=0)