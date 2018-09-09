import pandas as pd

# loading the file of game reviews
reviews_dataframe = pd.read_csv("ign.csv")
#  prints the first N rows of a DataFrame. By default 5.
print(reviews_dataframe.head())
#  prints the last N rows of a DataFrame. By default 5.
print(reviews_dataframe.tail())
#  Number of rows and columns
print(reviews_dataframe.shape)

# iloc method allows us to retrieve rows and columns by position or index
reviews_dataframe.iloc[:5,:]
 # – the first 5 rows, and all of the columns for those rows.
reviews_dataframe.iloc[:,:] 
# – the entire DataFrame.
reviews_dataframe.iloc[5:,5:] 
# – rows from position 5 onwards, and columns from position 5 onwards.
reviews_dataframe.iloc[:,0] 
# – the first column, and all of the rows for the column.
reviews_dataframe.iloc[9,:] 
# – the 10th row, and all of the columns for that row.

# let’s remove the first column, which doesn’t have any useful information:
reviews_dataframe = reviews_dataframe.iloc[:,1:]
reviews_dataframe.head()

#  pandas.DataFrame.loc method, which allows us to index using labels instead of positions.
reviews_dataframe.loc[:5,"score"]
# specify more than one column at a time by passing in a list:
reviews_dataframe.loc[:5,["score", "release_year"]]

reviews_dataframe.iloc[:,1] 
# – will retrieve the second column.
reviews_dataframe.loc[:,"score_phrase"] 
# – will also retrieve the second column.

# a third, even easier, way to retrieve a whole column. 
# We can just specify the column name in square brackets, like with a dictionary:

reviews["score"]
reviews[["score", "release_year"]]

# A DataFrame stores tabular data, but a Series stores a single column or row of data.
# We can verify that a single column is a Series:
# each column in a DataFrame is a Series object:
type(reviews["score"])
# pandas.core.series.Series

s1 = pd.Series([1,2])
s2 = pd.Series(["Boris Yeltsin", "Mikhail Gorbachev"])

#  create a DataFrame by passing multiple Series into the DataFrame class
pd.DataFrame([s1,s2])

frame = pd.DataFrame(
    [
        [1,2],
        ["Boris Yeltsin", "Mikhail Gorbachev"]
    ],
    index=["row1", "row2"],
    columns=["column1", "column2"]
)
# an skip specifying the columns keyword argument if we pass a dictionary into the DataFrame constructor. 
# This will automatically setup column names:
frame = pd.DataFrame(
    {
        "column1": [1, "Boris Yeltsin"],
        "column2": [2, "Mikhail Gorbachev"]
    }
)

reviews["score"].mean()
# find the mean of each numerical column in a DataFrame by default:
reviews.mean()
# compute the mean of the numerical values in each row:
reviews.mean(axis=1)
