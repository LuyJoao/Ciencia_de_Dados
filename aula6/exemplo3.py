import sqlite3
import pandas as pd

con = sqlite3.connect("mydata.sqlite")

df = pd.read_sql_query("SELECT * FROM test", con)

print(df)

con.close()