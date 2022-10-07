import sqlite3
import pickle
 
file = "kv_database.db"
  
try:
  conn = sqlite3.connect(file)
  print("Database kv_database.db formed.")
except:
  print("Database kv_database.db not formed.")
 
# cursor
crsr = conn.cursor()
 
# SQL command to create a table in the database
sql_create_raw = """CREATE TABLE raw_data (
key VARCHAR(30) PRIMARY KEY,
value blob);"""

sql_create_intermediate = """CREATE TABLE intermediate_data (
key VARCHAR(30) PRIMARY KEY,
value blob);"""

sql_create_output = """CREATE TABLE output_data (
key VARCHAR(30) PRIMARY KEY,
value blob);"""
crsr.execute(sql_create_raw)
crsr.execute(sql_create_intermediate)
crsr.execute(sql_create_output)

# close the connection
conn.close()