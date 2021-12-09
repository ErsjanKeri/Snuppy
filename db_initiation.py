import sqlite3 
import pymongo 
import ssl

import time 
from init_neo import Neo4J
import json 


is_sqlite = False
is_mongo =  False
is_neo4j = False



with open('data.json', 'r') as f:
	STARTING_DATA = json.loads(f.read())


 
def init_sqlite():
	print("[+] Initializing Sqlite3")
	# sqlite3
	con = sqlite3.connect('main.db')
	cur = con.cursor()
	for i in STARTING_DATA:
		columns = ', '.join([f"{key} {value}" for key, value in list(i['columns'].items())])
		create_query = f"CREATE TABLE {i['table']} ( {columns} )"

		cur.execute(create_query)
		con.commit()

		for j in i['data']:
	
			insert_query = f"INSERT INTO {i['table']} VALUES ({', '.join(list(map(lambda k: f':{k}', j.keys())))})"
			cur.execute(insert_query, j)
			con.commit()


def init_mongo():
	print("[+] Initializing pymongo")
	CONNECTION_STRING = "mongodb+srv://user:realtesting@real.hopxr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
	cluster = pymongo.MongoClient(CONNECTION_STRING)

	# mongo db
	
	# this is specifically needed for mongodb to assign the id 
	def convert_id(item):
		item['_id'] = item['id']
		return item 

	
	mydb = cluster['mydb']

	for i in STARTING_DATA:
	 	new_col = mydb[i['table']]		
	 	new_col.insert_many(list(map(convert_id, i['data'])))



def init_neo4j():
	print("[+] Initializing Neo4j")
	conn = Neo4J(
			uri='bolt://54.234.52.14:7687',
			user='neo4j',
			password='decision-washtub-specialist',
		)
	conn.init_db()
	for i in STARTING_DATA:
		conn.query(i['neo4j'], parameters = { 'rows' : i['data']})




if __name__ == '__main__':
	if not is_sqlite:
		init_sqlite()
	if not is_mongo:
		init_mongo()
	if not is_neo4j:
		init_neo4j()


