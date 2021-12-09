from neo4j import GraphDatabase


class Neo4J:
	db = 'real'
	
	def __init__(self, uri, user, password):
		self.__uri = uri
		self.__user = user
		self.__pwd = password
		self.__driver = None 

		

		try:
			self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
		except Exception as e:
			print(e)


	
	def init_db(self):
		session = self.__driver.session()
		response = list(session.run(f"CREATE OR REPLACE DATABASE {self.db}"))
	


	def close(self):
		if self.__driver is not None:
			self.__driver.close()



	def query(self, query, parameters = None):
		assert self.__driver is not None, "Driver is not initialized"
		session = None 
		response = None 


		try:
			session = self.__driver.session(database = self.db) if self.db is not None else self.__driver.session()
			response = list(session.run(query, parameters))

		except Exception as e:
			print(f"[-] Failed: {e}")

		finally:
			if session is not None:
				session.close()


		return response 

