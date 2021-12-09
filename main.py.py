from tkinter import * 
import sqlite3
import datetime 
import pymongo 
import ssl
from init_neo import Neo4J


# connection string for the mongo db database 
CONNECTION_STRING = "mongodb+srv://user:realtesting@real.hopxr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
					

# 1) “Find the name of all players that played in the 06/03/2021
# 2) “What is the most common bet on two aces?”
# 3) “Which player(s) has the most aggressive style of betting (largest average bet)?”
# 4) "Which person has bet the most?"
# 5) "Which person has won most games?" 

# query -> relational databases, graph databases
q_r = [{"squery" : """
					SELECT p.name FROM person p 
					JOIN (SELECT p.person_id person_id FROM player p
						JOIN tournament t ON p.tournament_id = t.id 
						WHERE t.date_held = '06/03/2021') sub 
					ON sub.person_id = p.id 
					""",
		"nquery" : """
					MATCH (n:Tournament {date_held : '06/03/2021'})
					MATCH (pl:Player {tournament_id : n.id})-[r:IS_PERSON]->(p:Person) 
					RETURN p.name

					""",
		"columns" : ['name']}, 

		
		{"squery" : """
					SELECT * FROM (
						SELECT amount, count(*) times FROM bet 
						WHERE cards = '2 Aces'
						GROUP BY amount) sub1 
					WHERE sub1.times = (SELECT max(sub2.times) FROM (
										SELECT amount, count(*) times FROM bet 
										WHERE cards = '2 Aces'
										GROUP BY amount) sub2)
					""",
		"nquery" :	"""

					MATCH (n:Bet {cards : '2 Aces'}) 
					RETURN n.amount, count(*) as times 
					ORDER BY times DESC limit 1
					""",
		"columns" : ["amount", "times"]},


		{"squery" : """
					SELECT p.name, max(amount) FROM (
						SELECT pla.person_id person_id, avg(bet.amount) amount FROM player pla 
						JOIN bet ON bet.player_id = pla.id 
						GROUP BY pla.person_id) sub1
					JOIN person p on p.id = sub1.person_id
					""",

		"nquery" : """

					MATCH (b:Bet)-[r:MADE_BY]->(pl:Player)
					MATCH (pl)-[:IS_PERSON]->(p:Person) 
					RETURN p.name, avg(b.amount) 
					ORDER BY avg(b.amount) DESC limit 1

		 			""",
		"columns" : ["name", "average amount"]},

		{"squery" : """
					SELECT person.name ,max(sub.amount) FROM  
						(SELECT play.person_id id, sum(bet.amount) amount FROM  player play
						JOIN bet ON bet.player_id = play.id 
						GROUP BY play.id) sub
					JOIN person ON person.id = sub.id
					""", 
		"nquery"  : """

					MATCH (b:Bet)-[r:MADE_BY]->(pl:Player) 
					MATCH (pl)-[:IS_PERSON]->(p:Person)
					RETURN p.name, b.amount
					ORDER BY b.amount DESC limit 1
 			
					""",
		"columns" : ["name", "amount"]},


		{"squery" : """
					SELECT p.name, max(sub.times) FROM 
						(SELECT pla.person_id person_id, count(*) times FROM player pla 
						JOIN game g ON g.winner = pla.id
						GROUP BY pla.person_id) sub
					JOIN person p on p.id = sub.person_id
					""",
		"nquery" :	"""

					MATCH (g:Game)-[r:WINNER]->(pl:Player) 
					MATCH (pl)-[:IS_PERSON]->(p:Person) 
					RETURN p.name, count(*) as wins
					ORDER BY wins DESC limit 1
					""",
		"columns" : ["name", "wins"]}]



class Application:
	results = []
	columns = []
	options = ['Sqlite3', 'Mongodb', 'Neo4j']

	
	# cursor of the relational database
	con = sqlite3.connect('main.db')
	cur = con.cursor()

	# cursor of the mongo database
	mongo = pymongo.MongoClient(CONNECTION_STRING, ssl_cert_reqs=ssl.CERT_NONE)['mydb']


	# cursor of the neo4j database
	neo = Neo4J(
			uri='bolt://54.234.52.14:7687',
			user='neo4j',
			password='decision-washtub-specialist',
		)


	def __init__(self) -> None:
		self.window = Tk()

		self.title_font = 'Verdana'
		self.title_size = 17 

		self.header_font = 'Arial'
		self.header_size = 13 

		
		self.db_options_id = IntVar(0)  # this sets Sqlite3 at default
		
		self.db_type = self.options[self.db_options_id.get()]

		self.select_query_frame = Frame(master = self.window, relief = GROOVE, borderwidth = 2)
		self.select_query_frame.pack(fill = 'x')

		self.query_1 = Button(master = self.select_query_frame, command = lambda: self.search(0), height='2' ,text = "Find the name of all players that played in the 06/03/2021")
		self.query_2 = Button(master = self.select_query_frame, command = lambda: self.search(1), text = 'What is the most common bet on two aces?')
		self.query_3 = Button(master = self.select_query_frame, command = lambda: self.search(2), text = 'Which player(s) has the most aggressive style of betting (largest average bet)?')
		self.query_4 = Button(master = self.select_query_frame, command = lambda: self.search(3), text = 'Which person has bet the most?')
		self.query_5 = Button(master = self.select_query_frame, command = lambda: self.search(4), text = "Which person has won most games?")


		self.query_1.grid(row = 0, column = 0, sticky='snwe', padx = 1)
		self.query_2.grid(row = 0, column = 1, sticky='snwe', padx = 1)
		self.query_3.grid(row = 0, column = 2, sticky='snwe', padx = 1)
		self.query_4.grid(row = 0, column = 3, sticky='snwe', padx = 1)
		self.query_5.grid(row = 0, column = 4, sticky='snwe', padx = 1)
						
		
		self.select_results_frame = Frame(master = self.window, relief = GROOVE, borderwidth = 2)
		self.select_results_frame.pack(fill = 'x')

		self.select_results_frame.rowconfigure([0,1], minsize=50, weight=1)
		self.select_results_frame.columnconfigure([0, 1, 2], minsize=200, weight=1)

		self.select_header_table = Frame(master = self.select_results_frame)
		self.select_header_table.grid(row = 0, column = 1)

		self.select_header_db = Frame(master = self.select_results_frame)
		self.select_header_db.grid(row = 0, column = 0)

		self.select_header_db_label = Label(master = self.select_header_db, text = self.db_type)
		self.select_header_db_label.pack()

		self.select_header_query_label = Label(master = self.select_header_table, text = '')
		self.select_header_query_label.pack()

		self.select_table_frame = Frame(master = self.select_results_frame)
		self.select_table_frame.grid(row = 1, column = 1)

		self.select_db_frame = Frame(master = self.select_results_frame)
		self.select_db_frame.grid(row = 1, column = 0)

		for index, instance in enumerate(self.options):
			self.radio_item = Radiobutton(master = self.select_db_frame, text = f"{instance.upper()}",
											variable  = self.db_options_id, value = index, command = self.select_db)


			self.radio_item.grid(row = index+1, sticky = 'w')


		self.window.title('Poker Database')
		self.window.mainloop()	


	def select_db(self):
		self.db_type = self.options[self.db_options_id.get()]
		self.select_header_db_label['text'] = self.db_type


	def search(self, query_index):
		self.columns = q_r[query_index]['columns']

		if self.db_type == 'Sqlite3':
			self.cur.execute(q_r[query_index]['squery'])
			self.results = self.cur.fetchall()

		elif self.db_type == 'Mongodb':
			eval(f"self.mongo{query_index + 1}()")

		else:
			if query_index == 0:
				self.results = list(map(lambda k: (k.value(), ), self.neo.query(q_r[query_index]['nquery'])))
			else:
				self.results = list(map(lambda k: (k[0], k[1]), self.neo.query(q_r[query_index]['nquery'])))
		


		print(self.results)

		# need this to delete the current table 
		for item in self.select_table_frame.grid_slaves():
			item.grid_forget()

		print(query_index)
		self.select_header_query_label['text'] = eval(f"self.query_{query_index + 1}['text']") + f" ({self.db_type})"

		for i, row in enumerate(list(self.results)):
			for j, column in enumerate(self.columns):
				if i == 0:
					# this is to set up the name of the columns
					item = Entry(master = self.select_table_frame)
					item.grid(row = i, column = j)
					item.insert(END, column)

				item = Entry(master = self.select_table_frame)
				item.grid(row = i+1, column = j)
				item.insert(END, row[j])


	def mongo1(self):
		tournament_id = self.mongo['tournament'].find_one({'date_held' : '06/03/2021'})['_id']
		person_ids = [i['person_id'] for i in self.mongo['player'].find({'tournament_id' : tournament_id})]
		persons = self.mongo['person'].find()
		def filter_persons(item):
			return item['_id'] in person_ids
		raw_results = list(filter(filter_persons, persons))
		self.results = list(map(lambda k: (k['name'], ), raw_results))

	
	def mongo2(self):
		collection = self.mongo['bet']

		item = list(collection.aggregate([{ '$match': { 'cards': '2 Aces' } }, 
								{ '$group': { '_id': "$amount", 'times': { '$sum': 1} }}, 
								{ '$sort': { 'times': -1 } }]))[0] 
	
		self.results = [(item['_id'], item['times'])]


	def mongo3(self):		
		collection = self.mongo['bet']

		bet_items = list(collection.aggregate([
				{ '$group': { '_id': "$player_id", 'sum': { '$sum': '$amount'}, 'times' : { '$sum' : 1 }}},
				{ '$lookup' : { 'from' : 'player',
								'localField' : '_id',
								'foreignField' : 'id',
								'as': 'arr'} 
								}
				]))

		filt = [{ 'sum' : i['sum'], 'times' : i['times'], 'person_id' : i['arr'][0]['person_id'] } for i in bet_items ]
		persons = {}


		for i in filt:
			if str(i['person_id']) not in persons.keys():

				persons[str(i['person_id'])] = {'sum' : i['sum'], 'times' : i['times']}

			else:
				try:
					persons[str(i['person_id'])]['sum'] += i['sum']
					persons[str(i['person_id'])]['times'] += i['times']
				
				except Exception as e:
					print(f'[--] {e}')

		for i in persons:
			persons[i] = persons[i]['sum'] / persons[i]['times']

		res = sorted(list(persons.items()), key = lambda k: k[1])[-1]

		person_item = self.mongo['person'].find_one({'id' : int(res[0])})


		self.results = [(person_item['name'], res[1])]


	def mongo4(self):

		bets = self.mongo['bet']

		highest_bet = list(bets.aggregate([{ '$sort': { 'amount': -1 }}]))[0]
		player = self.mongo['player'].find_one({ '_id' : highest_bet['player_id']})
		person = self.mongo['person'].find_one({ '_id' : player['person_id']})


		self.results = [(person['name'], highest_bet['amount'])]


	def mongo5(self):
		players = self.mongo['player']

		items = list(players.aggregate([
			{ '$lookup'	: { 'from' : 'game',
							'localField' : '_id',
							'foreignField' : 'winner',
							'as' : 'arr' }}
			]))
		persons = {}
		for i in items:
			if i['arr']:
				if str(i['person_id']) not in persons.keys():
					persons[str(i['person_id'])] = 1 
				else:
					persons[str(i['person_id'])] += 1 

		result = sorted(list(persons.items()), key = lambda k: k[1])[-1]
		person = self.mongo['person'].find_one({ '_id' : int(result[0]) })

		self.results = [(person['name'], result[1])]


if __name__ == '__main__':
	Application()