import csv
import psycopg2
import re

conn = psycopg2.connect(dbname="words-relational", user="postgres", password="password", 
		host="localhost", port="5432")


cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS WORDS (
			word varchar,
			type varchar(50),
			sentiment float,
			primary key(word, type))''')
			
cursor.execute('''CREATE TABLE IF NOT EXISTS IsSynonym (
			word varchar,
			type_word varchar(50),
			synonym varchar,
			type_synonym varchar(50),
			foreign key (word, type_word) references WORDS (word,type),
			foreign key (synonym, type_synonym) references WORDS(word,type)
			)
			''')
cursor.execute('''CREATE TABLE IF NOT EXISTS IsAntonym (
			word varchar,
			type_word varchar(50),
			antonym varchar,
			type_antonym varchar(50),
			foreign key (word, type_word) references WORDS (word,type),
			foreign key (antonym, type_antonym) references WORDS(word,type)
			)
			''')

cursor.execute('''CREATE TABLE IF NOT EXISTS IsHypernym (
			hyponym varchar,
			type_hyponym varchar(50),
			hypernym varchar,
			type_hypernym varchar(50),
			foreign key (hyponym, type_hyponym) references WORDS (word,type),
			foreign key (hypernym, type_hypernym) references WORDS(word,type)			
			)
			''')
			
def insert_into_entity_table(f_path):
	with open(f_path, 'r') as f:
		#handle csv
		row = f.readline().strip()
		
		#In the txt file we find all the words to populate the WORDS table with
		row = f.readline().strip()
		#we skip first lines
		while(".22_caliber#a	0" not in row):
			row = f.readline()
		row = row.strip()
		row = re.split("#|\t",row)
		
		while(row):
			word = row[0]
			type_ = row[1]
			if(type_ == 'a'):
				type_ = 'adjective'
			elif(type_ == 'n'):
				type_ = 'noun'
			elif(type_ == 'v'):
				type_ = 'verb'
			elif(type_ == 'r'):
				type_ = 'adverb'
			cursor.execute("SELECT * from words where word = %s and type = %s", (word, type_))
			
			if not cursor.fetchone():
				
				sentiment = float(row[2])
				
				cursor.execute("INSERT INTO WORDS (word, type, sentiment) values (%s, %s, %s)", (word, type_, sentiment))
				print("Inserted (%s, %s, %s)", (word, type_, sentiment))
			
			row = f.readline().strip()
			if not row:
				break
			row = re.split("#|\t", row)
			
		conn.commit()

#insert_into_entity_table('./SentiWords_1.0.txt')

def insert_into_relation_table(f_path):
	with open(f_path, 'r') as f:
		#Skip first line
		row = f.readline().strip()
		row = f.readline().strip()
		while(row):
			#Handle different tables
			row = re.split(",|;|\|", row)
			type = row[1]

			if('synonyms' in f_path):
				word = row[0].lower().replace(' ', '_')
				#check if the word is in the db, eventually take the types
				cursor.execute("SELECT type FROM words where word = %s", (word,))
				types1 = cursor.fetchall()

				for i in range(len(types1)):
					types1[i] = types1[i][0]
				types1.sort()
				
				#if the word in the db
				if len(types1) > 0:
					synonyms = row[2:]

					#iterate on synonyms
					for syn in synonyms:
						syn = syn.lower().replace(' ', '_')

						#avoid duplicates
						if word!= syn:
							#We make sure each of the synonym is present in the word table
							cursor.execute("SELECT type from words where word = %s", (syn,))
							types2 = cursor.fetchall()
							

							#check the synonym is in the db
							if len(types2) > 0:
								for i in range(len(types2)):
									types2[i] = types2[i][0]
								types2.sort()


								#choose the type: privilege intersection
								inters = list(set(types1) & set(types2))
								if len(inters) > 0:
									if type in inters:
										type_1 = type
										type_2 = type_1
									else:
										inters.sort()
										type_1 = inters[0]
										type_2 = type_1
								else:
									#if no intersection, try using the given type!
									type_1 = type if type in types1 else types1[0]
									type_2 = type if type in types2 else types2[0]

								#We make sure the tuple has not been inserted already
								cursor.execute("SELECT * FROM IsSynonym WHERE word = %s and synonym = %s and type_word = %s and type_synonym = %s", (word, syn, type_1, type_2))

								if not cursor.fetchone():
									#insert the tuple
									#cursor.execute("INSERT INTO IsSynonym (word, synonym, type_word, type_synonym) values (%s, %s, %s, %s)", (word, syn, type_1, type_2))
									print("INSERTED (word, type_word, synonym, type_synonym): ",(word, type_1, syn, type_2))
								
			if('antonyms' in f_path):
				word = row[0].lower().replace(' ', '_')
				#We make sure the tuple has not been inserted already
				cursor.execute("SELECT type FROM words where word = %s", (word,))
				types1 = cursor.fetchall()
				
				for i in range(len(types1)):
					types1[i] = types1[i][0]
				types1.sort()
				
				if len(types1) > 0:
					antonyms = row[2:]

					for an in antonyms:
						an = an.lower().replace(' ', '_')

						if word!= an:
							cursor.execute("SELECT type from words where word = %s", (an,))
							types2 = cursor.fetchall()

							if len(types2) > 0:
								for i in range(len(types2)):
									types2[i] = types2[i][0]
								types2.sort()


								#choose the type: privilege intersection
								inters = list(set(types1) & set(types2))
								if len(inters) > 0:
									if type in inters:
										type_1 = type
										type_2 = type_1
									else:
										inters.sort()
										type_1 = inters[0]
										type_2 = type_1
								else:
									#if no intersection, try using the given type!
									type_1 = type if type in types1 else types1[0]
									type_2 = type if type in types2 else types2[0]

								#We make sure the tuple has not been inserted already
								cursor.execute("SELECT * FROM IsAntonym WHERE word = %s and antonym = %s and type_word = %s and type_antonym = %s", (word, an, type_1, type_2))

								if not cursor.fetchone():
									cursor.execute("INSERT INTO IsAntonym (word, antonym, type_word, type_antonym) values (%s, %s, %s, %s)", (word, an, type_1, type_2))
									print("INSERTED (word, type_word, antonym, type_antonym): ",(word, type_1, an, type_2))
			
			if('hypernyms' in f_path):
				hyponym = row[0].lower().replace(' ', '_')
				cursor.execute("SELECT type FROM words where word = %s", (hyponym,))
				types1 = cursor.fetchall()

				for i in range(len(types1)):
					types1[i] = types1[i][0]
				types1.sort()
				

				if len(types1) > 0:
					hypernyms = row[2:]
					
					for hyper in hypernyms:
						hyper = hyper.lower().replace(' ', '_')
	
						if hyponym != hyper:
							cursor.execute("SELECT type from words where word = %s", (hyper,))
							types2 = cursor.fetchall()
	
							if len(types2) > 0:
								for i in range(len(types2)):
									types2[i] = types2[i][0]
								types2.sort()


								#choose the type: privilege intersection
								inters = list(set(types1) & set(types2))
								if len(inters) > 0:
									if type in inters:
										type_1 = type
										type_2 = type_1
									else:
										inters.sort()
										type_1 = inters[0]
										type_2 = type_1
								else:
									#if no intersection, try using the given type!
									type_1 = type if type in types1 else types1[0]
									type_2 = type if type in types2 else types2[0]
								
								#We make sure the tuple has not been inserted already
								cursor.execute("SELECT * FROM IsHypernym WHERE hyponym = %s and hypernym = %s and type_hyponym = %s and type_hypernym = %s", (hyponym, hyper, type_1, type_2))
								
								if not cursor.fetchone():
									cursor.execute("INSERT INTO IsHypernym (hyponym, hypernym, type_hyponym, type_hypernym) values (%s, %s, %s, %s)", (hyponym, hyper, type_1, type_2))
									print("INSERTED (hyponym, type_hyponym, hypernym, type_hypernym): ", (hyponym, type_1, hyper, type_2))
								
			if('hyponyms' in f_path):
				hypernym = row[0].lower().replace(' ', '_')
				cursor.execute("SELECT type FROM words where word = %s", (hypernym,))
				types1 = cursor.fetchall()

				for i in range(len(types1)):
					types1[i] = types1[i][0]
				types1.sort()
				
				if len(types1) > 0:
					hyponyms = row[2:]

					for hypon in hyponyms:
						hypon = hypon.lower().replace(' ', '_')
						if hypon != hypernym:
							cursor.execute("SELECT type from words where word = %s", (hypon,))
							types2 = cursor.fetchall()

							if len(types2) > 0:
								for i in range(len(types2)):
									types2[i] = types2[i][0]
								types2.sort()


								#choose the type: privilege intersection
								inters = list(set(types1) & set(types2))
								if len(inters) > 0:
									if type in inters:
										type_1 = type
										type_2 = type_1
									else:
										inters.sort()
										type_1 = inters[0]
										type_2 = type_1
								else:
									#if no intersection, try using the given type!
									type_1 = type if type in types1 else types1[0]
									type_2 = type if type in types2 else types2[0]

								#We make sure the tuple has not been inserted already
								cursor.execute("SELECT * FROM IsHypernym WHERE hyponym = %s and hypernym = %s and type_hyponym = %s and type_hypernym = %s", (hypon, hypernym, type_2, type_1))
								if not cursor.fetchone():
									cursor.execute("INSERT INTO IsHypernym (hyponym, hypernym, type_hyponym, type_hypernym) values (%s, %s, %s, %s)", (hypon, hypernym, type_2, type_1))
									print("INSERTED (hyponym, type_hyponym, hypernym, type_hypernym): ", (hypon, type_2, hypernym, type_1))
				
			row = f.readline().strip()
			if not row:
				break
		conn.commit()
		
insert_into_relation_table("./synonyms.csv")
#insert_into_relation_table("./antonyms.csv")
#insert_into_relation_table("./hypernyms.csv")
#insert_into_relation_table("./hyponyms.csv")
