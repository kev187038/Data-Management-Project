import csv
import psycopg2
import re

conn = psycopg2.connect(dbname="words_relational", user="postgres", password="password", 
		host="localhost", port="5432")


cursor = conn.cursor()
#ci sono dei problemi
cursor.execute('''CREATE TABLE IF NOT EXISTS WORDS (
			word varchar,
			type varchar(50),
			sentiment float,
			primary key (word, type)
			)''')
			
cursor.execute('''CREATE TABLE IF NOT EXISTS IsSynonym (
			word varchar,
			w_type varchar(50),
			synonym varchar,
			s_type varchar(50),
			foreign key (word, w_type) references WORDS(word, type),
			foreign key (synonym. s_type) references WORDS(word, type)
			)
			''')
cursor.execute('''CREATE TABLE IF NOT EXISTS IsAntonym (
			word varchar references WORDS(word),
			w_type varchar(50),
			antonym varchar references WORDS(word)
			a_type varchar(50),
			foreign key (word, w_type) references WORDS(word, type),
			foreign key (antonym. a_type) references WORDS(word, type)
			)
			''')

cursor.execute('''CREATE TABLE IF NOT EXISTS IsHypernym (
			hyponym varchar references WORDS(word),
			hypon_type varchar(50),
			hypernym varchar references WORDS(word)
			hyper_type varchar(50),
			foreign key (hyponym, hypon_type) references WORDS(word, type),
			foreign key (hypernym. hyper_type) references WORDS(word, type)
			)
			''')
			
def insert_into_entity_table(f_path):
	with open(f_path, 'r') as f:
		#handle csv
		row = f.readline().strip()
		if(".csv" in f_path):
			#If there are any words left from the CSVs we insert them into the db withouth the sentiment (No such words were left)
			while(row):
				row = f.readline().strip().split(',')
				word = row[0]
				cursor.execute("SELECT * from words where word = %s", (word,))
				if (not cursor.fetchone()) and ' ' not in word:
					type_ = row[1]
					cursor.execute("INSERT INTO WORDS (word, type) values (%s, %s)", (word, type_))
					print("Inserted (%s, %s)", (word, type_))
				
				row = f.readline().strip()
				if not row:
					break
				row = re.split("#|\t", row)
				
			conn.commit()
		
		elif(".txt" in f_path):
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
				cursor.execute("SELECT * from words where word = %s and w_type = %s", (word, type_))
				
				if not cursor.fetchone():
					
					if(type_ == 'a'):
						type_ = 'adjective'
					elif(type_ == 'n'):
						type_ = 'noun'
					elif(type_ == 'v'):
						type_ = 'verb'
					elif(type_ == 'r'):
						type_ = 'adverb'
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
			if('synonyms' in f_path):
				row = re.split(",|;|\|", row)
				word = row[0]
				synonyms = row[2:]
				
				for syn in synonyms:
					#We make sure the tuple has not been inserted already
					cursor.execute("SELECT * FROM IsSynonym WHERE word = %s and synonym = %s", (word, w_type, syn))
					if not cursor.fetchone():
						#We make sure each of the two words (word and synonym) are present in the word table
						cursor.execute("SELECT * FROM words where word = %s", (word,))
						if cursor.fetchone():
							cursor.execute("SELECT * from words where word = %s", (syn,))
							if cursor.fetchone():
								cursor.execute("INSERT INTO IsSynonym (word, synonym) values (%s, %s)", (word, syn))
								print("INSERTED (word, synonym): ",(word, syn))
								
			if('antonyms' in f_path):
				row = re.split(",|;|\|", row)
				word = row[0]
				antonyms = row[2:]
				
				for an in antonyms:
					#We make sure the tuple has not been inserted already
					cursor.execute("SELECT * FROM Isantonym WHERE word = %s and antonym = %s", (word, an))
					if not cursor.fetchone():
						#We make sure each of the two words (word and synonym) are present in the word table
						cursor.execute("SELECT * FROM words where word = %s", (word,))
						if cursor.fetchone():
							cursor.execute("SELECT * from words where word = %s", (an,))
							if cursor.fetchone():
								cursor.execute("INSERT INTO IsAntonym (word, antonym) values (%s, %s)", (word, an))
								print("INSERTED (word, antonym): ",(word, an))
			if('hypernyms' in f_path):
				row = re.split(",|;|\|", row)
				hyponym = row[0]
				hypernyms = row[2:]
				
				for hyper in hypernyms:
					#We make sure the tuple has not been inserted already
					cursor.execute("SELECT * FROM IsHypernym WHERE hyponym = %s and hypernym = %s", (hyponym, hyper))
					if not cursor.fetchone():
						#We make sure each of the two words (hyponym and hypernym) are present in the word table
						cursor.execute("SELECT * FROM words where word = %s", (hyponym,))
						if cursor.fetchone():
							cursor.execute("SELECT * from words where word = %s", (hyper,))
							if cursor.fetchone():
								cursor.execute("INSERT INTO IsHypernym (hyponym, hypernym) values (%s, %s)", (hyponym, hyper))
								print("INSERTED (hyponym, hypernym): ",(hyponym, hyper))
			
			if('hyponyms' in f_path):
				row = re.split(",|;|\|", row)
				hypernym = row[0]
				hyponyms = row[2:]
				
				for hypon in hyponyms:
					#We make sure the tuple has not been inserted already
					cursor.execute("SELECT * FROM IsHypernym WHERE hyponym = %s and hypernym = %s", (hypon, hypernym))
					if not cursor.fetchone():
						#We make sure each of the two words are present in the word table
						cursor.execute("SELECT * FROM words where word = %s", (hypon,))
						if cursor.fetchone():
							cursor.execute("SELECT * from words where word = %s", (hypernym,))
							if cursor.fetchone():
								cursor.execute("INSERT INTO IsHypernym (hyponym, hypernym) values (%s, %s)", (hypon, hypernym))
								print("INSERTED (hyponym, hypernym): ",(hypon, hypernym))
					
				
			row = f.readline().strip()
			if not row:
				break
		conn.commit()
		

#insert_into_relation_table("./synonyms.csv")

#insert_into_relation_table("./antonyms.csv")
#insert_into_relation_table("./hypernyms.csv")
insert_into_relation_table("./hyponyms.csv")
		
