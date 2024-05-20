import csv
import psycopg2
import re
from neo4j import GraphDatabase
#Open Relational DB
conn = psycopg2.connect(dbname="words-relational", user="postgres", password="password", 
		host="localhost", port="5432")


cursor = conn.cursor()
#Open Graph DB
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(user, password))
db_name = "words-graph"

def copy_words():
	cursor.execute("SELECT * FROM WORDS")
	words = cursor.fetchall()
	
	for tup in words:
		word = tup[0]
		type_ = tup[1]
		sentiment = tup[2]
		
		with driver.session(database=db_name) as session:
			print("Inserted ($word, $type, $sentiment)", {"word": word, "type": type_, "sentiment": sentiment})
			session.run("CREATE (w:Word  {word: $word, type: $type, sentiment: $sentiment})", {"word": word, "type": type_, "sentiment": sentiment})

copy_words()	

def copy_relationships():
	#SYNONYMS
	cursor.execute("SELECT * FROM IsSynonym where word < synonym") #######AGGIUNTA WHERE CLAUSE
	synonyms = cursor.fetchall()
	
	for tup in synonyms:
		word = tup[0]
		word_type = tup[1]
		syn = tup[2]
		syn_type = tup[3]
		
		with driver.session(database=db_name) as session:
			print("Inserted ($word, $type, $synonym, $sin_type)", {"word": word, "type": word_type, "synonym": syn, "syn_type": syn_type})
			session.run("MATCH (w:Word {word: $word, type: $word_type}), (s:Word {word: $synonym, type: $syn_type}) MERGE (w)-[:IsSynonym]->(s) MERGE (s)-[:IsSynonym]->(w)", {"word": word, "word_type": word_type, "synonym": syn, "syn_type": syn_type})
			
	#ANTONYMS	
	cursor.execute("SELECT * FROM IsAntonym where word < antonym") #######AGGIUNTA WHERE CLAUSE
	antonyms = cursor.fetchall()
	print("Doing antonyms")
	for tup in antonyms:
		word = tup[0]
		word_type = tup[1]
		ant = tup[2]
		ant_type = tup[3]
		
		with driver.session(database=db_name) as session:
			print("Inserted ($word, $type, $antonym, $ant_type)",  {"word": word, "word_type": word_type, "ant": ant, "ant_type": ant_type}) 
			session.run("MATCH (w:Word {word: $word, type: $word_type}), (s:Word {word: $ant, type: $ant_type}) MERGE (w)-[:IsAntonym]->(s) MERGE (s)-[:IsAntonym]->(w)", {"word": word, "word_type": word_type, "ant": ant, "ant_type": ant_type})
			
	#HYPERNYMS		
	cursor.execute("SELECT * FROM IsHypernym")
	hypernyms = cursor.fetchall()
	print("Doing hypernyms")
	for tup in hypernyms:
		hyponym = tup[0]
		hyponym_type = tup[1]
		hypernym = tup[2]
		hypernym_type = tup[3]
		
		with driver.session(database=db_name) as session:
			print("Inserted ($hyponym, $hyponym_type, $hypernym, $hypernym_type)",  {"hyponym": hyponym, "hyponym_type": hyponym_type, "hypernym": hypernym, "hypernym_type": hypernym_type}) 
			session.run("MATCH (w:Word {word: $hyponym, type: $hyponym_type}), (s:Word {word: $hypernym, type: $hypernym_type}) MERGE (s)-[:IsHypernym]->(w)", {"hyponym": hyponym, "hyponym_type": hyponym_type, "hypernym": hypernym, "hypernym_type": hypernym_type})

copy_relationships()
	
