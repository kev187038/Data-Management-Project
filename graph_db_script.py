import csv
import re

from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(user, password))
db_name = "words-graph"

def insert_into_entity_table(f_path):
    with open(f_path, 'r') as f, driver.session(database=db_name) as session:
        #handle csv
        row = f.readline().strip()
        if(".csv" in f_path):
            #If there are any words left from the CSVs we insert them into the db withouth the sentiment (No such words were left)
            while(row):
                row = f.readline().strip().split(',')
                word = row[0]
                type_ = row[1]
                #check that there is no node containing same word and type
                result = list(session.run("MATCH (w:Word {word: $word, type: $type}) RETURN w", {"word": word, "type": type_}))

                if len(result) == 0:
                    session.run("CREATE (w:Word  {word: $word, type: $type})", {"word": word, "type": type_})
                    print("Inserted (%s, %s)", (word, type_))
                
                row = f.readline().strip()
                if not row:
                    break
                row = re.split("#|\t", row)
                
        
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
                if(type_ == 'a'):
                    type_ = 'adjective'
                elif(type_ == 'n'):
                    type_ = 'noun'
                elif(type_ == 'v'):
                    type_ = 'verb'
                elif(type_ == 'r'):
                    type_ = 'adverb'
                #check that there is no node containing same word and type
                result = list(session.run("MATCH (w:Word {word: $word, type: $type}) RETURN w", {"word": word, "type": type_}))

                if len(result) == 0:
                    sentiment = float(row[2])
                    session.run("CREATE (w:Word  {word: $word, type: $type, sentiment: $sentiment})", {"word": word, "type": type_, "sentiment": sentiment})
                    print("Inserted (%s, %s, %s)", (word, type_, sentiment))
                
                row = f.readline().strip()
                if not row:
                    break
                row = re.split("#|\t", row)


insert_into_entity_table('./SentiWords_1.0.txt')

def insert_into_relation_table(f_path):
    with open(f_path, 'r') as f, driver.session(database=db_name) as session:
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
                    result = list(session.run("MATCH (w:Word {word: $word}) -[:IsSynonym]-> (s:Word {word: $synonym}) RETURN w, s", {"word": word, "synonym": syn}))
                    if len(result) == 0:
                        #We make sure each of the two words (word and synonym) are present in the word table
                        result = list((session.run("MATCH (w:Word {word: $word}) RETURN w", word=word)))
                        if len(result) > 0:
                            result = list((session.run("MATCH (s:Word {word: $synonym}) RETURN s", synonym=syn)))
                            if len(result) > 0:
                                session.run("MATCH (w:Word {word: $word}), (s:Word {word: $synonym}) CREATE (w)-[:IsSynonym]->(s)", {"word": word, "synonym": syn})
                                session.run("MATCH (w:Word {word: $word}), (s:Word {word: $synonym}) CREATE (s)-[:IsSynonym]->(w)", {"word": word, "synonym": syn})
                                print("INSERTED (word, synonym): ",(word, syn))
                                
            if('antonyms' in f_path):
                row = re.split(",|;|\|", row)
                word = row[0]
                antonyms = row[2:]
                
                for an in antonyms:
                    #We make sure the tuple has not been inserted already
                    result = list(session.run("MATCH (w:Word {word: $word}) -[:IsAntonym]-> (s:Word {word: $antonym}) RETURN w, s", {"word": word, "antonym": an}))
                    if len(result) == 0:
                        #We make sure each of the two words (word and antonym) are present in the word table
                        result = list((session.run("MATCH (w:Word {word: $word}) RETURN w", word=word)))
                        if len(result) > 0:
                            result = list((session.run("MATCH (s:Word {word: $antonym}) RETURN s", antonym=an)))
                            if len(result) > 0:
                                session.run("MATCH (w:Word {word: $word}), (s:Word {word: $antonym}) CREATE (w)-[:IsAntonym]->(s)", {"word": word, "antonym": an})
                                session.run("MATCH (w:Word {word: $word}), (s:Word {word: $antonym}) CREATE (s)-[:IsAntonym]->(w)", {"word": word, "antonym": an})
                                print("INSERTED (word, antonym): ",(word, an))
            if('hypernyms' in f_path):
                row = re.split(",|;|\|", row)
                hyponym = row[0]
                hypernyms = row[2:]


                for hyper in hypernyms:
                    #We make sure the tuple has not been inserted already
                    result = list(session.run("MATCH (hypo:Word {word: $hyponym}) -[:IsHypernym]-> (hyper:Word {word: $hypernym}) RETURN hypo, hyper", {"hyponym": hyponym, "hypernym": hyper}))
                    if len(result) == 0:
                        #We make sure each of the two words (hyponym and hypernym) are present in the word table
                        result = list((session.run("MATCH (w:Word {word: $word}) RETURN w", word=hyponym)))
                        if len(result) > 0:
                            result = list((session.run("MATCH (w:Word {word: $word}) RETURN w", word=hyper)))
                            if len(result) > 0:
                                session.run("MATCH (hypo:Word {word: $hyponym}), (hyper:Word {word: $hypernym}) CREATE (hyper)-[:IsHypernym]->(hypo)", {"hyponym": hyponym, "hypernym": hyper})
                                print("INSERTED (hyponym, hypernym): ",(hyponym, hyper))
            
            if('hyponyms' in f_path):
                row = re.split(",|;|\|", row)
                hypernym = row[0]
                hyponyms = row[2:]
                
                for hypon in hyponyms:
                    #We make sure the tuple has not been inserted already
                    result = list(session.run("MATCH (hypo:Word {word: $hyponym}) -[:IsHypernym]-> (hyper:Word {word: $hypernym}) RETURN hypo, hyper", {"hyponym": hypon, "hypernym": hypernym}))
                    if len(result) == 0:
                        #We make sure each of the two words are present in the word table
                        result = list((session.run("MATCH (w:Word {word: $word}) RETURN w", word=hypon)))
                        if len(result) > 0:
                            result = list((session.run("MATCH (w:Word {word: $word}) RETURN w", word=hypernym)))
                            if len(result) > 0:
                                session.run("MATCH (hypo:Word {word: $hyponym}), (hyper:Word {word: $hypernym}) CREATE (hyper)-[:IsHypernym]->(hypo)", {"hyponym": hypon, "hypernym": hypernym})
                                print("INSERTED (hyponym, hypernym): ",(hypon, hypernym))
                    
                
            row = f.readline().strip()
            if not row:
                break
        
insert_into_relation_table("./synonyms.csv")
insert_into_relation_table("./antonyms.csv")
insert_into_relation_table("./hypernyms.csv")
insert_into_relation_table("./hyponyms.csv")        
driver.close()
