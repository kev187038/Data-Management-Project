import psycopg2
import neo4j
import re
import threading
from time import time
import sys
# Connessione al database PostgreSQL
def connectToPostgresql(dbname):
    conn = psycopg2.connect(
        dbname=dbname, user="postgres", password="password", 
		host="localhost", port="5432"
    )
    return conn

# Esecuzione della query in PostgreSQL ed estrazione dei tempi medi di querying
def executePostgresqlQueryNTimes(query, N, dbname):
    try:
        result = 0
        for i in range(N):
            conn = connectToPostgresql(dbname)
            cur = conn.cursor()
            t1 = time()
            print(f"PG ADMIN DOING QUERY {i+1}")
            cur.execute(query)	
            #print(f"PG ADMIN DONE QUERY {i+1}")
            t2 = time()
            t = t2 - t1
            result += t
            cur.close()  ##Close connection to avoid caching (?)
            conn.close() ##

        return result/N
	    
    except Exception as e:
        print(e)
        sys.exit(1)

# Connessione al database Neo4j
class Neo4jConnection:
    def __init__(self, uri, user, password, db_name):
        self._driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))
        self.db_name = db_name
    def close(self):
        self._driver.close()

    def runQuery(self, query):  #modified to return execution time
        with self._driver.session(database=self.db_name) as session:
            t1 = time()
            result = session.run(query)
            t2 = time()
            t = t2 - t1
            return t

# Esecuzione della query in Neo4j
def execute_neo4j_queryNTimes(query, N, dbname):
    try:
        result = 0
        for i in range(N):
            #Each time we open and close connection to try avoid caching
            neo4j_conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password", dbname)
            print(f"NEO DOING QUERY {i+1}")
            time = neo4j_conn.runQuery(query)
            #print(f"NEO DONE QUERY {i+1}")
            neo4j_conn.close()
            result += time
        return result/N
	    
    except Exception as e:
        print(e)
        sys.exit(1)
    
# Funzione per leggere le query da un file
def readQueriesFromFile(file_path):
    queries = []
    with open(file_path, "r") as file:
        query = ""
        for line in file:
            if not line.startswith('#'):
                line = re.sub(r'\s+', ' ',line.replace('\n', ' ').replace('\t', ' '))
                query += line

                if line.strip().endswith(';'):
                    queries.append(query)
                    query = ""

    return queries
    
    
def getTimes(graph_query, relational_query, graph_query_norm, relational_query_norm, task, N):
    #Thread separati per neo4j e pgadmin4 
    avg_graph_time = execute_neo4j_queryNTimes(graph_query, N, "words-graph")
    avg_relational_time = executePostgresqlQueryNTimes(relational_query, N, "words-relational")
    avg_norm_graph_time = execute_neo4j_queryNTimes(graph_query_norm, N, "wg-half-relations")
    avg_norm_relational_time = executePostgresqlQueryNTimes(relational_query_norm, N, "wr-half-relations")
        
    if avg_graph_time < avg_relational_time:
        percentage = (avg_graph_time / avg_relational_time)*100
        print("Graph was faster than relational db.\n Graph avg time: " + str(avg_graph_time) + " seconds" + "\n Relational avg time: " + str(avg_relational_time) + " seconds")
        print("Graph on average employed " + str(percentage) + "% of the time of the relational db\n\n")
    else:
        percentage = (avg_relational_time / avg_graph_time)*100
        print("Relational db was faster than graph db.\n Graph avg time: " + str(avg_graph_time) + " seconds" + "\n Relational avg time: " + str(avg_relational_time) + " seconds")
        print("Relational db on average employed " + str(percentage) + "% of the time of the graph db\n\n")
    #Automated result registration
    with open('./query_times_hashidx.txt', 'a') as file:
    	file.write(f"Task {task}\nAvg graph time: {avg_graph_time}\nAvg relational time: {avg_relational_time}\nAvg normalized graph time: {avg_norm_graph_time}\nAvg normalized relational time: {avg_norm_relational_time}\n\n")
  
    
# Lettura delle query dal file
relational_queries = readQueriesFromFile("relational_queries.txt")
graph_queries = readQueriesFromFile("graph_queries.txt")
relational_queries_norm = readQueriesFromFile("normalized_relational_queries.txt")
graph_queries_norm = readQueriesFromFile("normalized_graph_queries.txt")

#N should normally be 100
#Repetitions vary for each task (we cannot repeat a 1 hour task 100 times!)
    # T1  T2 T3   T4  T5   T6   T7  T8  T9  T10 
N = [100, 5, 100, 70, 100, 100, 50, 100, 50, 100] #number of times we repeat the execution of the query

for i in range(min(len(relational_queries), len(graph_queries))):
    task = i+1
    print('Dealing with Task', task)
    getTimes(graph_queries[i], relational_queries[i], graph_queries_norm[i], relational_queries_norm[i], task, N[i]) 
    