import psycopg2
import neo4j
import re
import threading
from time import time
# Connessione al database PostgreSQL
def connectToPostgresql():
    conn = psycopg2.connect(
        dbname="words-relational", user="postgres", password="password", 
		host="localhost", port="5432"
    )
    return conn

# Esecuzione della query in PostgreSQL ed estrazione dei tempi medi di querying
def executePostgresqlQueryNTimes(query, N):
   
    result = 0
    for i in range(N):
    	conn = connectToPostgresql()
    	cur = conn.cursor()
    	t1 = time()
    	cur.execute(query)
    	t2 = time()
    	t = t2 - t1
    	result += t
    	cur.close()  ##Close connection to avoid caching (?)
    	conn.close() ##
    
    result = result/N
    
    return result

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
def execute_neo4j_queryNTimes(query, N):
    result = 0
    for i in range(N):
    	#Each time we open and close connection to try avoid caching
    	neo4j_conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password", "words-graph")
    	time = neo4j_conn.runQuery(query)
    	neo4j_conn.close()
    	result += time
    result = result/N
    return result
    
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
    
    
def getTimes(graph_query, relational_query, task, N):
    #Thread separati per neo4j e pgadmin4 
    neo = threading.Thread(target=execute_neo4j_queryNTimes, args=(graph_query, N))
    pgsql = threading.Thread(target=executePostgresqlQueryNTimes, args=(relational_query, N))
    neo.start()
    pgsql.start()
    neo.join()
    pgsql.join()
    # Esecuzione e confronto dei tempi medi delle query
    avg_graph_time = neo.result
    avg_relational_time = pgsql.result
    
    if avg_graph_time < avg_relational_time:
    	percentage = (avg_relational_time*100) / avg_graph_time
    	print("Graph was faster than relational db.\n Graph avg time: " + str(avg_graph_time) + "\n Relational avg time: " + str(avg_relational_time))
    	print("Graph on average employed " + str(percentage) + "% of the time of the relational db\n\n")
    else:
    	percentage = (avg_graph_time*100) / avg_relational_time
    	print("Relational db was faster than graph db.\n Graph avg time: " + str(avg_graph_time) + "\n Relational avg time: " + str(avg_relational_time))
    	print("Relational db on average employed " + str(percentage) + "% of the time of the graph db\n\n")
    #Automated result registration
    with open('./query_times.txt', 'w') as file:
    	file.write(f"Task {task}\nAvg graph time: {avg_graph_time}\nAvg relational time: {avg_relational_time}")
  
    
# Lettura delle query dal file
relational_queries = readQueriesFromFile("relational_queries.txt")
graph_queries = readQueriesFromFile("graph_queries.txt")
N = 30 #number of times we repeat the execution of the query

for i in range(min(len(relational_queries), len(graph_queries))):
    task = i+1
    print('Dealing with Task', task)
    getTimes(graph_queries[i], relational_queries[i], task, N) 
    
