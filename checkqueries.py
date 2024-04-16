import psycopg2
import neo4j
import re

# Connessione al database PostgreSQL
def connectToPostgresql():
    conn = psycopg2.connect(
        dbname="words-relational", user="postgres", password="password", 
		host="localhost", port="5432"
    )
    return conn

# Esecuzione della query in PostgreSQL
def executePostgresqlQuery(query):
    conn = connectToPostgresql()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# Connessione al database Neo4j
class Neo4jConnection:
    def __init__(self, uri, user, password, db_name):
        self._driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))
        self.db_name = db_name
    def close(self):
        self._driver.close()

    def runQuery(self, query):
        with self._driver.session(database=self.db_name) as session:
            result = session.run(query)
            return list(result)

# Esecuzione della query in Neo4j
def execute_neo4j_query(query):
    neo4j_conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password", "words-graph")
    result = neo4j_conn.runQuery(query)
    neo4j_conn.close()
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



def getResults(relational_query, graph_query, task):
    # Esecuzione e confronto delle query
    neo4j_result = execute_neo4j_query(graph_queries[i])
    postgresql_result = executePostgresqlQuery(relational_queries[i])
    tuples = []
    for record in neo4j_result:
        tup = []
        for item in record:
            if isinstance(item, neo4j.graph.Node):
                tup.append(item['word'])
                tup.append(item['type'])
                if task == 1:
                    tup.append(item['sentiment'])
            elif isinstance(item, (int, float)):
                tup.append(item)

        tuples.append(tuple(tup))

    return sorted(postgresql_result), sorted(tuples)


def compareResults(rel, gr):
    if (len(rel) != len(gr)):
        return False
    for i in range(len(rel)):
        tup_rel = rel[i]
        tup_gr = gr[i]
        if len(tup_rel) != len(tup_gr):
            print('Tuple di diversa lunghezza, ricontrolla un po\' la logica')
            return False

        for j in range(len(tup_rel)):
            if isinstance(tup_rel[j], str):
                if tup_rel[j] != tup_gr [j]:
                    return False
            elif isinstance(tup_rel[j], (int,float)):
                if abs(tup_rel[j] - tup_gr[j]) > 1e-5:
                    return False

    return True

# Lettura delle query dal file
relational_queries = readQueriesFromFile("relational_queries.txt")
graph_queries = readQueriesFromFile("graph_queries.txt")

for i in range(min(len(relational_queries), len(graph_queries))):
    task = i+1
    print('Dealing with Task', task)
    relational_results, graph_results = getResults(graph_queries[i], relational_queries[i], task) 
    if compareResults(relational_results, graph_results):
        print('Success! The results of the queries are the same!')
    else:
        print('Something went wrong in Task', task)