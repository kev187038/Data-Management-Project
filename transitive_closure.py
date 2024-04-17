import neo4j
from time import time
from tqdm import tqdm
# Connessione al database Neo4j
class Neo4jConnection:
    def __init__(self, uri, user, password, db_name):
        self._driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))
        self.db_name = db_name
    def close(self):
        self._driver.close()

    def runQuery(self, query, params):
        with self._driver.session(database=self.db_name) as session:
            result = session.run(query, params)
            return list(result)
        
def execute_neo4j_query(query, params):
    neo4j_conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password", "words-graph")
    result = neo4j_conn.runQuery(query, params)
    neo4j_conn.close()
    return result

def getResults(graph_query, params = None):
    # Esecuzione e confronto delle query
    neo4j_result = execute_neo4j_query(graph_query, params)
    tuples = []
    for record in neo4j_result:
        tup = []
        for item in record:
            tup.append(item['word'])
            tup.append(item['type'])
        tuples.append(tuple(tup))
    return tuples


#print(nodes)

def transitive_closure(node):
    hyponyms = getResults("MATCH (n {word: $word, type: $type})-[:IsHypernym]->(m) RETURN m;", {"word": node[0], "type": node[1]})
    
    #base case: return the dictionary of reachables  
    if node in dic.keys():
        return dic[node]
    #initialize reachable dictionary
    #print('execution')
    dic[node] = {}

    if len(hyponyms) > 0:
        for hyp in hyponyms:
            #insert hyponym in reachable dictionaries
            dic[node][hyp] =  1
            #if I already know reachables from the hyponym
            if hyp not in dic.keys():
                dic[hyp] = transitive_closure(hyp)

            for rag in dic[hyp]:
                    #check whether information about the reachable is already present
                    if rag in dic[node].keys():
                        dic[node][rag] = min(dic[hyp][rag]+1, dic[node][rag])                
                    else:
                        dic[node][rag] = dic[hyp][rag]+1
    #print(node, dic[node])
    return dic[node]


dic = {}
base_hypons = getResults("MATCH (n) WHERE NOT (n)-[:IsHypernym]->() RETURN DISTINCT n;")
hypernyms = getResults("MATCH (n)-[:IsHypernym]->(m) WITH n, COUNT(m) as num_hyp ORDER BY num_hyp RETURN n;")
t1 = time()

for hypon in tqdm(base_hypons):
    dic[hypon] = {}

for hyper in tqdm(hypernyms):
    if hyper not in dic.keys():
        dic[hyper] = transitive_closure(hyper)

t2 = time()
print(dic)
print(t2 - t1)