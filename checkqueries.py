import psycopg2  # Import PostgreSQL driver
import neo4j  # Import Neo4j driver
import re  # Import regular expression module

# Function to connect to PostgreSQL database
def connectToPostgresql():
    conn = psycopg2.connect(
        dbname="words-relational", user="postgres", password="password", 
        host="localhost", port="5432"
    )
    return conn

# Function to execute PostgreSQL query
def executePostgresqlQuery(query):
    conn = connectToPostgresql()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# Neo4j Connection class
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

# Function to execute Neo4j query
def execute_neo4j_query(query):
    neo4j_conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password", "words-graph")
    result = neo4j_conn.runQuery(query)
    neo4j_conn.close()
    return result

# Function to read queries from a file
def readQueriesFromFile(file_path):
    queries = []
    with open(file_path, "r") as file:
        query = ""
        for line in file:
            if not line.startswith('#'):  # Ignore commented lines
                line = re.sub(r'\s+', ' ', line.replace('\n', ' ').replace('\t', ' '))  # Remove extra whitespace
                query += line

                if line.strip().endswith(';'):
                    queries.append(query)
                    query = ""

    return queries

# Function to compare results obtained from PostgreSQL and Neo4j
def compareResults(rel, gr):
    # Check if the number of records returned by both databases is the same
    if len(rel) != len(gr):
        print('The number of records returned is different')
        return False
    
    # Iterate through each tuple of results
    for i in range(len(rel)):
        tup_rel = rel[i]  # Tuple of results from PostgreSQL
        tup_gr = gr[i]    # Tuple of results from Neo4j
        
        # Check if the length of tuples is different
        if len(tup_rel) != len(tup_gr):
            print('Tuple of different length, check the logic')
            return False

        # Iterate through each element in the tuples
        for j in range(len(tup_rel)):
            # Check if the element is a string
            if isinstance(tup_rel[j], str):
                # If it's a string, compare the strings
                if tup_rel[j] != tup_gr[j]:
                    # If the strings are different, handle special case for Task 9
                    if task == 9:
                        # Split strings into lists of words, remove whitespace, and sort them
                        tuprel = sorted([word.strip() for word in tup_rel[j].split(',')])
                        tupgr = sorted([word.strip() for word in tup_gr[j].split(',')])

                        # Check if the sorted lists are different
                        if len(tuprel) != len(tupgr):
                            print(tupgr, tuprel)  # Print the lists for debugging
                            return False
                        # Compare each element in the sorted lists
                        for k in range(len(tupgr)):
                            if tupgr[k] != tuprel[k]:
                                print(tupgr, tuprel)  # Print the lists for debugging
                                return False
                        continue  # Continue to the next element
                    print(tup_gr, tup_rel)  # Print the tuples for debugging
                    return False
            # Check if the element is a number (int or float)
            elif isinstance(tup_rel[j], (int, float)):
                # If it's a number, compare the absolute difference
                if abs(tup_rel[j] - tup_gr[j]) > 1e-5:
                    print(tup_gr, tup_rel)  # Print the tuples for debugging
                    return False

    return True  # Return True if all comparisons pass

# Function to execute queries in Neo4j and PostgreSQL, and compare their results
def getResults(graph_query, relational_query, task):
    # Execute the Neo4j query and store the result
    neo4j_result = execute_neo4j_query(graph_query)
    # Execute the PostgreSQL query and store the result
    postgresql_result = executePostgresqlQuery(relational_query)
    tuples = []

    # Iterate through each record in the Neo4j result
    for record in neo4j_result:
        tup = []
        # Iterate through each item in the record
        for item in record:
            # Check if the item is a list (e.g., a list of nodes)
            if isinstance(item, list):
                result = ''
                # Iterate through each node in the list
                for node in item:
                    # Concatenate the node's word and type with parentheses
                    result += node['word'] + '(' + node['type'] + '), '
                # Remove the trailing comma and space
                result = result[:-2]
                # Append the formatted result to the tuple
                tup.append(result)
            # Check if the item is a Neo4j node
            elif isinstance(item, neo4j.graph.Node):
                # Append the node's word and type to the tuple
                tup.append(item['word'])
                tup.append(item['type'])
                # If the task is 7, append the node's sentiment to the tuple
                if task == 7:
                    tup.append(item['sentiment'])
            # Check if the item is a number (int, float) or string
            elif isinstance(item, (int, float, str)):
                # Append the item to the tuple
                tup.append(item)

        # Convert the tuple to a sorted list and append it to the list of tuples
        tuples.append(tuple(tup))

    # Sort the PostgreSQL result and the list of tuples and return them
    return sorted(postgresql_result), sorted(tuples)


# Read queries from files
relational_queries = readQueriesFromFile("relational_queries.txt")
graph_queries = readQueriesFromFile("graph_queries.txt")
errors = 0

# Compare results for each task
for i in range(min(len(relational_queries), len(graph_queries))):
    task = i+1

    print('Dealing with Task', task)
    relational_results, graph_results = getResults(graph_queries[i], relational_queries[i], task)
    if compareResults(relational_results, graph_results):
        print('Success! The results of the queries are the same!')
    else:
        print('Something went wrong in Task', task)
        errors += 1

print(errors, 'errors occurred')