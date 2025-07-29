import psycopg2
from elasticsearch import Elasticsearch, helpers
import time
from pymongo import MongoClient
import json
from datetime import datetime
from bson import ObjectId
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer

def ETL_XML(xml_file):
    events = BeautifulSoup(xml_file, "xml")
    events = events.findAll("Event")
    documents = []
    for event in events:
        document = {}
        document["ProviderName"] = event.find("Provider").get("Name")
        document["EventID"] = event.find("EventID").text
        document["Version"] = event.find("Version").text
        document["Level"] = event.find("Level").text
        document["Task"] = event.find("Task").text
        document["Opcode"] = event.find("Opcode").text
        document["Keywords"] = event.find("Keywords").text
        document["TimeCreated"] = event.find("TimeCreated").get("SystemTime")
        document["EventRecordID"] = event.find("EventRecordID").text
        document["Channel"] = event.find("Channel").text
        document["Computer"] = event.find("Computer").text
        # document["Security"] = event.find("Security")
        event_data = event.find("EventData")
        print("EventData", event_data)

        if event_data:
            for data_item in event_data.find_all("Data"):
                document[data_item.get("Name")] = data_item.text
        
        documents.append(document)
    return documents

def connect_to_mongodb():
    """MongoDB Connection."""
    try:
        # Adjust connection string as needed
        client = MongoClient("mongodb://localhost:27017/")
        # Test connection
        client.admin.command('ping')
        print("Connected to MongoDB")
        return client
    except Exception as e:
        print(f"Could not connect to MongoDB: {e}")
        return None

def insert_into_mongodb(client: MongoClient, db_name, collection_name, documents):
    """Inserts data into MongoDB."""
    db = client[db_name]
    collection = db[collection_name]

    collection.insert_many(documents)


def fetch_data_from_mongodb(client, db_name, collection_name):
    """Fetches data from MongoDB collection."""
    if not client:
        return []
    
    try:
        db = client[db_name]
        collection = db[collection_name]
        
        # Fetch all documents
        documents = list(collection.find())
        documents = [{k: v for k,v in document.items() if k != "_id"} for document in documents]
        print(f"Fetched {len(documents)} documents from MongoDB")
        return documents
    except Exception as e:
        print(f"Error fetching data from MongoDB: {e}")
        return []

def connect_to_elasticsearch():
    """Establishes a connection to Elasticsearch."""
    es = Elasticsearch(hosts=["https://localhost:9200"], basic_auth=["elastic", "vkGS-g1kirw7OO-pobRo"], verify_certs=False, ssl_show_warn=False)
    if es.ping():
        print("Connected to Elasticsearch")
        return es
    else:
        print("Could not connect to Elasticsearch")
        return None

def create_index(es_client: Elasticsearch, index_name):
    """Creates an Elasticsearch index if it doesn't exist."""

    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
    
    try:
        # es_client.indices.create(index=index_name)
        mapping = {'mappings': {'properties': {'Action': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, "Action_vector": {"type": "dense_vector", "dims": 384, "index": True, "similarity": "cosine"}, 'Channel': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'Computer': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'EventID': {'type': 'long'}, 'EventRecordID': {'type': 'long'}, 'Keywords': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'Level': {'type': 'long'}, 'Opcode': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'ProviderName': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'SubjectUserName': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'TargetUserName': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}, 'Task': {'type': 'long'}, 'TimeCreated': {'type': 'date'}, 'Version': {'type': 'long'}}}}
        es_client.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created")
    except Exception as e:
        print(f"Error creating index: {e}")

def index_data_to_elasticsearch(es_client, model, index_name, data):
    """Indexes data into the specified Elasticsearch index."""
    if not es_client or not data:
        return
    actions = []
    for record in data:
        embedding = model.encode(record.get("Action"))
        record["Action_vector"] = embedding.tolist()  # Convert numpy array to list for JSON serialization
        action = {
            "_index": index_name,
            "_source": record
        }
        actions.append(action)

    try:
        helpers.bulk(es_client, actions)
        print("Data indexed successfully")
    except helpers.BulkIndexError as e:
        print(f"Error indexing data: {e}")

def getvalue(nested_dict, value, prepath=()):
    for k, v in nested_dict.items():
        path = prepath + (k,)
        if k == value: # found value
            return v
        elif hasattr(v, 'items'): # v is a dict
            p = getvalue(v, value, path) # recursive call
            if p is not None:
                return p

def setvalue(nested_dict, value, set_value):
    for k, v in nested_dict.items():
        if k == value: # found value
            nested_dict[k] = set_value
            return None
        elif hasattr(v, 'items'): # v is a dict
            setvalue(nested_dict[k], value, set_value) # recursive call

def switch_to_knn(nested_dict, model):
    for k, v in nested_dict.items():
        if k == "match": # found value
            if ("Action" in list(v.keys())):
                embedded_message = model.encode(nested_dict[k]["Action"])
                nested_dict["knn"] = {
                        "field": "Action_vector",
                        "query_vector": embedded_message.tolist(),
                        "k": 50,
                        "num_candidates": 100
                    }
                
                del nested_dict[k]
            return None
        elif hasattr(v, 'items'): # v is a dict
            switch_to_knn(nested_dict[k], model) # recursive call
        elif isinstance(v, list):
            for i in range(len(v)):
                if v[i].get("match") is not None:
                    if (list(v[i].get("match").keys())[0] == "Action"):
                        embedded_message = model.encode(nested_dict[k][i]["match"]["Action"])
                        nested_dict[k][i] = {"knn": {
                            "field": "Action_vector",
                            "query_vector": embedded_message.tolist(),
                            "k": 50,
                            "num_candidates": 100
                        }}
                        return None

def search_documents(es_client, model, index_name, query):
    """
    Searches for documents in the specified index using the given query.
    """
    if not es_client:
        return
    
    query_string = getvalue(query, "Action")
    if query_string is not None:
        encoded_string = model.encode(query_string)
        setvalue(query, "Action", encoded_string.tolist())

    search_results = []
    try:
        response = es_client.search(index=index_name, body=query)
        # Extract the actual documents from the response
        hits = response['hits']['hits']
        aggregations = response.get("aggregations")
        print(f"Found {len(hits)} documents:")
        if hits == []:
            return search_results
        
        print(aggregations)
        if (aggregations == None):
            for hit in hits:
                search_results.append(hit['_source'])
                # Each hit contains the document in the _source field
                print(hit['_source'])
        else:
            # print(aggregations)
            for bucket in list(aggregations.values()):
                if (bucket.get("buckets") is not None):
                    search_results.append(bucket["buckets"])
                    print(bucket["buckets"])
                elif (bucket.get("value_as_string") is not None):
                    search_results.append(bucket["value_as_string"])
                    print(bucket["value_as_string"])
        
        return search_results
    except Exception as e:
        print(f"An error occurred: {e}")

# STRUCTURED LOG QUERIES
# query = {"size": 10, "query": {"match": {"Description": "reset password"}}}
# query = {"query": {"match": {"Description": "removed from security enabled global group"}}, "aggs": {"removed_users": {"terms": {"field": "Username.keyword"}}}}
# query = {"query": {"range": {"Timestamp": {"gte": "2025-06-14","lte": "2025-06-25"}}}}
# query = {"query": {"bool": {"must": [{"match": {"Description": "reset passwords"}}, {"range": {"Timestamp": {"gte": "2025-06-14", "lte": "2025-06-25"}}}]}}, "aggs": {"password_resets_by_user": {"terms": {"field": "Username.keyword"}}}}
# query = {"query": {"match": {"description": "account created"}}}

# STRUCTURED LOGFILE
# if __name__ == "__main__":
#     # PostgreSQL connection details
#     mongo_client = connect_to_mongodb()

#     if mongo_client:
#         # Fetch data from PostgreSQL
#         postgres_data = fetch_data_from_mongodb(mongo_client, "Logs", "logs2")
#         mongo_client.close()

#         if postgres_data:
#             # Elasticsearch connection details
#             es = connect_to_elasticsearch()
#             if es:
#                 index_name = "logs4_index"

#                 if not es.indices.exists(index=index_name):
#                     create_index(es, index_name)
#                     # Index the data
#                 index_data_to_elasticsearch(es, index_name, postgres_data)
#                 time.sleep(1)

#                 search_documents(es, index_name, query)

model = SentenceTransformer('all-MiniLM-L6-v2')

# UNSTRUCTURED LOG QUERIES
# query = {"query": {"match": {"Action": "removed from security-enabled local group"}}}
# query = {"size": 10000, "query": {"multi_match": {"query": "account created", "minimum_should_match": "2"}}}
# query = {"size": 50, "query": {"multi_match": {"query": "security settings", "minimum_should_match": 2}}}
# query = {"query": {"match": {"Action": "account created"}}, "aggs": {"creation_time": {"terms": {"field": "SubjectUserName.keyword"}}}}
# query = {"size": 50, "query": {"match": {"Action": "removed from security enabled local group"}}}
# query = {"query": {"match": {"Action": "removed from security enabled local groups"}}, "aggs": {"users_removed": {"terms": {"field": "TargetUserName.keyword"}}}}
# query = {"query": {"bool": {"must": [{"match": {"Action": "account created"}}, {"term": {"SubjectUserName.keyword": "natalierivera"}}]}}, "aggs": {"creation_times": {"terms": {"field": "TimeCreated"}}}}
# query = {"query": {"match": {"Action": "reset password"}}}
query = {"query": {"match": {"Action": "password reset"}}, "aggs": {"users_reset_password": {"terms": {"field": "SubjectUserName.keyword"}}}}

# message = "removed from security enabled global group"
# message = model.encode(message)
# query = {"knn": {"field": "Action_vector","query_vector": message.tolist(),"k": 50,"num_candidates": 100}, "aggs": {"removed_users": {"terms": {"field": "TargetUserName.keyword"}}}, "_source": {"excludes": ["Action_vector"]}}

# query = {"query": {"bool": {"must": [{"match": {"Action": "account created"}}, {"term": {"SubjectUserName.keyword": "natalierivera"}}]}}, "aggs": {"creation_time": {"min": {"field": "TimeCreated"}}}}
# query = {"query": {"bool": {"must": [{"knn": {"field": "Action_vector","query_vector": message.tolist(),"k": 50,"num_candidates": 100}}, {"term": {"SubjectUserName.keyword": "natalierivera"}}]}}, "aggs": {"creation_time": {"min": {"field": "TimeCreated"}}}}


switch_to_knn(query, model)
query["_source"] = {"excludes": ["Action_vector"]}
# print("Query: ", query)
query["min_score"] = 0.9
# PIPELINE
# message = getvalue(query, "Action")
# if message is not None:
#     encoded_message = model.encode(message)
#     del query["query"]
#     query["knn"] = {
#         "field": "Action_vector",
#         "query_vector": encoded_message.tolist(),
#         "k": 50,
#         "num_candidates": 100
#     }
#     query["_source"] = {"excludes": ["Action_vector"]}

  # Load the pre-trained model for sentence embeddings
# Unstructured Logfile
if __name__ == "__main__":
    # PostgreSQL connection details
    mongo_client = connect_to_mongodb()
    
    with open("ad_simulated_events.xml", "r") as file:
        # data = file.read()
        # xml_file = ETL_XML(data)
        file.close()
        # insert_into_mongodb(mongo_client, "Logs", "unstructured_logs", xml_file)

        if mongo_client:
            # Fetch data from MongoDB
            mongodb_data = fetch_data_from_mongodb(mongo_client, "Logs", "unstructured_logs")
            mongo_client.close()

            if mongodb_data:
                # Elasticsearch connection details
                es = connect_to_elasticsearch()
                if es:
                    index_name = "unstructured_logs_index"

                    create_index(es, index_name)
                        
                    # Index the data
                    index_data_to_elasticsearch(es, model, index_name, mongodb_data)
                    time.sleep(1)

                    search_results = []
                    while True:
                        search_results = search_documents(es, model, index_name, query)
                        if (search_results == []):
                            query["min_score"] = query["min_score"] - 0.05
                        else:
                            query["min_score"] = 0.9
                            break