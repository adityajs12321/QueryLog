import psycopg2
from elasticsearch import Elasticsearch, helpers
import time
from pymongo import MongoClient
import json
from datetime import datetime
from bson import ObjectId
from bs4 import BeautifulSoup

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

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name, ignore=[400, 404])
    try:
        es_client.indices.create(index=index_name)
        print(f"Index '{index_name}' created")
    except Exception as e:
        print(f"Error creating index: {e}")

def index_data_to_elasticsearch(es_client, index_name, data):
    """Indexes data into the specified Elasticsearch index."""

    if not es_client or not data:
        return
    actions = [
        {
            "_index": index_name,
            "_source": record
        }
        for record in data
    ]
    try:
        helpers.bulk(es_client, actions)
        print("Data indexed successfully")
    except helpers.BulkIndexError as e:
        print(f"Error indexing data: {e}")

def search_documents(es_client, index_name, query):
    """
    Searches for documents in the specified index using the given query.
    """
    if not es_client:
        return

    try:
        response = es_client.search(index=index_name, body=query)
        # Extract the actual documents from the response
        hits = response['hits']['hits']
        aggregations = response.get("aggregations")
        print(aggregations)
        print(f"Found {len(hits)} documents:")
        if (aggregations == None):
            for hit in hits:
                # Each hit contains the document in the _source field
                print(hit['_source'])
        else:
            # print(aggregations)
            for bucket in list(aggregations.values()):
                if (bucket.get("buckets") is not None):
                    print(bucket["buckets"])
                elif (bucket.get("value_as_string") is not None):
                    print(bucket["value_as_string"])
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


# UNSTRUCTURED LOG QUERIES
# query = {"query": {"match": {"Action": "removed from security-enabled local group"}}}
# query = {"size": 10000, "query": {"multi_match": {"query": "account created", "minimum_should_match": "2"}}}
# query = {"size": 50, "query": {"multi_match": {"query": "security settings", "minimum_should_match": 2}}}
query = {"query": {"match": {"Action": "account created"}}, "aggs": {"creation_time": {"terms": {"field": "SubjectUserName.keyword"}}}}
# query = {"query": {"bool": {"must": [{"match": {"Action": "account created"}}, {"term": {"SubjectUserName.keyword": "natalierivera"}}]}}, "aggs": {"creation_time": {"min": {"field": "TimeCreated"}}}}

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
                    index_data_to_elasticsearch(es, index_name, mongodb_data)
                    time.sleep(1)

                    search_documents(es, index_name, query)