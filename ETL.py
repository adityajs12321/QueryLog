from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from langchain_community.chat_models import ChatOllama
from datetime import datetime
from pydantic import BaseModel, Field
import os
from google import genai
import time
import re
from bs4 import BeautifulSoup

def gemini_response(client: genai.Client, messages: list, model: str, format=None) -> str:
    chat_history = []
    for message in messages[:len(messages) - 1]:
        chat_history.append(
            {
                "role": "model" if (message["role"] == "system" or message["role"] == "assistant") else message["role"],
                "parts": [
                    {
                        "text": message["content"]
                    }
                ]
            }
        )
    # print(chat_history)
    config = {}
    if (format != None): config = {"response_mime_type": "application/json", "response_schema": format}
    print(format)
    chat = client.chats.create(model=model, history=chat_history, config=config)
    response = chat.send_message(messages[-1]["content"])
    return response.text

class Query(BaseModel):
    Query: str = Field(..., description="The query")

def transform(record):
    return {
        "Timestamp": datetime.strptime(record[0], "%Y-%m-%d %H:%M:%S"),
        "EventID": record[1],
        "Description": record[2],
        "Username": record[3],
        "TargetAccount": record[4]
    }

def extract_tag_content(text: str, tag: str) -> list[str]:
    """
    Extracts all content enclosed by specified tags (e.g., <thought>, <response>, etc.).

    Parameters:
        text (str): The input string containing multiple potential tags.
        tag (str): The name of the tag to search for (e.g., 'thought', 'response').

    Returns:
        dict: A dictionary with the following keys:
            - 'content' (list): A list of strings containing the content found between the specified tags.
            - 'found' (bool): A flag indicating whether any content was found for the given tag.
    """
    # Build the regex pattern dynamically to find multiple occurrences of the tag
    tag_pattern = rf"<{tag}>(.*?)</{tag}>"

    # Use findall to capture all content between the specified tag
    matched_contents = re.findall(tag_pattern, text, re.DOTALL)

    return [content.strip() for content in matched_contents]

def ETL_XML(xml_file):
    events = BeautifulSoup(xml_file, "xml")
    events = events.findAll("Event")
    documents = []
    for event in events:
        document = {}
        document["ProviderName"] = event.find("Provider").get("Name")
        document["EventID"] = int(event.find("EventID").text)
        document["Version"] = int(event.find("Version").text)
        document["Level"] = int(event.find("Level").text)
        document["Task"] = int(event.find("Task").text)
        document["Opcode"] = event.find("Opcode").text
        document["Keywords"] = event.find("Keywords").text
        document["TimeCreated"] = datetime.strptime(event.find("TimeCreated").get("SystemTime"), "%Y-%m-%d %H:%M:%S")
        document["EventRecordID"] = int(event.find("EventRecordID").text)
        document["Channel"] = event.find("Channel").text
        document["Computer"] = event.find("Computer").text
        # document["Security"] = event.find("Security")
        event_data = event.find("EventData")

        if event_data:
            for data_item in event_data.find_all("Data"):
                document[data_item.get("Name")] = data_item.text
        
        documents.append(document)
    return documents

def connect_to_mongodb():
    """Establishes a connection to MongoDB."""
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

    try:
        db = client[db_name]
        collection = db[collection_name]

        collection.insert_many(documents)
        print(f"Inserted {len(documents)} documents into MongoDB collection: {collection_name}")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")
        return None

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

def create_index(es_client, index_name):
    """Creates an Elasticsearch index if it doesn't exist."""

    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
    
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
    
    search_results = []
    try:
        response = es_client.search(index=index_name, body=query)
        # Extract the actual documents from the response
        hits = response['hits']['hits']
        aggregations = response.get("aggregations")
        print(f"Found {len(hits)} documents:")
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

# llm = ChatOllama(model="gemma3:4b")
llm = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

#STRUCTURED LOGS SYSTEM PROMPT
# SYSTEM_PROMPT = """
# You are an ElasticSearch agent that generates an Elastic Search query that answers the user's question. Format the output in JSON.
# Here is the table structure:

# Timestamp, EventID, Description, Username, TargetAccount

# Example Usage:

# User: "Return all records that are associated with jacksonheather"
# Response: "{query: {"match": {"Username": "jacksonheather"}}}"

# User: "Who attempted to reset their password multiple times"
# Response: "{"size": 10, "query": {"match": {"Description": "reset password"}}, "aggs": {"password_resets": {"terms": {"field": "Username.keyword","min_doc_count": 2}}}}"

# Don' forget to add "query" to the start of the query.
# """

#UNSTRUCTURED LOGS SYSTEM PROMPT
SYSTEM_PROMPT = """
You are an ElasticSearch agent that generates an Elastic Search query that answers the user's question. Format the output in JSON.
Here is the table structure:

ProviderName: str, EventID: int, Version: int, Level: int, Task: int, Opcode: str, Keywords: str, TimeCreated: date, EventRecordID: int, Channel: str, Computer: str, SubjectUserName: str, TargetUserName: str, Action: str; Action is the description of the event.

Example Usage:

User: "Return all records that are associated with the computer ztran.corp.local"
Response: "{"query": {"term": {"Computer": "ztran.corp.local"}}}"

User: "Who was removed from the security enabled local group"
Response: "{"query": {"multi_match": {"query": "removed from security enabled local group"}, "fields": ["Action"], "minimum_should_match": 5}, "aggs": {"removed_from_group": {"terms": {"field": "TargetUserName.keyword"}}}}"

User: "Whose accounts were deleted"
Response: "{"query": {"match": {"Action": "account deleted"}}, "aggs": {"deleted_accounts": {"terms": {"field": "TargetUserName.keyword"}}}}"

Use "multi_match" for long queries and use an appropriate number for "minimum_should_match" to ensure the query is specific enough (by counting the relevant words).
Don' forget to add "query" to the start of the query.
"""

messages = [
        {"role": "system", "content": SYSTEM_PROMPT}
    ]

def search(query: str):
    messages.append({"role": "user", "content": query})


    # llm.format = Query.model_json_schema()
    # response = llm.invoke(messages)

    response = gemini_response(llm, messages, "gemini-2.5-flash", Query)

    response = Query.model_validate_json(response)

    query = response.Query
    print("\n\nResponse: ", query)

    mongo_client = connect_to_mongodb()

    if mongo_client:
            # Fetch data from PostgreSQL
        # with open("ad_simulated_events.xml", "r") as file:
        #     data = file.read()
        #     xml_file = ETL_XML(data)
        #     insert_into_mongodb(mongo_client, "Logs", "unstructured_logs", xml_file)

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

                return search_documents(es, index_name, query)
    return []

if __name__ == "__main__":
    message = input("> ")
    messages.append({"role": "user", "content": message})


    # llm.format = Query.model_json_schema()
    # response = llm.invoke(messages)

    response = gemini_response(llm, messages, "gemini-2.5-flash", Query)

    response = Query.model_validate_json(response)

    query = response.Query
    print("\n\nResponse: ", query)

    mongo_client = connect_to_mongodb()

    if mongo_client:
            # Fetch data from PostgreSQL
        # with open("ad_simulated_events.xml", "r") as file:
        #     data = file.read()
        #     xml_file = ETL_XML(data)
        #     insert_into_mongodb(mongo_client, "Logs", "unstructured_logs", xml_file)

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