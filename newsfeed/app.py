from flask import Flask, jsonify, request
import grpc
from newsfeed_pb2 import NewsRequest
from newsfeed_pb2_grpc import NewsServiceStub
from elasticsearch import Elasticsearch
from py2neo import Graph

app = Flask(__name__)


@app.route("/")
def index():
    return "Flask App!"

@app.route("/get_es", methods=["GET"])
def get_es():
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    try:
        
        # Simple match query
        query = {
            'size': 1000,
            'query': {
                'match_all': {}
                }
        }

        # Execute the query
        result = es.search(index="news_rss", doc_type="feeds", body=query)

        # extract and return the hits
        hits = result['hits']['hits']
        data = [hit['_source'] for hit in hits]

        return jsonify(data)
    
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/get_neo4j", methods=["GET"])
def get_neo():
    uri = "bolt://localhost:7687"
    graph = Graph(uri, auth=("neo4j", "neo4j1"))
    try:
        # Query to retrieve all News nodes and their properties
        query = (
            "MATCH (n:News)-[r:posted_on]->(s:Source) "
            "RETURN n.title AS title, n.description AS description, n.link AS link, "
            "n.published_date AS published_date, n.source AS source"
        )
        
        result = graph.run(query).data()
        
        # Convert result to a list of dictionaries
        entries = [{"title": r["title"], "description": r["description"],
                    "link": r["link"], "published_date": r["published_date"],
                    "source": r["source"]} for r in result]
        
        return jsonify(entries)
    
    except Exception as e:
        return jsonify({"error": str(e)})



@app.route("/route", methods=["POST"])
def url_data():
    if request.method == "POST":
        data = request.get_json()  
        if "news_url" in data:
            news_url = data["news_url"]
            channel = grpc.insecure_channel("localhost:50051")
            client = NewsServiceStub(channel)

            grpc_request = NewsRequest(news_url=news_url)
            response = client.SendNewsURL(grpc_request)
            return f"{response}"
        else:
            return "'news_url' is missing in request", 400
    else:
        return "Only POST requests are allowed.", 405


if __name__ == "__main__":
    # run app in debug mode on port 5000
    app.run(debug=True, port=5000, host="0.0.0.0")
