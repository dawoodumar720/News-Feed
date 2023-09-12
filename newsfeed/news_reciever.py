import pika, sys, os
import feedparser
import pandas as pd
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from py2neo import Graph, Node, Relationship


# method to display fetched news
def parse_rssFeed(feed_url):
    try:
        # fetch and parse the feed
        feed = feedparser.parse(feed_url)

        try:
            # fetch news channel title
            title = feed.feed.title
        except AttributeError:
            # extract the title from the link URL
            link_parts = feed_url.split("/")
            title = link_parts[2]

        # count the number of entries
        total_entries = len(feed.entries)

        # iterate through feed entries
        for entry in feed.entries:
            # check for extra <p> tag in description
            if "<p>" in entry.description:
                description_text = BeautifulSoup(
                    entry.description, "html.parser"
                ).get_text()
                print("Description:", description_text)
            else:
                print("Description:", entry.description)

            print("Title:", entry.title)
            print("Link:", entry.link)
            print("Published Date:", entry.published)
            print("Source:", title)
            print("\n")

        print(f"Total number of entries from {title} is: {total_entries}\n")

    except Exception as error:
        print("An error occurred while parsing the feed:")
        print(str(error))
        print("\n")


# method to index fetched news in elastic search
def index_es(feed_url):
    try:
        # ------------ Elastic Search Connection -----------------
        es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

        # check if the connection is successful
        if es.ping():
            print("Connected to Elasticsearch")
        else:
            print("Connection to Elasticsearch failed")

        # fetch and parse the feed
        feed = feedparser.parse(feed_url)

        try:
            # fetch news channel title
            title = feed.feed.title
        except AttributeError:
            # extract the title from the link URL
            link_parts = feed_url.split("/")
            title = link_parts[2]

        # count the number of entries
        total_entries = len(feed.entries)

        # iterate through feed entries and index them
        for entry in feed.entries:
            # check if the description contains HTML tags
            if "<p>" in entry.description:
                description_text = BeautifulSoup(
                    entry.description, "html.parser"
                ).get_text()
                description = description_text
            else:
                description = entry.description

            # query to check duplicate
            try:
                query = {"query": {"match": {"title.keyword": entry.title}}}

                result = es.search(index="news_rss", doc_type="feeds", body=query)
                result_count = result["hits"]["hits"]

            except:
                result_count = []

            if len(result_count) == 0:
                rss_entry = {
                    "title": entry.title,
                    "description": description,
                    "link": entry.link,
                    "published_date": entry.published,
                    "source": title,
                }

                # index the entry in Elasticsearch
                es.index(index="news_rss", doc_type="feeds", body=rss_entry)
                print("Indexed:", entry.title)

            else:
                print("Duplicate Entry:", entry.title)

        print(f"\nTotal number of enteries from {title} is: {total_entries}\n")

    except Exception as error:
        print("An error occurred while parsing the feed:")
        print(str(error))
        print("\n")


# method to index fetched news in neo4j and also built relations
def index_neo4j(feed_url):
    try:
        uri = "bolt://localhost:7687"
        graph = Graph(uri, user="neo4j", password="neo4j1")

        if graph:
            print("Connected to Neo4j.")
        else:
            print("Connection to Neo4j is not established.")

        # fetch and parse the feed
        feed = feedparser.parse(feed_url)

        try:
            # fetch news channel title
            title = feed.feed.title
        except AttributeError:
            # extract the title from the link URL
            link_parts = feed_url.split("/")
            title = link_parts[2]

        # iterate through feed entries and index them
        for entry in feed.entries:
            try:
                # Check if the entry already exists based on title and link
                query = (
                    f"MATCH (n:News) "
                    f"WHERE n.title = '{entry.title}'"
                    f"RETURN n"
                )
                result = graph.run(query).data()

                if not result:  # Entry doesn't exist, proceed to create
                    # check if the description contains HTML tags
                    if "<p>" in entry.description:
                        description_text = BeautifulSoup(
                            entry.description, "html.parser"
                        ).get_text()
                        description = description_text
                    else:
                        description = entry.description

                    # Use the feed's title as the source name
                    source_name = title

                    # Create or fetch the target/source node
                    target_node = Node("Source", name=source_name)
                    graph.merge(target_node, "Source", "name")

                    # Create the main node with properties
                    properties = {
                        "title": entry.title,
                        "description": description,
                        "link": entry.link,
                        "published_date": entry.published,
                        "source": title,  # Use the feed's title as the source property
                    }
                    main_node = Node("News", **properties)
                    graph.create(main_node)

                    # Create relationship between main node and target node
                    relationship = Relationship(
                        main_node,
                        "posted_on",
                        target_node,
                        published_date=entry.published,
                    )
                    graph.create(relationship)
                    print("Indexed:", entry.title)
                else:
                    print("Duplicate Entry Neo4j:", entry.title)

            except Exception as error:
                print("An error occurred while indexing entry:", str(error))

        print(f"\nTotal number of entries from {title} is: {len(feed.entries)}\n")

    except Exception as error:
        print("An error occurred while parsing the feed:")
        print(str(error))
        print("\n")


def main():
    """This is the main function"""
    # ---------- Connection for RabbitMQ -----------------

    # establish a connection and create a channel
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    # declare the queue you want to subscribe to
    channel.queue_declare(queue="news-queue")

    # Define the callback function
    def callback(ch, method, properties, body):
        print(f"[x] Recieved message with URL: {body}")
        received_data = body.decode("utf-8")

        # method for display data
        # parse_rssFeed(received_data)

        # method to store data in elasticsearch
        # index_es(received_data)

        # method to store data in neo4j
        index_neo4j(received_data)

    # ------------- RabbitMQ Consuming Code -------------------

    # Set up the consumer to the queue and associate the callback function
    channel.basic_consume(
        queue="news-queue", on_message_callback=callback, auto_ack=True
    )

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
