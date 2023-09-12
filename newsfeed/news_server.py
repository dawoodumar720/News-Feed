import grpc
from concurrent import futures
import newsfeed_pb2
import newsfeed_pb2_grpc
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import datetime
import pika

# NewsHistory table
Base = declarative_base()

class NewsHistory(Base):
    __tablename__ = 'news_history'
    
    id = Column(Integer, primary_key=True)
    url = Column(String)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)


# create the database engine and session
engine = create_engine('sqlite:///news_history.db')
Session = sessionmaker(bind=engine)

# establish a connection and create a channel
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


class NewsServicer(newsfeed_pb2_grpc.NewsServiceServicer):
    def SendNewsURL(self, request, context):
        url = request.news_url

        # --------- Code for rabbitmq ----------

        # declare the queue you want to subscribe to
        channel.queue_declare(queue='news-queue')

        # lisher to publish messages to the queue
        channel.basic_publish(exchange='', 
                            routing_key='news-queue',
                            body=url)

        # ------------- Code for SQL -------------

        # check if url with same id exists in database
        session = Session()
        existing_url = session.query(NewsHistory).filter_by(url=url).first()

        if existing_url:
            session.close()
            return newsfeed_pb2.NewsResponse(message=f"URL already exists: {url}")


        # if exsisting url not exsist in sql store the URL in the history table
        news_entry = NewsHistory(url=url)
        session.add(news_entry)
        session.commit()
        session.close()

        # send grpc server response to client
        # print(f"News Freed URL: {url}")
        return newsfeed_pb2.NewsResponse(message=f"News Freed URL: {url}")
    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    newsfeed_pb2_grpc.add_NewsServiceServicer_to_server(NewsServicer(), server)

    port = 50051
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server is started on Port: {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    # create the table
    Base.metadata.create_all(engine)  
    serve()
