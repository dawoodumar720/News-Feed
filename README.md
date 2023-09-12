# News-Feed
A News Scraper 
Scrape News from News Channels, Store news in elasticserach and neo4j, have flask routes for getting data from databases. 
Store a table of url to keep track the history of urls serach for news. 
Implement a grpc in it, that takes url from grpc client and pass it to the grpc server where url is store in sqlite and passed to the rabbitmq to queue.  
After Receiving the url to rabbitmq reciver the scraper function is implemented using bs4 and threading and mutliprocessing and functions for storing the scraped data in the databases. Also check the duplicate data and update data in databases.
