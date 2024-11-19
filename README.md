This repositorys includes a data pipeline to stream data from a Kaggle dataset capturing UK user behavior on Netflix browsing activity to Confluent Cloud for movie analysis

Kaggle dataset - https://www.kaggle.com/datasets/vodclickstream/netflix-audience-behaviour-uk-movies 

1. A Kafka producer that reads the csv dataset and streams it to Confluent Cloud with AVRO serialization
2. Flink queries to be run on Confluent Cloud using Flink SQL Workspace for movie analysis

