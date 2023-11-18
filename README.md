# WORKSHOP_003 - Machine Learning and Data Streaming

## Introduction

Welcome to the Workshop 003 repository! Here, you'll find the codebase developed for creating an ML model targeting 'happiness_score' prediction. This project integrates a data streaming process, utilizing kafka-python, and includes a database loading mechanism.
You will find the necessary requirements and some instructions in case you want to run this repository and use it for your main projects. I hope you find it helpful! 

## Datasets

The project worked with 5 datasets, all CSV files. They held info about happiness scores in different countries, along with various related factors that changed each year. Each file represented a specific year, covering from 2015 to 2019.

If you are interested in exploring the details of the datasets previously mentioned, feel free to check them out in my [EDA](main/EDA.ipynb)

## Prerequisites

Before you get started, ensure that you have the following prerequisites installed:

- Python (version 3.10.5 recommended)
- Jupyter Notebook
- PostgreSQL (version 14.9 recommended)
- Docker and Docker-compose (version 2.10.17 and 2.10.2 recommended, respectively)

## Getting Started

1. Clone this repository to your local machine using git bash. You can use the following command.
    
    ```
    git clone https://github.com/xilenAtenea/workshop003_etl
    
    ```
    
2. In your SQL shell (psql) create the project’s database. In my case I named it 'workshop3'.
    
    ```jsx
    CREATE DATABASE workshop3;
    ```
    
3. Create a virtual environment whenever you wanna work. You can name it as you prefer; in this case, we'll use ‘wenv’ as an example.
    
    ```
    python -m venv wenv
    ```
    
4. Activate the virtual environment:
    
    ```
    ./wenv/bin/activate
    
    ```
    
5. Install the required Python packages in your environment by running the following command:
    
    ```
    pip install -r requirements.txt
    ```
    
6. Create database credentials file 'config_db.json' with the following structure and fill it with your own credentials:
    
    ```
    {
    "host": "",
    "user": "",
    "password": "",
    "database":""
    }
    
    ```
    
    > Note: Remember the name of the database you created in previous steps and keep in mind your PostgreSQL user credentials.
    > 
7. Ensure the existence of a 'data/' directory containing the datasets you intend to use. Remember the specifics mentioned earlier about the datasets.
8. To use kafka make sure to start the containers, remember to be in the path that has the ‘docker-compose.yml’ for the command below to work.
    
    ```jsx
    docker-compose up -d
    ```
    
9. When the containers have been started you must create the kafka topic through which the messages will be sent, in this case it will be called 'test-data' as established in the codes. Follow the instructions below:
    - Enter the newly started kafka container.
    
    ```jsx
    docker exec -it kafka bash
    ```
    
    - Run the following command to create the mentioned topic.
    
    ```jsx
    kafka-topics --bootstrap-server kafka:9092 --create --topic test-data
    ```
    
    - Exit the iterable tty with 'exit'.
10. To explore the EDA, open Jupyter and select the kernel linked to the newly established virtual environment ('wenv'). Ensure that for all notebooks, you designate the environment as the notebook's kernel.

Follow the codes and comments in the workshop files to understand the various parts of the project.

## Repository Structure

### main/

This folder contains the main components of the project, which contain the processes of data extraction, transformation, data loading and the creation of the ML model, its implementation and the creation of metrics to evaluate the performance of the implemented model.

The following is a brief description of what is contained in each of the files in this folder:

**EDA.ipynb:** This folder holds the step-by-step transformations made to the datasets to create one complete dataset. This combined dataset is used to train and test the model within this file. Once trained, the model is exported as a .pkl file.

**my_model.pkl:** This is the model exported in the EDA.

**feature_selection.ipynb:** This folder includes same transformations as the EDA but simplified for feature selection. It also manages the test data set, passing it through a KafkaProducer for data streaming purposes.

**model_prediction.ipynb:** File that receives the data with a kafkaConsumer, imports the model to make the prediction and uses the functions of 'db_queries.py' to load the data to the PostgreSQL database.

**metric.ipynb:** Takes data from the database and makes metrics to evaluate the performance of the model.

**db_queries.py:** File that contains the database functions (connection to the db, create table and insert data).

**config_db.json:** File that must be created as previously shown in the 'Getting Started' part, in my case it was located inside this directory.

### docker-compose.yml

This docker-compose.yml file defines two services: zookeeper and kafka-broker-1. Zookeeper is a tool for managing distributed services, while kafka-broker-1 is a Kafka server.
The kafka service depends on zookeeper. It is configured for Kafka-broker with ID 1 and connects to zookeeper at 'zookeeper:2181'. In addition, Kafka is configured to listen on two different ports: 9092 and 29092.

### requirements.txt

This file includes all the essential dependencies needed for an easier installation after cloning the repository.

### data/

This folder holds the project's essential data, comprising the five mentioned CSV files. Remember to add the relevant files, ensuring they match the characteristics of the datasets used in the project.

---

## References

[1] *Multiple Linear Regression With scikit learn*. (2022, July 9). GeeksforGeeks; GeeksforGeeks. https://www.geeksforgeeks.org/multiple-linear-regression-with-scikit-learn/

[2] jcdmb. (2013, July 7). *Python scikit-learn: exporting trained classifier*. Stack Overflow. https://stackoverflow.com/questions/17511968/python-scikit-learn-exporting-trained-classifier
