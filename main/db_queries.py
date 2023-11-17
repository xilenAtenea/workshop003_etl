"""Database related functions"""

import json

import pandas as pd
import psycopg2 as psy


def create_db_connection() -> psy.connect:
    """Function that creates a database connection using the configuration parameters in the file `config_db.json`.

    Returns
    -------
        A psycopg2 connection object.
    """
    with open('config_db.json') as config_json:
        config = json.load(config_json)
    conx = psy.connect(**config)
    return conx

def create_table() -> str :
    """Creates a table named `regression_model` in the database connection returned by the function `create_db_connection()`.

    Returns
    -------
        The string `"ok"` if successful else an error message.
    """
    conx = create_db_connection()
    cursor = conx.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS regression_model (
    id serial PRIMARY KEY,
    gdp_per_capita FLOAT,
    social_support FLOAT,
    healthy_life_expectancy FLOAT,
    freedom_to_make_life_choices FLOAT,
    perceptions_of_corruption FLOAT,
    happiness_score FLOAT,
    happiness_score_prediction FLOAT
    );
    """)

    conx.commit()
    cursor.close()
    conx.close()
    return "ok"

def insert_data(df: pd.DataFrame) -> str:
  """Insert data into the database table `regression_model`
    
    Parameters
    ----------
    df : DataFrame
          Pandas dataframe containing the data to be inserted in the database.
        
    Returns
    -------
    The string `"ok"` if successful else an error message.
    """
  conx = create_db_connection()
  cursor = conx.cursor()

  insert_query = """
    INSERT INTO regression_model (
      gdp_per_capita,
      social_support,
      healthy_life_expectancy,
      freedom_to_make_life_choices,
      perceptions_of_corruption,  
      happiness_score,
      happiness_score_prediction
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
  """
  
  for _, row in df.iterrows():
    data = (
      row['gdp_per_capita'], 
      row['social_support'],
      row['healthy_life_expectancy'],
      row['freedom_to_make_life_choices'],
      row['perceptions_of_corruption'],
      row['happiness_score'],
      row['happiness_score_prediction']  
    )

    cursor.execute(insert_query, data)

  conx.commit()
  
  cursor.close()
  conx.close()
  
  return "ok"

def get_real_data_and_predic_from_db() -> pd.DataFrame:
  """Get real and predicted values from the database table `regression_model`.
    
    Returns
    -------
    A pandas dataframe with columns as follows:
      1. 'happiness_score'
      2. 'happiness_score_prediction'
    """
  conx = create_db_connection()
  cursor = conx.cursor()

  cursor.execute("SELECT happiness_score, happiness_score_prediction FROM regression_model;")
  column_names = [desc[0] for desc in cursor.description]
  data = cursor.fetchall()

  cursor.close()
  conx.close()

  df = pd.DataFrame(data, columns=column_names)

  return df