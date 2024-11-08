from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook





def get_data(date, ti):
    page = requests.get(f"https://www.yallakora.com/match-center/?date={date}")
    src = page.content
    soup=BeautifulSoup(src,"lxml")
    matches_detailles=[]

    championship =soup.find_all("div",{'class':'matchCard'})

    def get_match_info(championship):
        championship_title=championship.contents[1].find("h2").text.strip()

        all_matches=championship.contents[3].find_all("div",{'class':'item finish liItem'})
        n=len(all_matches)
        for i in range(n):
            teamA=all_matches[i].find("div",{'class':'teamA'}).text.strip()

            teamB=all_matches[i].find("div",{'class':'teamB'}).text.strip()


            resultat=all_matches[i].find("div",{'class':'MResult'}).find_all('span',{'class':'score'})
            score=f"{resultat[0].text.strip()} - {resultat[1].text.strip()}"

            time = all_matches[i].find("div", {'class': 'MResult'}).find('span', {'class': 'time'}).text.strip()

            matches_detailles.append({'championat':championship_title,'teamA':teamA,'teamB':teamB,'score':score,'time':time})

    for i in range(len(championship)) :
        get_match_info(championship[i])


    df = pd.DataFrame(matches_detailles)
    
    df=df[~df['championat'].str.contains('المصري', na=False)]
    
    ti.xcom_push(key='match_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='match_data', task_ids='fetch_match_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='match_connection')
    insert_query = """
    INSERT INTO matchs (championat, teamA, teamB, score, time)
    VALUES (%s, %s, %s, %s, %s)
    """
    for match in match_data:
        postgres_hook.run(insert_query, parameters=(match['championat'], match['teamA'], match['teamB'], match['score'], match['time']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_match',
    default_args=default_args,
    description='Dag',
    schedule_interval=timedelta(days=1),
)


today_date = datetime.today().strftime('%m/%d/%Y')

fetch_data_task = PythonOperator(
    task_id='fetch_match_data',
    python_callable=get_data,
    op_args=[today_date],  
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='match_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS matchs (
        id SERIAL PRIMARY KEY,
        championat TEXT,
        teamA TEXT,
        teamB TEXT,
        score TEXT,
        time TEXT
    );
    """,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_data_task >> create_table_task >> insert_data_task
