from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'shivom',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def fetch_posts_from_postgres(**context):
    
    logger.info("Starting fetch_posts_from_postgres task")
    
    try:
        import pandas as pd
        from datetime import datetime
        from azure.storage.blob import BlobServiceClient
        from airflow.sdk import Variable
        import psycopg2
        from sqlalchemy import create_engine
        
        logger.info("All imports successful")
        
        
        
        pg_host = Variable.get('POSTGRES_HOST', 'localhost')
        pg_port = Variable.get('POSTGRES_PORT', '5432')
        pg_database = Variable.get('POSTGRES_DATABASE', 'your_database')
        pg_username = Variable.get('POSTGRES_USERNAME', 'your_username')
        pg_password = Variable.get('POSTGRES_PASSWORD', 'your_password')
        
        connection_string = f"postgresql://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
        
        logger.info(f"Connecting to PostgreSQL at {pg_host}:{pg_port}")
        

        engine = create_engine(connection_string)
        
        query = """
        SELECT 
            id,
            title,
            body,
            user_id as userId,
            tags,
            reactions_likes,
            reactions_dislikes,
            views,
            created_at
        FROM posts 
        WHERE created_at >= current_date - interval '30 days'
        ORDER BY id
        LIMIT 100
        """
        
        logger.info("Executing PostgreSQL query")
        df = pd.read_sql(query, engine)
        logger.info(f"Fetched {len(df)} posts from PostgreSQL")

        if df.empty:
            raise ValueError("No posts data received from PostgreSQL")

        
        df['id'] = df['id'].astype(str)
        df['userId'] = df['userId'].astype(str) 
        df['tags'] = df['tags'].fillna('').astype(str)
        df['reactions_likes'] = pd.to_numeric(df['reactions_likes'], errors='coerce').fillna(0)
        df['reactions_dislikes'] = pd.to_numeric(df['reactions_dislikes'], errors='coerce').fillna(0)
        df['views'] = pd.to_numeric(df['views'], errors='coerce').fillna(0)
        
        
        df['extraction_timestamp'] = datetime.now().isoformat()
        df['extraction_date'] = context['ds']
        df['source_system'] = 'PostgreSQL_Local'

        logger.info(f"Data prepared with shape: {df.shape}")

        
        azure_connection_string = Variable.get('AZURE_STORAGE_CONNECTION_STRING')
        container_name = Variable.get('AZURE_CONTAINER_NAME', 'airflow-test')
        
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        temp_blob = f"temp/raw_posts_{context['ds_nodash']}.json"
        
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=temp_blob
        )
        
        json_data = df.to_json(orient='records')
        blob_client.upload_blob(json_data, overwrite=True)
        
        logger.info(f"Saved raw PostgreSQL data to temp folder: {temp_blob}")
        return temp_blob
        
    except Exception as e:
        logger.error(f"Error in fetch_posts_from_postgres: {str(e)}")
        raise

def preprocess_posts(**context):
    logger.info("Starting preprocess_posts task")
    
    try:
        import pandas as pd
        import re
        from azure.storage.blob import BlobServiceClient
        from airflow.sdk import Variable
        
        temp_blob = context['task_instance'].xcom_pull(task_ids='fetch_posts_from_postgres')
        
        azure_connection_string = Variable.get('AZURE_STORAGE_CONNECTION_STRING')
        container_name = Variable.get('AZURE_CONTAINER_NAME', 'airflow-test')
        
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=temp_blob
        )
        
        json_data = blob_client.download_blob().readall()
        json_string = json_data.decode('utf-8')  
        df = pd.read_json(json_string, orient='records')
        
        logger.info(f"Downloaded data with shape: {df.shape}")
        
        
        df['title'] = df['title'].fillna('No Title Available')
        df['reactions_likes'] = df['reactions_likes'].clip(lower=0)
        df['reactions_dislikes'] = df['reactions_dislikes'].clip(lower=0)
        df['views'] = df['views'].fillna(0).astype(int)
        df = df[df['body'].str.len() > 10]
        
        df['id'] = df['id'].astype(int)
        df['userId'] = df['userId'].astype(int)
        df['tags_standardized'] = df['tags'].str.lower().str.strip()
        df['extraction_timestamp'] = pd.to_datetime(df['extraction_timestamp'])
        
        df['title_cleaned'] = df['title'].str.strip().str.lower()
        df['title_cleaned'] = df['title_cleaned'].str.replace(r'[^\w\s]', '', regex=True)
        df['body_cleaned'] = df['body'].str.strip()
        df['body_cleaned'] = df['body_cleaned'].str.replace(r'\s+', ' ', regex=True)
        df['body_cleaned'] = df['body_cleaned'].str.replace(r'[^\w\s.,!?]', '', regex=True)
        
        df['title_length'] = df['title'].str.len()
        df['body_length'] = df['body'].str.len()
        df['word_count'] = df['body'].str.split().str.len()
        df['total_reactions'] = df['reactions_likes'] + df['reactions_dislikes']
        df['engagement_ratio'] = df['reactions_likes'] / (df['total_reactions'] + 1)
        df['views_per_reaction'] = df['views'] / (df['total_reactions'] + 1)
        df['tag_count'] = df['tags'].str.split(',').str.len()
        df['has_tags'] = df['tags'].str.len() > 0
        df['is_short_post'] = df['body_length'] < 100
        df['is_popular'] = df['reactions_likes'] > df['reactions_likes'].quantile(0.75)
        
        
        positive_words = ['good', 'great', 'amazing', 'wonderful', 'excellent', 'love']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst', 'horrible']
        df['positive_keywords'] = df['body'].str.lower().str.count('|'.join(positive_words))
        df['negative_keywords'] = df['body'].str.lower().str.count('|'.join(negative_words))
        df['sentiment_score'] = df['positive_keywords'] - df['negative_keywords']
        df['has_url'] = df['body'].str.contains(r'http[s]?://\S+', regex=True, na=False)
        
        logger.info(f"Complex preprocessing complete. Final shape: {df.shape}")
        
        
        processed_blob = f"temp/processed_posts_{context['ds_nodash']}.json"
        processed_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=processed_blob
        )
        
        processed_json = df.to_json(orient='records')
        processed_client.upload_blob(processed_json, overwrite=True)
        
        
        blob_client.delete_blob()
        
        logger.info(f"Saved processed data to temp folder: {processed_blob}")
        return processed_blob
        
    except Exception as e:
        logger.error(f"Error in preprocess_posts: {str(e)}")
        raise

def upload_to_azure(**context):
    logger.info("Starting upload_to_azure task")
    
    try:
        import pandas as pd
        from azure.storage.blob import BlobServiceClient
        from io import StringIO
        from airflow.sdk import Variable
        from datetime import datetime
        
        
        temp_blob = context['task_instance'].xcom_pull(task_ids='preprocess_posts')
        
        azure_connection_string = Variable.get('AZURE_STORAGE_CONNECTION_STRING')
        container_name = Variable.get('AZURE_CONTAINER_NAME', 'airflow-test')
        
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        temp_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=temp_blob
        )
        
        json_data = temp_client.download_blob().readall()
        json_string = json_data.decode('utf-8')
        df = pd.read_json(json_string, orient='records')
        
        logger.info(f"Downloaded processed data with shape: {df.shape}")
        
        
        timestamp = datetime.now().strftime('%H%M')
        final_blob = f"posts/processed_posts_postgres_{context['ds_nodash']}_{timestamp}.csv"
        final_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=final_blob
        )
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        final_client.upload_blob(csv_data, overwrite=True)
        
        
        temp_client.delete_blob()
        
        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{final_blob}"
        logger.info(f"Successfully uploaded {len(df)} processed posts to: {blob_url}")
        
        return blob_url
        
    except Exception as e:
        logger.error(f"Error in upload_to_azure: {str(e)}")
        raise

with DAG(
    'postgres_to_azure_wPreP_v1',
    default_args=default_args,
    description='PostgreSQL Posts pipeline with complex preprocessing and Azure storage',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['postgres', 'preprocessing', 'azure', 'complex'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_posts_from_postgres',
        python_callable=fetch_posts_from_postgres,
    )
    
    preprocess_task = PythonOperator(
        task_id='preprocess_posts',
        python_callable=preprocess_posts,
    )
    
    upload_task = PythonOperator(
        task_id='upload_to_azure',
        python_callable=upload_to_azure,
    )

    fetch_task >> preprocess_task >> upload_task
