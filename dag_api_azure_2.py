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

def fetch_posts(**context):
    # Fetch from DummyJSON API and save to Azure temp
    logger.info("Starting fetch_posts task")
    
    try:
        import requests
        import pandas as pd
        from datetime import datetime
        from azure.storage.blob import BlobServiceClient
        from airflow.sdk import Variable
        
        # Fetch data from DummyJSON
        url = 'https://dummyjson.com/posts?limit=100'
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Airflow-Pipeline/3.0.3'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        response_data = response.json()
        posts_data = response_data.get('posts', [])
        logger.info(f"Received {len(posts_data)} posts")

        if not posts_data:
            raise ValueError("No posts data received")

        # Flatten data
        flattened_posts = []
        for post in posts_data:
            flattened_post = {
                'id': post.get('id', ''),
                'title': post.get('title', ''),
                'body': post.get('body', ''),
                'userId': post.get('userId', ''),
                'tags': ', '.join(post.get('tags', [])) if post.get('tags') else '',
                'reactions_likes': post.get('reactions', {}).get('likes', 0),
                'reactions_dislikes': post.get('reactions', {}).get('dislikes', 0),
                'views': post.get('views', 0)
            }
            flattened_posts.append(flattened_post)

        df = pd.DataFrame(flattened_posts)
        df['extraction_timestamp'] = datetime.now().isoformat()
        df['extraction_date'] = context['ds']
        df['source_system'] = 'DummyJSON_API'

        # Save to Azure temp folder
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
        
        logger.info(f"Saved raw data to temp folder: {temp_blob}")
        return temp_blob
        
    except Exception as e:
        logger.error(f"Error in fetch_posts: {str(e)}")
        raise

def preprocess_posts(**context):
    # Download, preprocess, and back to temp folder
    logger.info("Starting preprocess_posts task")
    
    try:
        import pandas as pd
        import re
        from azure.storage.blob import BlobServiceClient
        from airflow.sdk import Variable
        
        # Get temp blob path from previous task
        temp_blob = context['task_instance'].xcom_pull(task_ids='fetch_posts')
        
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

        # data cleaning
        df['title'] = df['title'].fillna('No Title Available')
        df['reactions_likes'] = df['reactions_likes'].clip(lower=0) 
        df['reactions_dislikes'] = df['reactions_dislikes'].clip(lower=0)
        df['views'] = df['views'].fillna(0).astype(int)
        df = df[df['body'].str.len() > 10]  # Remove very short post

        # standardization
        df['id'] = df['id'].astype(int)
        df['userId'] = df['userId'].astype(int)

        df['total_reactions'] = df['reactions_likes'] + df['reactions_dislikes']
        df['engagement_ratio'] = df['reactions_likes'] / (df['total_reactions'] + 1)
        df['word_count'] = df['body'].str.split().str.len()
        df['is_popular'] = df['reactions_likes'] > df['reactions_likes'].quantile(0.75)

        logger.info(f"Minimal preprocessing complete. Final shape: {df.shape}")

        
        processed_blob = f"temp/processed_posts_{context['ds_nodash']}.json"
        processed_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=processed_blob
        )
        
        processed_json = df.to_json(orient='records')
        processed_client.upload_blob(processed_json, overwrite=True)
        
        # Clean up
        blob_client.delete_blob()
        
        logger.info(f"Saved processed data to temp folder: {processed_blob}")
        return processed_blob
        
    except Exception as e:
        logger.error(f"Error in preprocess_posts: {str(e)}")
        raise

def upload_to_azure(**context):
    # Move processed data from temp folder to final location
    logger.info("Starting upload_to_azure task")
    
    try:
        import pandas as pd
        from azure.storage.blob import BlobServiceClient
        from io import StringIO
        from airflow.sdk import Variable
        from datetime import datetime
        
        # Get temp blob path
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
        final_blob = f"posts/processed_posts_{context['ds_nodash']}_{timestamp}.csv"
        final_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=final_blob
        )
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        final_client.upload_blob(csv_data, overwrite=True)
        
        # Clean up temp
        temp_client.delete_blob()
        
        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{final_blob}"
        logger.info(f"Successfully uploaded {len(df)} processed posts to: {blob_url}")
        
        return blob_url
        
    except Exception as e:
        logger.error(f"Error in upload_to_azure: {str(e)}")
        raise

with DAG(
    'dummyjson_to_azure_wPreP_v7',
    default_args=default_args,
    description='DummyJSON Posts pipeline with Azure temp storage',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_posts',
        python_callable=fetch_posts,
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
