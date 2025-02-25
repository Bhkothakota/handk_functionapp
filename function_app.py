import io
import math
import azure.functions as func
import logging
import json
import os
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient,generate_blob_sas, BlobSasPermissions
from azure.storage.queue import  BinaryBase64EncodePolicy, BinaryBase64DecodePolicy, QueueClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from unstructured.partition.text import partition_text
from unstructured.partition.ppt import partition_ppt
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.csv import partition_csv
from unstructured.partition.xlsx import partition_xlsx
from unstructured.partition.ppt import partition_ppt
from unstructured.partition.xml import partition_xml
from unstructured.partition.docx import partition_docx
from langchain.text_splitter import RecursiveCharacterTextSplitter
from azure.search.documents import SearchClient
from azure.search.documents.indexes.models import SearchIndex
import openai
from openai import OpenAI,AzureOpenAI
import urllib.parse
from datetime import datetime, time,timedelta
# import nltk
# from transformers import pipeline


app = func.FunctionApp()

SEARCHAPIENDPOINT = os.getenv("SEARCHAPIENDPOINT")
SEARCHAPIKEY = os.getenv("SEARCHAPIKEY")
CHUNK_URL = os.getenv("CHUNK_URL")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
BLOB_CONTAINER_NAME=os.getenv("BLOB_CONTAINER_NAME")
AZURE_STORAGE_CONNECTION_STRING=os.getenv("AZURE_STORAGE_CONNECTION_STRING")
QUEUE_NAME = os.getenv("QUEUE_NAME")
CHUNK_QUEUE_NAME = os.getenv("CHUNK_QUEUE_NAME")
# Azure Document Intelligence credentials
FORM_RECOGNIZER_ENDPOINT = os.getenv("FORM_RECOGNIZER_ENDPOINT")
FORM_RECOGNIZER_KEY = os.getenv("FORM_RECOGNIZER_KEY")

#openai.api_key = os.getenv("openai_api_key")
client = OpenAI(api_key=os.getenv("openai_api_key"))
client = AzureOpenAI(
  api_key = AZURE_OPENAI_API_KEY,  
  api_version = "2024-02-01",
  azure_endpoint =AZURE_OPENAI_ENDPOINT 
)
content = ""

@app.blob_trigger(arg_name="myblob", path="handkblobstorage/{name}",
                               connection="AzureWebJobsStorage") 
def blobtrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=myblob.name)  
   
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
   
    
    queue_client = QueueClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING,
    QUEUE_NAME
    )
  
    queue_client._message_encode_policy = BinaryBase64EncodePolicy()
    queue_client._message_decode_policy = BinaryBase64DecodePolicy()
    substring = "metadata"
    urlArr = urllib.parse.unquote(blob_client.url).split('/')
    if not substring in urlArr[5]:
      # List all blobs in the container and filter by name
        bfileName,ext = os.path.splitext(myblob.name)
        bfileNameArr = bfileName.split('$')
    
        matching_blobs = []
        for b in container_client.list_blobs():
            if bfileNameArr[1] in b.name:
                matching_blobs.append(b.name)
                


        blob_client_metadata = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=matching_blobs[1])
        metaData = blob_client_metadata.download_blob().readall()
        json_meta_data = json.loads(metaData)
        

        if(myblob):
            #blob_url = blob_client.url
            blob_url={
                'url': blob_client.url,
                'metadata':json_meta_data
            }    
            
            print(f"Blob uploaded: {blob_url}")
            message_string = json.dumps(blob_url)
            message_bytes = message_string.encode('utf-8')
        
        try:
            queue_client.send_message(message_bytes)  
            # queue_client._message_encode_policy.encode(content =  message_bytes)     
        except Exception as e:
            print(f"Exception Occured inside blob trigger function : {e}")



@app.queue_trigger(arg_name="azqueue", queue_name="blobqueue",
                               connection="AzureWebJobsStorage") 
def blobqueue(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s',
                azqueue.get_body().decode('utf-8'))
    
    blob_url = azqueue.get_body().decode("utf-8")
    MAX_SERVICE_BUS_SIZE = 64 * 1024
    
    data = json.loads(blob_url)
    file_path = data["url"]
    file_metadata = data["metadata"]

    
    blob_name = os.path.basename(file_path)
    blob_name = urllib.parse.unquote(blob_name)
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
 
   # Get blob properties
    properties = blob_client.get_blob_properties()
    content_type = properties.content_settings.content_type
    
    document_client = DocumentAnalysisClient(FORM_RECOGNIZER_ENDPOINT, AzureKeyCredential(FORM_RECOGNIZER_KEY))
    blob_data = blob_client.download_blob().readall()
    

    if(content_type == "application/pdf"):
        try:
            content = ""
            poller = document_client.begin_analyze_document("prebuilt-layout", blob_data)
            result = poller.result()
            for page in result.pages:
                for line in page.lines:            
                    content += line.content +  " "
        except Exception as ex:
            print("Exception occured for pdf ",ex)

    if(content_type == "image/jpeg" or content_type == "image/png" or content_type == "image/gif"):
        try:
            blob_data = blob_client.download_blob().readall()
            content = ""
            poller = document_client.begin_analyze_document("prebuilt-layout", blob_data)
            result = poller.result()
            for page in result.pages:
                for line in page.lines:            
                    content += line.content +  " "
        except Exception as ex:
            print("Exception occured for images ",ex)

    if(content_type == "text/plain"):
        blob_data = blob_client.download_blob().readall()
        content = ""
        
        if blob_name.endswith(".txt"):
            elements = partition_text(file=blob_data)
            content = "\n".join(str(el) for el in elements)
        if blob_name.endswith(".docx"):
            elements = partition_docx(file=blob_data)
            content =  "\n".join([str(el) for el in elements])
    
    
    if(content_type == "application/vnd.ms-powerpoint" or content_type == "application/vnd.openxmlformats-officedocument.presentationml.presentation"):
        blob_data = blob_client.download_blob().readall()
        content = ""
        if blob_name.endswith(".ppt"):
            elements = partition_ppt(file=blob_data)
            content = "\n".join(str(el) for el in elements)
        if blob_name.endswith(".pptx"):
            elements = partition_pptx(file=blob_data)
            content = "\n".join(str(el) for el in elements)

    if(content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" or content_type == "text/csv"):
        blob_data = blob_client.download_blob().readall()
        content = ""
        if blob_name.endswith(".csv"):
            elements = partition_csv(file=blob_data)
            content = "\n".join(str(el) for el in elements)
        if blob_name.endswith(".xlsx"):
            elements = partition_xlsx(file=blob_data)
            content = "\n".join(str(el) for el in elements)

    if(content_type == "application/xml"):
        blob_data = blob_client.download_blob().readall()
        content = ""
        elements = partition_xml(file=blob_data)
        content = "\n".join(str(el) for el in elements)

    data = {
        "content" : content,
        "blobmetadata" : file_metadata
    }
    data_content = json.dumps(data)
  
    queue_client_chunk = QueueClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING,
    CHUNK_QUEUE_NAME
    )
    queue_client_chunk._message_encode_policy = BinaryBase64EncodePolicy()
    queue_client_chunk._message_decode_policy = BinaryBase64DecodePolicy()

    message_string = json.dumps(data)
    message_bytes = message_string.encode('utf-8')
    try:
        if(len(message_bytes) > MAX_SERVICE_BUS_SIZE):

            upload_large_document(file_metadata["fileGUID"], message_bytes,file_metadata,data_content)
            # response = chunk_text(text = content)
           
            # for chunk in response:  
            #     embedd_response = client.embeddings.create(input = chunk,model= "text-embedding-ada-002").data[0].embedding
        
            # search_client = SearchClient(endpoint=SEARCHAPIENDPOINT,index_name="aiindex",credential = AzureKeyCredential(SEARCHAPIKEY))
            # document = {
            #     "chunk_id": file_metadata["fileGUID"],
            #     "parent_id":file_metadata["fileGUID"],
            #     "content": content,
            #     "title": file_metadata["DisplayName"],
            #     "url": file_metadata["FileLink"],
            #     "filepath":file_metadata["Path"],
            #     "vector": embedd_response,
            #     "doc": file_metadata["Modified"]
            # }
            # try:
            # # Upload the document to the index
            #     result = search_client.upload_documents(documents=[document])
            #     print(f"Upload result: {result}")
            # except Exception as e:
            #     print(f"Exception occured : ",ex)

        else:
            queue_client_chunk.send_message(message_bytes)

    except Exception as ex :
        print("Exception occured : ", ex)



@app.queue_trigger(arg_name="azqueue", queue_name="chunckqueuetrigger",
                               connection="AzureWebJobsStorage") 
def chunckqueuetrigger(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s',
                azqueue.get_body().decode('utf-8'))
    
    blob_obj = azqueue.get_body().decode('utf-8')
    data = json.loads(blob_obj)
    content = data["content"]
    blobmetadata = data["blobmetadata"]
    response = chunk_text(text = content)
   # response_dynamic_chunk = dynamic_chunk_text(text = content)
    for chunk in response:  
        embedd_response = client.embeddings.create(input = chunk,model= "text-embedding-ada-002").data[0].embedding
  
    search_client = SearchClient(endpoint=SEARCHAPIENDPOINT,index_name="aiindex",credential = AzureKeyCredential(SEARCHAPIKEY))

    # Set up the client
   
    #search_index_client = SearchIndexClient(endpoint=SEARCHAPIENDPOINT,credential = AzureKeyCredential(SEARCHAPIKEY))

    # Define the scoring profile
    # scoring_profile = ScoringProfile(
    #     name="vector-similarity",
    #     functions=[
    #         VectorScoringFunction(
    #             field_name="vectorField",
    #             boost=5,
    #             parameters=VectorScoringParameters(
    #             similarity_metric="cosine"
    #             )
    #         )
    #     ]
    # )

    # index = SearchIndex(
    #     name="documents",
    #     fields=[
    #             blobmetadata["fileGUID"],
    #             blobmetadata["fileGUID"],
    #             content,
    #                 blobmetadata["DisplayName"],
    #             blobmetadata["FileLink"],
    #             blobmetadata["Path"],
    #             embedd_response,
    #             blobmetadata["Modified"]
    #         ],
    #     scoring_profiles=[scoring_profile]
    # )

    # client.create_or_update_index(index)
    
    document = {
        "chunk_id": blobmetadata["fileGUID"],
        "parent_id":blobmetadata["fileGUID"],
        "content": content,
        "title": blobmetadata["DisplayName"],
        "url": blobmetadata["FileLink"],
        "filepath":blobmetadata["Path"],
        "vector": embedd_response,
        "doc": blobmetadata["Modified"]
    }

    # Upload the document to the index
    result = search_client.upload_documents(documents=[document])
    print(f"Upload result: {result}")

def chunk_text(text: str, chunk_size: int = 1000, chunk_overlap: int = 200):
    """Chunk the text into smaller chunks using RecursiveCharacterTextSplitter."""
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks = text_splitter.split_text(text)
    return chunks

def upload_large_data(data_bytes,blob_name):
    CONTAINER_NAME = "largefiles"
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    """Uploads large data to Blob Storage and returns the blob name."""
    
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, blob_name)

    blob_client.upload_blob(io.BytesIO(data_bytes), overwrite=True)
    print(f"Uploaded {blob_name} to Blob Storage.")

    return blob_name  # Return blob reference





# def dynamic_chunk_text(text, chunk_size=1000, overlap_size=200):
#     summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
#     sentences = nltk.sent_tokenize(text)
#     chunks = []
#     current_chunk = ""
#     for sentence in sentences:
#         # If adding this sentence exceeds chunk size, finalize current chunk
#         if len(current_chunk) + len(sentence) > chunk_size:
#             chunks.append(current_chunk)
#             current_chunk = sentence  # Start a new chunk with the current sentence
#         else:
#             # Otherwise, add the sentence to the current chunk
#             current_chunk += " " + sentence
#     # Add the last chunk
#     if current_chunk:
#         chunks.append(current_chunk)

#     # Handle overlap between chunks (optional, useful for context retention)
#     chunk_with_overlap = []
#     for i in range(len(chunks) - 1):
#         chunk_with_overlap.append(chunks[i] + " " + chunks[i + 1][:overlap_size])

#     # Add the last chunk without any overlap
#     chunk_with_overlap.append(chunks[-1])
    
#     return chunk_with_overlap



def split_large_text(text, max_size=32000):
    """Splits large text content into smaller chunks."""
    total_size = len(text) 
    num_parts = math.ceil(total_size / max_size)  # Calculate needed parts

    chunks = []
    chunk_size = len(text) // num_parts  # Split evenly
    for i in range(num_parts):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < num_parts - 1 else len(text)
        chunks.append(text[start:end])
    
    return chunks

def upload_large_document(doc_id, content,file_metadata,data_content):
    """Splits large document & uploads chunks separately to Azure AI Search."""
    data = json.loads(data_content)
    search_client = SearchClient(endpoint=SEARCHAPIENDPOINT,index_name="aiindex",credential = AzureKeyCredential(SEARCHAPIKEY))
    chunks = split_large_text(content)
    response = chunk_text(text = data["content"])
    for chunk in response:  
            embedd_response = client.embeddings.create(input = chunk,model= "text-embedding-ada-002").data[0].embedding
    
    documents = []
    for i, chunk in enumerate(chunks):
        documents.append({
            "chunk_id": f"{doc_id}-chunk-{i+1}",  # Unique ID per chunk
            "parent_id": doc_id,  # Original document ID
            # "chunk_number": i + 1,
            "content": chunk,  # Chunked content
            "title": file_metadata["DisplayName"],
            "url": file_metadata["FileLink"],
            "filepath":file_metadata["Path"],
            "vector": embedd_response,
            "doc": file_metadata["Modified"]        
               
        })
    cleaned_documents = [clean_and_encode(doc) for doc in documents]


    #  Upload chunks in batches (Azure allows up to 1000 documents per request)
    batch_size = 10  # Adjust batch size as needed
    for i in range(0, len(cleaned_documents), batch_size):
        batch = cleaned_documents[i:i + batch_size]   
        if not batch:  # ✅ Avoid empty batch error
            continue  
        response = search_client.upload_documents(batch)
        print(f" Uploaded {len(batch)} chunks for {doc_id}")



def clean_and_encode(doc):
    """Clean null values and ensure UTF-8 encoding."""
    return {k: (v.encode("utf-8").decode("utf-8") if isinstance(v, str) else v) if v is not None else "" for k, v in doc.items()}

