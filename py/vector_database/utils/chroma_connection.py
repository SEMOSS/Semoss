import chromadb
import json, base64
from chromadb import Client
from chromadb.config import Settings
from typing import List, Dict, Any, Optional


def get_collection(
    collection_name: str,
    tenant: Optional[str] = None,
    database_name: Optional[str] = None,
    api_key: Optional[str] = None,
):
    """
    This function is used to get or create a collection using the client

        Args:
            collection_name (str): The name of collection to be added.
            tenant (Optional[str]): The tenant to use for this client.
            database_name (Optional[str]): The database to use for this client.
            api_key (Optional[str]): The api key to use for this client.

        Returns:
            Collection that is created or fetched using the collection name.

        Raises:
            Exception: If we are unable to create or get the collection using the collection name.
    """
    try:
        client = get_client(
            tenant=tenant,
            database_name=database_name,
            api_key=api_key,
        )
        return client.get_or_create_collection(collection_name)
    except Exception as e:
        raise Exception(f"Unable to get or create collection: {e}")


def get_client(
    tenant: Optional[str] = None,
    database_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Client:
    """
    This function is used to create a client instance using tenant, apiKey and databaseName

        Args:
            tenant (Optional[str]): The tenant to use for this client.
            database_name (Optional[str]): The database to use for this client.
            api_key (Optional[str]): The api key to use for this client.

        Returns:
            Client: The client instance created using tenant, apiKey and database name.

        Raises:
            Exception: If we are unable to connect to the chroma cloud client.
    """
    try:
        return chromadb.CloudClient(
            tenant=tenant,
            database=database_name,
            api_key=api_key
        )
    except Exception as e:
        raise Exception(f"Unable to connect to the chroma cloud client: {e}")


def create_collection(
    collection_name: str,
    tenant: Optional[str] = None,
    database_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> str:
    """
    This function is used to get the collection id

        Args:
            collection_name (str): The name of collection to be added.
            tenant (Optional[str]): The tenant to use for this client.
            database_name (Optional[str]): The database to use for this client.
            api_key (Optional[str]): The api key to use for this client.

        Returns:
            str: The id of the collection which is created or fetched.

        Raises:
            Exception: If unable to get the collection id.
    """
    try:
        collection = get_collection(
            collection_name=collection_name,
            tenant=tenant,
            database_name=database_name,
            api_key=api_key,
        )
        return collection.id
    except Exception as e:
        raise Exception(f"Unable to get the collection id")


def add_document_collection(
    tenant: str,
    database_name: str,
    api_key: str,
    collection_name: str,
    idsJson: str,
    embeddingsJson: Optional[str] = None,
    jsonMetadatas: Optional[str] = None,
    documentJson: Optional[str] = None
) -> str:
    """
    This function is used to add a document to the collection

        Args:
            collection_name (str): The name of collection to be added.
            tenant (Optional[str]): The tenant to use for this client.
            database_name (Optional[str]): The database to use for this client.
            api_key (Optional[str]): The api key to use for this client.
            idsJson (str): The ids of the embeddings you wish to add.
            embeddingsJson (Optional[str]): The embeddings to add. If None, embeddings will be computed based on the documents using the embedding_function set for the Collection.
            jsonMetadatas (Optional[str]): The metadata to associate with the embeddings. When querying, you can filter on this metadata..
            documentJson (Optional[str]): The documents to associate with the embeddings.

        Returns:
            str: A success reponse if the docuement is added successfully to the collection.

        Raises:
            Exception: If unable to add the document to the collection in the database.
    """
    try:
        collection = get_collection(
            collection_name=collection_name,
            tenant=tenant,
            database_name=database_name,
            api_key=api_key,
        )
        ids = json.loads(idsJson) if idsJson else []
        embeddings = json.loads(embeddingsJson) if embeddingsJson else None
        metadatas = json.loads(jsonMetadatas) if jsonMetadatas else None
        documents = json.loads(documentJson) if documentJson else None
        collection.add(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )
        response = f"Added document with id {ids} successfully to the collection"
        return response
    except Exception as e:
        raise Exception(f"Unable to add document to the collection: {e}")


def delete_document_collection(
    tenant: str,
    database_name: str,
    api_key: str,
    collection_name: str,
    jsonWhere: Optional[str] = None
) -> str:
    """
    This function is used to delete a document from the collection

        Args:
            collection_name (str): The name of collection to be added.
            tenant (Optional[str]): The tenant to use for this client.
            database_name (Optional[str]): The database to use for this client.
            api_key (Optional[str]): The api key to use for this client.
            jsonWhere (Optional[str]): A Where type dict used to filter the deletion by.

        Returns:
            str: A success response if the document is removed successfully from the collection.

        Raises:
            Exception: If unable to remove the document from the collection using the where condition.
    """
    try:
        collection = get_collection(
            collection_name=collection_name,
            tenant=tenant,
            database_name=database_name,
            api_key=api_key,
        )
        where = json.loads(jsonWhere) if jsonWhere else None
        collection.delete(
            where=where
        )
        response = "Document removed successfully from the collection"
        return response
    except Exception as e:
        raise Exception(f"Unable to delete document: {e}")
        

def search_document_collection(
    tenant: str,
    database_name: str,
    api_key: str,
    collection_name: str,
    queryEmbeddingJson: str,
    n_results: Optional[int] = 10,
) -> str:
    """
    This function is used to search from the document using the search query

        Args:
            collection_name (str): The name of collection to be added.
            tenant (Optional[str]): The tenant to use for this client.
            database_name (Optional[str]): The database to use for this client.
            api_key (Optional[str]): The api key to use for this client.
            queryEmbeddingJson (str): The embeddings to get the closest neighbors of.
            n_results (Optional[str]): The number of neighbors to return for each query_embedding or query_texts.

        Returns:
            str: Returns an encoded string of the dict response received using the search query.

        Raises:
            Exception: If unable to search the document using the query embeddings.
    """
    try:
        collection = get_collection(
            collection_name=collection_name,
            tenant=tenant,
            database_name=database_name,
            api_key=api_key,
        )
        query_embeddings = json.loads(queryEmbeddingJson) if queryEmbeddingJson else None
        results = collection.query(
            query_embeddings=query_embeddings,
            n_results=n_results
        )
        return base64.b64encode(json.dumps(results).encode("utf-8")).decode("utf-8")
    except Exception as e:
        raise Exception(f"Unable to query document: {e}")
