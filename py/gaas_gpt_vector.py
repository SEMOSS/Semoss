from typing import List, Tuple, Dict, Optional
import json
from gaas_server_proxy import ServerProxy


class VectorEngine(ServerProxy):
    def __init__(
        self,
        engine_id: str = None,
        insight_id: Optional[str] = None,
        # we do not use this anymore
        insight_folder: Optional[str] = None,
    ):
        assert engine_id is not None
        super().__init__()
        self.engine_id = engine_id
        if insight_id is None:
            insight_id = super().get_thread_insight_id()
        self.insight_id = insight_id
        # we do not use this anymore
        self.insight_folder = insight_folder
        print(f"Vector Engine {engine_id} is initialized")

    def addDocument(
        self,
        file_paths: List[str],
        space: Optional[str] = None,
        param_dict: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> bool:
        """
        Add the documents into the vector database

        Args:
            file_paths (`List[str]`):  The paths (relative to the insight_id) of the files to add
            param_dict (`dict`): A dictionary with optional parameters for listing the documents (index class for FAISS as an example)
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
            space (`Optional[str]`): If the files being loaded are in an app and not the insight, provide space='appid'
        """
        assert file_paths is not None
        if insight_id is None:
            insight_id = self.insight_id

        optionalParams = (
            f",paramValues=[{json.dumps(param_dict)}]" if param_dict is not None else ""
        )

        optionalSpace = (
            f",space=['{space}']" if (space is not None and space != "") else ""
        )

        pixel = f'CreateEmbeddingsFromDocuments(engine="{self.engine_id}",filePaths={file_paths}{optionalSpace}{optionalParams});'
        epoc = super().get_next_epoc()

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def addVectorCSVFile(
        self,
        file_paths: List[str],
        space: Optional[str] = None,
        param_dict: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> bool:
        """
        Add the vector csv file format documents into the vector database

        Args:
            file_paths (`List[str]`):  The paths (relative to the insight_id) of the files to add
            param_dict (`dict`): A dictionary with optional parameters for listing the documents (index class for FAISS as an example)
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
            space (`Optional[str]`): If the files being loaded are in an app and not the insight, provide space='appid'
        """
        assert file_paths is not None
        if insight_id is None:
            insight_id = self.insight_id

        optionalParams = (
            f",paramValues=[{json.dumps(param_dict)}]" if param_dict is not None else ""
        )

        optionalSpace = (
            f",space=['{space}']" if (space is not None and space != "") else ""
        )

        pixel = f'CreateEmbeddingsFromVectorCSVFile(engine="{self.engine_id}",filePaths={file_paths}{optionalSpace}{optionalParams});'
        epoc = super().get_next_epoc()

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def removeDocument(
        self,
        file_names: List[str],
        param_dict: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> bool:
        """
        Remove the documents from the vector database

        Args:
            file_names (`List[str]`):  The names of the files to remove
            param_dict (`dict`): A dictionary with optional parameters for listing the documents (index class for FAISS as an example)
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
        """
        assert file_names is not None
        if insight_id is None:
            insight_id = self.insight_id

        optionalParams = (
            f",paramValues=[{json.dumps(param_dict)}]" if param_dict is not None else ""
        )

        pixel = f'RemoveDocumentFromVectorDatabase(engine="{self.engine_id}",fileNames={file_names}{optionalParams});'
        epoc = super().get_next_epoc()

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def nearestNeighbor(
        self,
        search_statement: str,
        limit: Optional[int] = 5,
        filters: Optional[Dict] | Optional[str] = None,
        filters_str: Optional[str] = None,
        metafilters: Optional[Dict] | Optional[str] = None,
        metafilters_str: Optional[str] = None,
        param_dict: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Find the most relevant values in the vector database based on a search statement

        Args:
            search_statement (`str`):  The value being compared against the vector database embeddings
            limit (`Optional[int]`): Limit for the number of records to return
            param_dict (`dict`): A dictionary with optional parameters for nearest neighbor calculation
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
        """
        assert search_statement is not None
        search_statement = search_statement.strip()
        assert search_statement != ""

        if insight_id is None:
            insight_id = self.insight_id

        # Building limits param
        optionalLimit = f",limit=[{limit}]" if (limit is not None and limit > 0) else ""

        optional_filters = ""

        # 1. Check if filters_str parameter is provided (if so use this)
        # 2. If not, check if filters parameter is provided and check if it is a string (if so use this)
        # 3. If not, check if filters parameter is provided and check if it is a dictionary (if so build the string)
        if filters_str is not None:
            optional_filters = f",filters=[{filters_str}]"
        if filters is not None and optional_filters == "":
            if isinstance(filters, str):
                optional_filters = f",filters=[{filters}]"
            elif isinstance(filters, dict):
                filter_conditions = []
                for key, value in filters.items():
                    formatted_key = key.capitalize()
                    if isinstance(value, str):
                        formatted_values = f'"{value}"'
                    else:
                        formatted_values = ", ".join([f'"{v}"' for v in value])
                    filter_conditions.append(f"{formatted_key} == [{formatted_values}]")

                optional_filters = (
                    f",filters = [ Filter({', '.join(filter_conditions)})]"
                    if filter_conditions
                    else ""
                )
            else:
                raise ValueError(
                    "Invalid filters type. Filter must be string or dictionary"
                )

        # 1. Check if metafilters_str parameter is provided (if so use this)
        # 2. If not, check if metafilters parameter is provided and check if it is a string (if so use this)
        # 3. If not, check if metafilters parameter is provided and check if it is a dictionary (if so build the string)
        optional_meta_filters = ""
        if metafilters_str is not None:
            optional_meta_filters = f",metaFilters=[{metafilters_str}]"
        if metafilters is not None and optional_meta_filters == "":
            if isinstance(metafilters, str):
                optional_meta_filters = f",metaFilters=[{metafilters}]"
            elif isinstance(metafilters, dict):
                metafilter_conditions = []
                for key, value in metafilters.items():
                    # formatted_key = key.capitalize() # Assuming the user has passed the key in the correct format
                    if isinstance(value, str):
                        formatted_values = f'"{value}"'
                    else:
                        formatted_values = ", ".join([f'"{v}"' for v in value])
                    metafilter_conditions.append(
                        f"{formatted_key} == [{formatted_values}]"
                    )

                optional_meta_filters = (
                    f",metaFilters=[ Filter({', '.join(metafilter_conditions)})]"
                    if metafilter_conditions
                    else ""
                )
            else:
                raise ValueError(
                    "Invalid metafilters type. Metafilters must be string or dictionary"
                )

        # Building the rest of the optional parameters in paramValues
        optionalParams = (
            f",paramValues=[{param_dict}]" if param_dict is not None else ""
        )

        pixel = f'VectorDatabaseQuery(engine="{self.engine_id}",command=["<encode>{search_statement}</encode>"]{optionalLimit}{optional_filters}{optional_meta_filters}{optionalParams});'
        epoc = super().get_next_epoc()

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def listDocuments(
        self,
        param_dict: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        List the documents in the vector database

        Args:
            param_dict (`dict`): A dictionary with optional parameters for listing the documents (index class for FAISS as an example)
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
        """
        if insight_id is None:
            insight_id = self.insight_id

        optionalParams = (
            f",paramValues=[{json.dumps(param_dict)}]" if param_dict is not None else ""
        )

        pixel = (
            f'ListDocumentsInVectorDatabase(engine="{self.engine_id}"{optionalParams});'
        )
        epoc = super().get_next_epoc()

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def to_langchain_vector_store(self):
        """Transform the vector engine into a langchain BaseRetriever object so that it can be used with langchain code."""
        from langchain_core.callbacks import CallbackManagerForRetrieverRun
        from langchain_core.documents import Document
        from langchain_core.retrievers import BaseRetriever

        class SemossLangchainVector(BaseRetriever):
            engine_id: str
            vector_engine: VectorEngine
            insight_id: Optional[str]

            def __init__(self, vector_engine):
                """Initialize with the provided vector engine."""
                data = {
                    "engine_id": vector_engine.engine_id,
                    "insight_id": vector_engine.insight_id,
                    "vector_engine": vector_engine,
                }
                super().__init__(**data)

            class Config:
                """Configuration for this pydantic object."""

                allow_population_by_field_name = True

            def addDocs(self, file_paths: List[str]) -> None:
                """Add documents to the vector store."""
                self.vector_engine.addDocument(
                    file_paths=file_paths, insight_id=self.insight_id
                )

            def removeDocs(self, file_names: List[str]) -> None:
                """Remove documents from the vector store."""
                return self.vector_engine.removeDocument(
                    file_names=file_names, insight_id=self.insight_id
                )

            def similaritySearch(self, query: str, k: int) -> List[Document]:
                """Search for documents similar to the query."""
                results = self.vector_engine.nearestNeighbor(
                    search_statement=query, limit=k, insight_id=self.insight_id
                )

                documents = [
                    Document(page_content=result["Content"], metadata=result)
                    for result in results
                ]
                return documents

            def listDocs(self):
                """List the documents in the vector store"""
                return self.vector_engine.listDocuments()

            def _get_relevant_documents(
                self, query: str, *, run_manager: CallbackManagerForRetrieverRun, k: int
            ) -> List[Document]:
                """Retrieve relevant documents based on the query."""
                return self.similarity_search(query, k)

        return SemossLangchainVector(vector_engine=self)
