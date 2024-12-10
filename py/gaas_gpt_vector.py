from typing import List, Tuple, Dict, Optional
import os
import zipfile
import shutil

from gaas_server_proxy import ServerProxy


class VectorEngine(ServerProxy):
    engine_type = "VECTOR"

    def __init__(
        self,
        insight_folder: str,
        engine_id: str,
        insight_id: Optional[str] = None,
    ):
        assert engine_id is not None
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        self.insight_folder = insight_folder
        print(f"Vector Engine {engine_id} is initialized")

    def addDocument(
        self,
        file_paths: List[str],
        param_dict: Optional[Dict] = {},
        insight_id: Optional[str] = None,
        space: Optional[str] = None,
    ) -> bool:
        """
        Add the documents into the vector database

        Args:
            file_paths (`List[str]`):  The paths (relative to the insight_id) of the files to add
            param_dict (`dict`): A dictionary with optional parameters for listing the documents (index class for FAISS as an example)
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
        """
        assert file_paths is not None
        if insight_id is None:
            insight_id = self.insight_id

        optionalParams = (
            f",paramValues=[{param_dict}]" if param_dict is not None else ""
        )

        optionalSpace = (
            f",space=['{space}']" if (space is not None and space != "") else ""
        )

        pixel = f'CreateEmbeddingsFromDocuments(engine="{self.engine_id}",filePaths={file_paths}{optionalParams}{optionalSpace});'
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
        param_dict: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> bool:
        """
        Add the vector csv file format documents into the vector database

        Args:
            file_paths (`List[str]`):  The paths (relative to the insight_id) of the files to add
            param_dict (`dict`): A dictionary with optional parameters for listing the documents (index class for FAISS as an example)
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
        """
        assert file_paths is not None
        if insight_id is None:
            insight_id = self.insight_id

        optionalParams = (
            f",paramValues=[{param_dict}]" if param_dict is not None else ""
        )

        pixel = f'CreateEmbeddingsFromVectorCSVFile(engine="{self.engine_id}",filePaths={file_paths}{optionalParams});'
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
            f",paramValues=[{param_dict}]" if param_dict is not None else ""
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
            f",paramValues=[{param_dict}]" if param_dict is not None else ""
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

    def _determine_ids(
        self, engine_id: Optional[str], insight_id: Optional[str]
    ) -> Tuple[str, str]:
        if engine_id is None:
            engine_id = self.engine_id

        if insight_id is None:
            insight_id = self.insight_id

        assert engine_id is not None
        assert insight_id is not None

        return engine_id, insight_id

    def get_files(self, file_paths: List[str]) -> List[str]:
        valid_files = []

        for file_path in file_paths:
            # Update file_path to include the insight folder path
            if not os.path.isfile(file_path):
                # should never start a path with a /
                if file_path[0] == "/":
                    file_path = file_path[1:]

                updated_file_path = os.path.join(self.insight_folder, file_path)
                if os.path.isfile(updated_file_path):
                    file_path = updated_file_path
                else:
                    raise IOError(f"Unable to find file path for {file_path}")

            if self._is_zip_file(file_path):
                valid_files_in_zip = self._unzip_and_filter(
                    file_path, os.path.splitext(file_path)[0]
                )
                valid_files.extend(valid_files_in_zip)
            elif self._is_supported_file_type(file_path):
                valid_files.append(file_path)

        return valid_files

    @staticmethod
    def _unzip_and_filter(zip_file_path, dest_directory) -> List[str]:
        valid_file_paths = []

        def extract_file(z, entry, file_path):
            dir_path = os.path.dirname(file_path)
            os.makedirs(dir_path, exist_ok=True)
            with open(file_path, "wb") as f:
                shutil.copyfileobj(z.open(entry), f)

        with zipfile.ZipFile(zip_file_path, "r") as z:
            for entry in z.namelist():
                file_path = os.path.join(dest_directory, entry)
                if not entry.endswith("/") and VectorEngine._is_supported_file_type(
                    file_path
                ):
                    extract_file(z, entry, file_path)
                    valid_file_paths.append(file_path)
                elif entry.endswith("/"):
                    os.makedirs(file_path, exist_ok=True)
                elif VectorEngine._is_zip_file(file_path):
                    extract_file(z, entry, file_path)
                    parent_path = os.path.dirname(file_path)
                    base_name = os.path.splitext(os.path.basename(file_path))[0]
                    nested_dest_directory = os.path.join(parent_path, base_name)
                    nested_valid_paths = VectorEngine._unzip_and_filter(
                        file_path, nested_dest_directory
                    )
                    valid_file_paths.extend(nested_valid_paths)

        return valid_file_paths

    @staticmethod
    def _is_supported_file_type(file_path):
        return file_path.split(".")[-1].lower() in {
            "pdf",
            "pptx",
            "ppt",
            "doc",
            "docx",
            "txt",
            "csv",
        }

    @staticmethod
    def _is_zip_file(file_path):
        return file_path.split(".")[-1].lower() == "zip"
