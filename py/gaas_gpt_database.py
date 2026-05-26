from gaas_server_proxy import ServerProxy
from typing import Optional


class DatabaseEngine(ServerProxy):
    def __init__(
        self,
        engine_id: str = None,
        insight_id: Optional[str] = None,
    ):
        assert engine_id is not None
        super().__init__()
        self.engine_id = engine_id
        if insight_id is None:
            insight_id = super().get_thread_insight_id()
        self.insight_id = insight_id
        print(f"Database Engine {engine_id} is initialized")

    def execQuery(
        self, query=None, insight_id: Optional[str] = None, return_pandas=True
    ):
        assert query is not None
        if insight_id is None:
            insight_id = self.insight_id
        # assert insight_id is not None
        epoc = super().get_next_epoc()

        fileLoc = super().callEngine(
            epoc=epoc,
            engine_type="database",
            engine_id=self.engine_id,
            insight_id=insight_id,
            method_name="execQuery",
            method_args=[query],
            method_arg_types=["java.lang.String"],
        )
        if isinstance(fileLoc, list) and len(fileLoc) > 0:
            fileLoc = fileLoc[0]
        try:
            if return_pandas:
                print(f"file Location {fileLoc}")
                import pandas as pd

                return pd.read_json(fileLoc)
            else:
                return open(fileLoc, "r").read()
        finally:
            # Always attempt to remove the file regardless of success
            import os

            if os.path.exists(fileLoc):
                os.remove(fileLoc)

    def get_database_structure(self, insight_id: Optional[str] = None):
        """
        Retrieve the modeled database structure metadata for this engine.

        Args:
            insight_id (`Optional[str]`): Unique identifier for the temporal workspace where actions are being isolated.

        Returns:
            list: Rows containing table/column metadata fields such as
            `PARENTSEMOSSNAME`, `SEMOSSNAME`, `PARENTPHYSICALNAME`,
            `PHYSICALNAME`, `PROPERTY_TYPE`, and `PK`.
        """
        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()
        pixel = f'GetDatabaseTableStructure(database = "{self.engine_id}");'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def insertData(
        self, query=None, insight_id: Optional[str] = None, commit: bool = True
    ):
        """
        This method is responsible for running a insert data into the database

        Args:
            query (`str`): The query to run against the database
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
            commit (`bool`): commit to the database if autocommit is false. default is true

        Returns:
            boolean: true/false if this ran successfully
        """
        return self.runQuery(query, insight_id, commit)

    def updateData(
        self, query=None, insight_id: Optional[str] = None, commit: bool = True
    ):
        """
        This method is responsible for running a insert data into the database

        Args:
            query (`str`): The query to run against the database
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
            commit (`bool`): commit to the database if autocommit is false. default is true

        Returns:
            boolean: true/false if this ran successfully
        """
        return self.runQuery(query, insight_id, commit)

    def removeData(
        self, query=None, insight_id: Optional[str] = None, commit: bool = True
    ):
        """
        This method is responsible for removing data from the database

        Args:
            query (`str`): The query to run against the database
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
            commit (`bool`): commit to the database if autocommit is false. default is true

        Returns:
            boolean: true/false if this ran successfully
        """
        return self.runQuery(query, insight_id, commit)

    def runQuery(
        self, query=None, insight_id: Optional[str] = None, commit: bool = True
    ):
        """
        This method is responsible for running the exec query against the database

        Args:
            query (`str`): The query to run against the database
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
            commit (`bool`): commit to the database if autocommit is false. default is true

        Returns:
            boolean: true/false if this ran successfully
        """
        assert query is not None
        if insight_id is None:
            insight_id = self.insight_id

        commitStr = "true" if commit else "false"

        # assert insight_id is not None
        epoc = super().get_next_epoc()

        pixel = f'Database("{self.engine_id}")|Query("<encode>{query}</encode>")|ExecQuery(commit={commitStr});'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def to_langchain_database(self):
        """Transform the database engine into a langchain BaseRetriever object so that it can be used with langchain code"""
        from langchain_core.retrievers import BaseRetriever

        class SemossLangchainDatabase(BaseRetriever):
            engine_id: str
            database_engine: DatabaseEngine
            insight_id: Optional[str]

            def __init__(self, database_engine):
                """Initialize with the provided database engine."""
                data = {
                    "engine_id": database_engine.engine_id,
                    "insight_id": database_engine.insight_id,
                    "database_engine": database_engine,
                }
                super().__init__(**data)

            class Config:
                """Configuration for this pydantic object."""

                allow_population_by_field_name = True

            def executeQuery(self, query: str) -> any:
                """Execute a query on the database."""
                return self.database_engine.execQuery(query=query)

            def insertQuery(self, query: str) -> any:
                """Insert data into the database."""
                return self.database_engine.insertData(query=query)

            def updateQuery(self, query: str) -> any:
                """Update data in the database."""
                return self.database_engine.updateData(query=query)

            def removeQuery(self, query: str) -> any:
                """Remove data from the database."""
                return self.database_engine.removeData(query=query)

            def _get_relevant_documents(self) -> str:
                """Retrieve relevant documents from the database."""
                return "SQL Operations"

        return SemossLangchainDatabase(database_engine=self)
