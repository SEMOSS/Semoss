from gaas_server_proxy import ServerProxy


class StorageEngine(ServerProxy):
    
    def __init__(self, engine_id=None, insight_id=None):
        assert engine_id is not None
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        print("initialized")

    def list(self, storagePath=None, insight_id=None):
        """
        This method is responsible for listing the files in the storage engine

        Args:
            storagePath (`str`): The path in the storage engine to delete
            leaveFolderStructure (`bool`): If we should maintain the folder structure after deletion
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            boolean: true/false if this ran successfully
        """
        assert storagePath is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        epoc = super().get_next_epoc()
        pixel = f'Storage("{self.engine_id}")|ListStoragePath(storagePath="{storagePath}");'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def listDetails(self, storagePath=None, insight_id=None):
        """
        This method is responsible for listing the files in the storage engine

        Args:
            storagePath (`str`): The path in the storage engine to delete
            leaveFolderStructure (`bool`): If we should maintain the folder structure after deletion
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            boolean: true/false if this ran successfully
        """
        assert storagePath is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        epoc = super().get_next_epoc()
        pixel = f'Storage("{self.engine_id}")|ListStoragePathDetails(storagePath="{storagePath}");'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def syncLocalToStorage(self, storagePath=None, localPath=None, space=None, insight_id=None):
        """
        This method is responsible for syncing from insight/project/user space to cloud storage

        Args:
            storagePath (`str`): The path in the storage engine to sync into
            localPath (`str`): The path in the application to sync from (insight, project, user space)
            space (`str`): The space to use. None = current insight. Can be the project id or 'user' for the user specific space
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            boolean: true/false if this ran successfully
        """
        assert storagePath is not None
        assert localPath is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        spaceStr = f',space="{space}"' if space is not None else ""

        epoc = super().get_next_epoc()
        pixel = f'Storage("{self.engine_id}")|SyncLocalToStorage(storagePath="{storagePath}",filePath="{localPath}"{spaceStr});'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def syncStorageToLocal(self, storagePath=None, localPath=None, space=None, insight_id=None):
        """
        This method is responsible for syncing from cloud storage into the insight/project/user space

        Args:
            storagePath (`str`): The path in the storage engine to sync from
            localPath (`str`): The path in the application to sync into (insight, project, user space)
            space (`str`): The space to use. None = current insight. Can be the project id or 'user' for the user specific space
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            boolean: true/false if this ran successfully
        """
        assert storagePath is not None
        assert localPath is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        spaceStr = f',space="{space}"' if space is not None else ""

        epoc = super().get_next_epoc()
        pixel = f'Storage("{self.engine_id}")|SyncStorageToLocal(storagePath="{storagePath}",filePath="{localPath}"{spaceStr});'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn

    def copyToLocal(self, storagePath=None, localPath=None, space=None, insight_id=None):
        """
        This method is responsible for copying from cloud storage into the insight/project/user space

        Args:
            storagePath (`str`): The path in the storage engine to pull from
            localPath (`str`): The path in the application to copy into (insight, project, user space)
            space (`str`): The space to use. None = current insight. Can be the project id or 'user' for the user specific space
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            boolean: true/false if this ran successfully
        """
        assert storagePath is not None
        assert localPath is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        spaceStr = f',space="{space}"' if space is not None else ""

        epoc = super().get_next_epoc()
        pixel = f'Storage("{self.engine_id}")|PullFromStorage(storagePath="{storagePath}",filePath="{localPath}"{spaceStr});'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn
    
    def copyToStorage(self, storagePath=None, localPath=None, space=None, insight_id=None):
        """
        This method is responsible for copying from insight/project/user space to cloud storage 

        Args:
            storagePath (`str`): The path in the storage engine to push into
            localPath (`str`): The path in the application we are pushing to cloud storage (insight, project, user space)
            space (`str`): The space to use. None = current insight. Can be the project id or 'user' for the user specific space
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            boolean: true/false if this ran successfully
        """
        assert storagePath is not None
        assert localPath is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        spaceStr = f',space="{space}"' if space is not None else ""

        epoc = super().get_next_epoc()
        pixel = f'Storage("{self.engine_id}")|PushToStorage(storagePath="{storagePath}",filePath="{localPath}"{spaceStr});'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn
    

    def deleteFromStorage(self, storagePath=None, leaveFolderStructure: bool = False, insight_id=None):
        """
        This method is responsible for deleting from a storage

        Args:
            storagePath (`str`): The path in the storage engine to delete
            leaveFolderStructure (`bool`): If we should maintain the folder structure after deletion
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            boolean: true/false if this ran successfully
        """
        assert storagePath is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        leaveFolderStructureStr = "true" if leaveFolderStructure else "false"

        epoc = super().get_next_epoc()
        pixel = f'Storage("{self.engine_id}")|DeleteFromStorage(storagePath="{storagePath}",leaveFolderStructure={leaveFolderStructureStr});'
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn
