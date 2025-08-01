package prerna.reactor.storage;

import java.io.File;
import java.util.List;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class PullMultipleFromStorageReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(PullMultipleFromStorageReactor.class);
    private static final String STORAGE_PATHS = "storagePaths";

    public PullMultipleFromStorageReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
                ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), STORAGE_PATHS };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        IStorageEngine storage = getStorage();
        List<String> storagePaths = getStoragePaths();

        // Get the base directory for downloads
        String baseDir = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
        if (!(new File(baseDir).isDirectory())) {
            new File(baseDir).mkdirs();
        }

        try {
            for (String storagePath : storagePaths) {
                // Extract filename from storage path
                String filename = storagePath.substring(storagePath.lastIndexOf('/') + 1);
                if (filename.isEmpty()) {
                    filename = "downloaded_file_" + System.currentTimeMillis();
                }

                // Create unique local path for each file
                String localPath = baseDir + "/" + filename;
                storage.copyToLocal(storagePath, localPath);
            }
            return new NounMetadata(true, PixelDataType.BOOLEAN);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new IllegalArgumentException("Error occurred downloading storage files to local");
        }
    }

    private List<String> getStoragePaths() {
        List<String> storagePaths = new ArrayList<>();

        String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
        if (storagePath != null && !storagePath.isEmpty()) {
            storagePaths.add(storagePath);
            return storagePaths;
        }

        GenRowStruct grs = this.store.getNoun(STORAGE_PATHS);
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) {
                storagePaths.add(grs.get(i).toString());
            }
            return storagePaths;
        }

        List<NounMetadata> storagePathInputs = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
        if (storagePathInputs != null && !storagePathInputs.isEmpty()) {
            for (NounMetadata input : storagePathInputs) {
                storagePaths.add(input.getValue().toString());
            }
            return storagePaths;
        }

        throw new IllegalArgumentException("No storage paths provided for download");
    }

    private IStorageEngine getStorage() {
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE.getKey());
        if (grs != null && !grs.isEmpty()) {
            return (IStorageEngine) grs.get(0);
        }

        List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
        if (storageInputs != null && !storageInputs.isEmpty()) {
            return (IStorageEngine) storageInputs.get(0).getValue();
        }

        throw new NullPointerException("No storage engine defined");
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(STORAGE_PATHS)) {
            return "List of storage paths to download. If provided, this takes precedence over storagePath.";
        }
        return super.getDescriptionForKey(key);
    }
}