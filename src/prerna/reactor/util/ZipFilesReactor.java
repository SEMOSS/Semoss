package prerna.reactor.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import java.util.zip.ZipOutputStream;

public class ZipFilesReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(ZipFilesReactor.class);
    private static final String FILE_PATHS = "filePaths";

    public ZipFilesReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
                FILE_PATHS };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        List<String> filePaths = getFilePaths();
        String zipFilePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());

        if (zipFilePath == null || zipFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Zip file path is required");
        }

        try {
            FileOutputStream fos = new FileOutputStream(zipFilePath);
            ZipOutputStream zos = new ZipOutputStream(fos);

            for (String filePath : filePaths) {
                File file = new File(filePath);
                if (file.exists() && file.isFile()) {
                    ZipUtils.addToZipFile(file, zos);
                } else {
                    classLogger.warn("File does not exist or is not a file: {}", filePath);
                }
            }

            zos.close();
            fos.close();

            return new NounMetadata(true, PixelDataType.BOOLEAN);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new IllegalArgumentException("Error occurred zipping files: " + e.getMessage());
        }
    }

    private List<String> getFilePaths() {
        List<String> filePaths = new ArrayList<>();

        GenRowStruct grs = this.store.getNoun(FILE_PATHS);
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) {
                filePaths.add(grs.get(i).toString());
            }
            return filePaths;
        }

        List<NounMetadata> filePathInputs = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
        if (filePathInputs != null && !filePathInputs.isEmpty()) {
            for (NounMetadata input : filePathInputs) {
                filePaths.add(input.getValue().toString());
            }
            return filePaths;
        }

        throw new IllegalArgumentException("No file paths provided for zipping");
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(FILE_PATHS)) {
            return "List of file paths to zip together.";
        }
        return super.getDescriptionForKey(key);
    }
}