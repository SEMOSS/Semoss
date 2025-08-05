package prerna.reactor.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import java.util.zip.ZipOutputStream;

public class ZipFilesReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(ZipFilesReactor.class);
    private static final String FILE_PATHS = "filePaths";

    public ZipFilesReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
                FILE_PATHS };
        this.keyRequired = new int[] { 0, 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
            throwAnonymousUserError();
        }

        String space = this.keyValue.get(ReactorKeysEnum.SPACE.getKey());
        String zipFilePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());

        if (zipFilePath == null || zipFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Zip file path is required");
        }

        String baseFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
        String zipFileFullPath = (baseFolder + "/" + Utility.normalizePath(zipFilePath)).replace('\\', '/');
        File zipFile = new File(zipFileFullPath);

        List<String> filePaths = getFilePaths();
        if (filePaths.isEmpty()) {
            throw new IllegalArgumentException("No file paths provided for zipping");
        }

        long totalSourceSize = 0;
        List<File> filesToZip = new ArrayList<>();

        for (String filePath : filePaths) {
            String fileToZip = (baseFolder + "/" + Utility.normalizePath(filePath)).replace('\\', '/');
            File file = new File(fileToZip);
            if (!file.exists()) {
                throw new IllegalArgumentException("Cannot find file '" + filePath + "'");
            }
            if (!file.isFile()) {
                throw new IllegalArgumentException("Path '" + filePath + "' is not a file");
            }
            filesToZip.add(file);
            totalSourceSize += file.length();
        }

        try {
            FileOutputStream fos = new FileOutputStream(zipFile);
            ZipOutputStream zos = new ZipOutputStream(fos);

            for (File file : filesToZip) {
                ZipUtils.addToZipFile(file, zos);
            }

            zos.close();
            fos.close();

        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to zip files. Detailed error = " + e.getMessage());
        }

        if (ClusterUtil.IS_CLUSTER) {
            if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
                AuthProvider provider = user.getPrimaryLogin();
                String projectId = user.getAssetProjectId(provider);
                if (projectId != null && !(projectId.isEmpty())) {
                    ClusterUtil.pushUserWorkspace(projectId, true);
                }
            } else if (space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
                if (this.insight.isSavedInsight()) {
                    IProject project = Utility.getProject(this.insight.getProjectId());
                    ClusterUtil.pushProjectFolder(project, zipFile.getParent());
                }
            } else {
                IProject project = Utility.getProject(space);
                ClusterUtil.pushProjectFolder(project, zipFile.getParent());
            }
        }

        Map<String, Object> fileDetails = new HashMap<>();
        fileDetails.put("fileName", zipFile.getName());
        fileDetails.put("sourceSize", Utility.getReadableFileSize(totalSourceSize));
        fileDetails.put("size", Utility.getReadableFileSize(zipFile.length()));
        return new NounMetadata(fileDetails, PixelDataType.MAP);
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

        return filePaths;
    }

    @Override
    public String getReactorDescription() {
        return "This reactor is used to zip multiple files together";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(FILE_PATHS)) {
            return "List of file paths to zip together.";
        } else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
            return "This is a required value containing the relative file path for the output zip file";
        } else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
            return "This is an optional field to determine the space in which the relative file paths exist (user project space, current insight space, project id space).";
        }
        return super.getDescriptionForKey(key);
    }
}