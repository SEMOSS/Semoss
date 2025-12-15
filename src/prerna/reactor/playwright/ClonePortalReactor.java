package prerna.reactor.playwright;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.CmdExecUtil;
import prerna.util.Utility;

/**
 * Clones portal files from a GitHub subdirectory to a project
 * Usage: ClonePortal(project="projectId", repo="https://github.com/org/repo.git", branch="dev", subdirectory="path/to/portals");
 */
public class ClonePortalReactor extends AbstractReactor {

    private static final String REPO = "repo";
    private static final String BRANCH = "branch";
    private static final String SUBDIRECTORY = "subdirectory";

    public ClonePortalReactor() {
        this.keysToGet = new String[]{
                ReactorKeysEnum.PROJECT.getKey(),
                REPO,
                BRANCH,
                SUBDIRECTORY
        };
        this.keyRequired = new int[] {1, 1, 0, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String projectId = keyValue.get(keysToGet[0]);
        String repo = keyValue.get(keysToGet[1]);

        String branch = "dev";
        if(keyValue.containsKey(keysToGet[2])) {
            branch = keyValue.get(keysToGet[2]);
        }

        String subdirectory = null;
        if(keyValue.containsKey(keysToGet[3])) {
            subdirectory = keyValue.get(keysToGet[3]);
        }

        String projectFolder = AssetUtility.getProjectAssetsFolder(projectId);

        try {
            cloneAndCopyPortal(projectFolder, repo, branch, subdirectory);
        } catch (Exception e) {
            return NounMetadata.getErrorNounMessage("Failed to clone portal: " + e.getMessage());
        }

        return NounMetadata.getSuccessNounMessage("Successfully cloned portals to project");
    }

    private void cloneAndCopyPortal(String projectFolder, String repo, String branch, String subdirectory) throws Exception {
        String mountName = Utility.getRandomString(5);
        String tempClonePath = Path.of(System.getProperty("java.io.tmpdir"), mountName).toString().replace("\\", "/");

        File tempDir = new File(tempClonePath);
        if (tempDir.exists()) {
            deleteDirectory(tempDir);
        }

        try {
            // Clone the repo to temp location
            CmdExecUtil cloneUtil = new CmdExecUtil(this.insight.getUser(), this.insight.getInsightId(), System.getProperty("java.io.tmpdir"));
            String cloneCommand = "git clone -b " + branch + " " + repo + " " + mountName;
            cloneUtil.executeCommand(cloneCommand);

            // Determine source directory
            File sourceDir;
            if (subdirectory != null && !subdirectory.isEmpty()) {
                sourceDir = new File(tempClonePath + File.separator + subdirectory);
            } else {
                sourceDir = tempDir;
            }

            if (!sourceDir.exists()) {
                throw new Exception("Subdirectory not found: " + subdirectory);
            }

            // Copy portal files directly to project's portals folder
            String portalsFolder = projectFolder + File.separator + "portals";
            File portalsFolderFile = new File(portalsFolder);
            if (portalsFolderFile.exists()) {
                deleteDirectory(portalsFolderFile);
            }
            portalsFolderFile.mkdirs();

            copyDirectory(sourceDir, portalsFolderFile);

        } finally {
            // Clean up temp directory
            if (tempDir.exists()) {
                deleteDirectory(tempDir);
            }
        }
    }

    private void copyDirectory(File source, File target) throws IOException {
        if (!target.exists()) {
            target.mkdirs();
        }

        File[] children = source.listFiles();
        if (children == null) {
            return;
        }

        for (File child : children) {
            if (child.getName().equals(".git")) {
                continue;
            }

            File dest = new File(target, child.getName());
            if (child.isDirectory()) {
                copyDirectory(child, dest);
            } else {
                copyFile(child, dest);
            }
        }
    }

    private void copyFile(File source, File dest) throws IOException {
        FileInputStream in = null;
        FileOutputStream out = null;
        try {
            in = new FileInputStream(source);
            out = new FileOutputStream(dest);
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
        } catch (IOException e) {
            throw new IOException("Failed to copy " + source + " to " + dest);
        } finally {
            if (in != null) {
                try { in.close(); } catch (IOException ignore) {}
            }
            if (out != null) {
                try { out.close(); } catch (IOException ignore) {}
            }
        }
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    public String getReactorDescription() {
        return "Clones portal files from a public GitHub subdirectory to a project on Semoss";
    }

    @Override
    public String getDescriptionForKey(String key) {
        if (key.equals("PROJECT")) {
            return "Semoss project id to clone portal files to.";
        } else if (key.equals("REPO")) {
            return "The HTTPS URL of the GitHub repository to clone portal files from.";
        } else if (key.equals("BRANCH")) {
            return "The branch of the GitHub repository to clone portal files from, defaults to 'dev'.";
        } else if (key.equals("SUBDIRECTORY")) {
            return "The subdirectory of the GitHub repository to clone portal files from.";
        }
        return super.getDescriptionForKey(key);
    }
}

