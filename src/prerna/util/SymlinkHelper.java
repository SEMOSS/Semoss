package prerna.util;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.io.IOException;

public class SymlinkHelper {

	private static final Logger classLogger = LogManager.getLogger(SymlinkHelper.class);
	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private String userChrootFolder = null;


	// this will be username__uuid - ex. /opt/kunalppatel9__a123123
	public SymlinkHelper(String targetDirName) {

		this.userChrootFolder = targetDirName;
		File targetDir = new File(Utility.normalizePath(userChrootFolder));
		if (!targetDir.exists()) {
			classLogger.info("User chroot folder doesn't exist. Making folder now at: " + userChrootFolder);
			boolean success = targetDir.mkdir(); // make directory
			classLogger.info("User chroot folder creation at " + userChrootFolder + " " + success);
		}

		// also create the semoss home folder
		String newSemossHomeFolderPath = this.userChrootFolder + FILE_SEPARATOR + DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		File userSemosshomeDir = new File(Utility.normalizePath(newSemossHomeFolderPath));
		if (!userSemosshomeDir.exists()) {
			userSemosshomeDir.mkdirs(); // make user home directory
		}
		
		initalizeChrootFolder();

	}
	
	private void initalizeChrootFolder() {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		symlinkFolder(baseFolder + FILE_SEPARATOR + Constants.PY_BASE_FOLDER);
		symlinkFolder(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR));
        // Read paths from DIHelper or configuration
        String pathsToSymlink = DIHelper.getInstance().getProperty("CHROOT_SYMLINK_PATHS");
        if (pathsToSymlink != null && !pathsToSymlink.isEmpty()) {
            String[] paths = pathsToSymlink.split(",");
            for (String path : paths) {
                symlinkFolder(path.trim());
            }
        } else {
            classLogger.warn("No paths specified for symlinking.");
        }
	}	
	
	public void symlinkFolder(String sourceDirName) {
	    classLogger.debug("Making symlink for folder " + sourceDirName);
	    // Convert the source directory and user chroot folder to Path objects
	    sourceDirName = Utility.normalizePath(sourceDirName);
	    Path sourceDir = Paths.get(sourceDirName);
	    Path userChrootPath = Paths.get(userChrootFolder);

	    classLogger.debug("User chroot path is " + userChrootFolder);

	    // Construct the path for the symbolic link
	    Path symlinkPath = userChrootPath.resolve(sourceDirName.substring(1)); // Remove leading slash
	    classLogger.debug("Full symlink path is " + symlinkPath);

	    try {
	        // Check if the source directory exists
	        if (!Files.exists(sourceDir)) {
	            throw new IllegalArgumentException("Source directory does not exist: " + sourceDirName);
	        }

	        // Ensure the parent directories exist for the symlink path
	        Files.createDirectories(symlinkPath.getParent());

	        // Check if the symlink already exists
	        if (Files.exists(symlinkPath)) {
	            classLogger.debug("Symbolic link already exists at: " + symlinkPath);
	            // Optionally, delete the existing symlink
	            // Files.delete(symlinkPath);
	        } else {
	            // Create the symbolic link
	            Files.createSymbolicLink(symlinkPath, sourceDir);
	            classLogger.info("Symbolic link created at: " + symlinkPath);
	        }
	    } catch (IllegalArgumentException e) {
	        classLogger.error("Invalid argument: " + e.getMessage(), e);
	    } catch (IOException e) {
	        classLogger.error("Error creating symbolic link: " + e.getMessage(), e);
	    } catch (UnsupportedOperationException e) {
	        classLogger.error("Symbolic links are not supported on this file system.", e);
	    }
	}
    
	public String getUserChrootFolder() {
		return this.userChrootFolder;
	}

	public void removeChrootFolder() {
        try {
            FileUtils.deleteDirectory(new File(userChrootFolder));
            classLogger.info(userChrootFolder + " Directory and all contents deleted successfully.");
        } catch (IOException e) {
        	classLogger.error(Constants.STACKTRACE, "Error deleting directory: " + e.getMessage());
        }
		
	}
	

}
