package prerna.util;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SymlinkHelper {

	private static final Logger classLogger = LogManager.getLogger(SymlinkHelper.class);

	private String userChrootFolder = null;

	/**
	 * This will be username__uuid - ex. /opt/kunalppatel9__a123123
	 * @param targetDirName
	 */
	public SymlinkHelper(String targetDirName) {
		this.userChrootFolder = targetDirName;
		File targetDir = new File(Utility.normalizePath(userChrootFolder));
		if (!targetDir.exists()) {
			classLogger.info("User chroot folder doesn't exist. Making folder now at: " + userChrootFolder);
			boolean success = targetDir.mkdir(); // make directory
			classLogger.info("User chroot folder creation at " + userChrootFolder + " " + success);
		}

		// also create the semoss home folder
		String newSemossHomeFolderPath = this.userChrootFolder + "/" + Utility.getBaseFolder();
		File userSemosshomeDir = new File(Utility.normalizePath(newSemossHomeFolderPath));
		if (!userSemosshomeDir.exists()) {
			userSemosshomeDir.mkdirs(); // make user home directory
		}

		initalizeChrootFolder();
	}

	/**
	 * 
	 */
	private void initalizeChrootFolder() {
		String baseFolder = Utility.getBaseFolder();
		symlinkFolder(baseFolder + "/" + Constants.PY_BASE_FOLDER);
		symlinkFolder(Utility.getDIHelperProperty(Constants.INSIGHT_CACHE_DIR));
		// Read paths from DIHelper or configuration
		String pathsToSymlink = Utility.getDIHelperProperty("CHROOT_SYMLINK_PATHS");
		if (pathsToSymlink != null && !pathsToSymlink.isEmpty()) {
			String[] paths = pathsToSymlink.split(",");
			for (String path : paths) {
				symlinkFolder(path.trim());
			}
		} else {
			classLogger.warn("No paths specified for symlinking.");
		}

		createChrootDirectory("/root");
		createChrootDirectory("/home/default");
		// Setup minimal bash environment for chroot
		setupBashForChroot();
		setupGitForChroot();
	}

	/**
	 * Setup minimal bash environment - only bash binary and its required libraries
	 */
	private void setupBashForChroot() {
		try {
			// Create essential directories
			createChrootDirectory("bin");
			createChrootDirectory("lib");
			createChrootDirectory("lib64");

			String[] coreutilsCommands = { "/bin/bash", "/bin/sh", "/bin/ls", "/bin/echo", "/usr/bin/coreutils" };
			for (String cmd : coreutilsCommands) {
				copySystemBinary(cmd);
				copyRequiredLibraries(cmd);
			}

			// Copy coreutils master binary
			copySystemBinary("/usr/bin/coreutils");
			copyRequiredLibraries("/usr/bin/coreutils");

			// Copy common symlinks that point to coreutils
			String[] coreutilsCmds = { "/bin/ls", "/bin/echo", "/bin/mkdir", "/bin/touch", "/bin/cp", "/bin/mv",
					"/bin/rm", "/bin/cat", "/bin/pwd" };

			for (String cmd : coreutilsCmds) {
				copySystemBinary(cmd); // copies symlink
				copyRequiredLibraries(cmd); // mostly no-op, since all point to coreutils
			}
			
	        // Add whoami and id commands
	        String[] identityCommands = { "/usr/bin/whoami", "/usr/bin/id" };
	        for (String cmd : identityCommands) {
	            copySystemBinary(cmd);
	            copyRequiredLibraries(cmd);
	        }

			classLogger.info("Bash and essential commands setup completed for chroot: " + userChrootFolder);
		} catch (Exception e) {
			classLogger.error("Error setting up bash for chroot: " + e.getMessage(), e);
		}
	}

	/**
	 * Create a directory inside the chroot environment
	 * @param relativePath
	 */
	private void createChrootDirectory(String relativePath) {
		try {
			Path chrootDir = Paths.get(userChrootFolder, relativePath);
			Files.createDirectories(chrootDir);
			classLogger.debug("Created chroot directory: " + chrootDir);
		} catch (IOException e) {
			classLogger.error("Failed to create chroot directory: " + relativePath, e);
		}
	}

	/**
	 * Copy a system binary to the chroot environment
	 * @param binaryPath
	 */
	private void copySystemBinary(String binaryPath) {
		try {
			Path sourceBinary = Paths.get(binaryPath);
			if (!Files.exists(sourceBinary)) {
				classLogger.warn("System binary does not exist: " + binaryPath);
				return;
			}

			Path targetBinary = Paths.get(userChrootFolder, binaryPath.substring(1)); // Remove leading slash

			// Create parent directories if they don't exist
			Files.createDirectories(targetBinary.getParent());

			// Copy the binary
			Files.copy(sourceBinary, targetBinary, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

			// Make it executable
			targetBinary.toFile().setExecutable(true);

			classLogger.debug("Copied system binary: " + binaryPath + " to " + targetBinary);
		} catch (IOException e) {
			classLogger.error("Failed to copy system binary: " + binaryPath, e);
		}
	}

	/**
	 * Copy required shared libraries for bash using ldd command
	 * @param binaryPath
	 */
	private void copyRequiredLibraries(String binaryPath) {
		try {
			// Use ldd to find required libraries
			ProcessBuilder pb = new ProcessBuilder("ldd", binaryPath);
			Process process = pb.start();

			try (java.io.BufferedReader reader = new java.io.BufferedReader(
					new java.io.InputStreamReader(process.getInputStream()))) {

				String line;
				while ((line = reader.readLine()) != null) {
					// Parse ldd output to extract library paths
					// Format: libname.so => /path/to/lib (0x...)
					if (line.contains("=>") && line.contains("/")) {
						String[] parts = line.split("=>");
						if (parts.length == 2) {
							String libPath = parts[1].trim().split("\\s+")[0];
							if (libPath.startsWith("/")) {
								copyLibraryIfExists(libPath);
							}
						}
					}
					// Handle direct library paths (like /lib64/ld-linux-x86-64.so.2)
					else if (line.trim().startsWith("/lib")) {
						String libPath = line.trim().split("\\s+")[0];
						copyLibraryIfExists(libPath);
					}
				}
			}

			process.waitFor();
		} catch (Exception e) {
			classLogger.error("Failed to copy required libraries for: " + binaryPath, e);
		}
	}

	/**
	 * Copy a single library to the chroot environment
	 * @param libraryPath
	 */
	private void copyLibraryIfExists(String libraryPath) {
		try {
			Path sourceLib = Paths.get(libraryPath);
			if (!Files.exists(sourceLib)) {
				return;
			}

			Path targetLib = Paths.get(userChrootFolder, libraryPath.substring(1)); // Remove leading slash

			// Create parent directories if they don't exist
			Files.createDirectories(targetLib.getParent());

			// Copy the library if it doesn't already exist
			if (!Files.exists(targetLib)) {
				Files.copy(sourceLib, targetLib, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
				classLogger.debug("Copied library: " + libraryPath + " to " + targetLib);
			}
		} catch (IOException e) {
			classLogger.debug("Could not copy library: " + libraryPath + " - " + e.getMessage());
		}
	}

	/**
	 * Setup Git inside the chroot by copying the binary, its dependencies,
	 * and required helper programs.
	 */
	private void setupGitForChroot() {
		try {
			// main git binary
			copySystemBinary("/usr/bin/git");
			copyRequiredLibraries("/usr/bin/git");

			// git helper programs (git-remote-https, git-upload-pack, etc.)
			Path gitHelpersHost = Paths.get("/usr/lib/git-core");
			Path gitHelpersChroot = Paths.get(userChrootFolder).resolve("usr/lib/git-core");
			copyDirectoryRecursively(gitHelpersHost, gitHelpersChroot);

			// common dependencies (needed for cloning, fetching, archiving)
			String[] extraBinaries = {
					"/usr/bin/ssh",   // for git@... URLs
					"/bin/tar",       // archive ops
					"/bin/gzip"       // compression
			};

			for (String bin : extraBinaries) {
				if (Files.exists(Paths.get(bin))) {
					copySystemBinary(bin);
					copyRequiredLibraries(bin);
				}
			}

			// git templates
			copyDirectoryRecursively(
					Paths.get("/usr/share/git-core/templates"),
					Paths.get(userChrootFolder).resolve("usr/share/git-core/templates")
					);

			// SSL CA certs (needed for https remotes)
			copySystemBinary("/etc/ssl/certs/ca-certificates.crt");
			copyDirectoryRecursively(
					Paths.get("/etc/ssl/certs"),
					Paths.get(userChrootFolder).resolve("etc/ssl/certs")
					);

		} catch (Exception e) {
			classLogger.error("Error setting up git for chroot: " + e.getMessage(), e);
		}
	}

	/**
	 * Copy an entire directory recursively into the chroot, preserving structure.
	 */
	private void copyDirectoryRecursively(Path source, Path target) throws IOException {
		if(source.toFile().exists()) {
			Files.walk(source).forEach(src -> {
				try {
					Path dest = target.resolve(source.relativize(src));
					if (Files.isDirectory(src)) {
						Files.createDirectories(dest);
					} else {
						Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
					}
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
		}
	}


	/**
	 * 
	 * @param sourceDirName
	 */
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

	/**
	 * 
	 * @return
	 */
	public String getUserChrootFolder() {
		return this.userChrootFolder;
	}

	/**
	 * 
	 */
	public void removeChrootFolder() {
		try {
			FileUtils.deleteDirectory(new File(userChrootFolder));
			classLogger.info(userChrootFolder + " Directory and all contents deleted successfully.");
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, "Error deleting directory: " + e.getMessage());
		}
	}


}
