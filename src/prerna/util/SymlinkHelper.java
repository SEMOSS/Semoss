/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;

public class SymlinkHelper {

	private static final Logger classLogger = LogManager.getLogger(SymlinkHelper.class);

	private String userChrootFolder = null;

	private final boolean injectMode;
	private SandboxInjector injector;
	private final Map<String, Boolean> activeInjects = new LinkedHashMap<>();

	/**
	 * This will be username__uuid - ex. /opt/kunalppatel9__a123123
	 *
	 * @param targetDirName
	 */
	public SymlinkHelper(String targetDirName) {
		this.userChrootFolder = targetDirName;
		this.injectMode = "NSJAIL".equalsIgnoreCase(Utility.getDIHelperProperty(Constants.SANDBOX_MODE));

		if (injectMode) {
			return;
		}

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

	public synchronized void setInjector(SandboxInjector injector) {
		this.injector = injector;
		if (injector != null && !activeInjects.isEmpty()) {
			for (Map.Entry<String, Boolean> entry : activeInjects.entrySet()) {
				injector.inject(entry.getKey(), entry.getValue());
			}
		}
	}

	public boolean isInjectMode() {
		return injectMode;
	}

	private synchronized void enqueueOrInject(String absPath, boolean readWrite) {
		Boolean existing = activeInjects.get(absPath);
		if (existing == null || (readWrite && !existing)) {
			activeInjects.put(absPath, readWrite);
		}
		if (injector != null) {
			injector.inject(absPath, readWrite);
		}
	}

	/**
	 * Initialization for cross-platform support
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

		// Set proper ownership for home directories
		setDirectoryOwnership("/home/default", "1001", "1001");

		// Setup basic shell environment
		setupBashForChroot();
		// Setup Git
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
	 * 
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
	 * 
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
	 * 
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
	 * 
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
	 * Setup Git inside the chroot by copying the binary, its dependencies, and
	 * required helper programs for both RHEL/UBI and Ubuntu systems.
	 */
	private void setupGitForChroot() {
		try {
			// Main git binary
			copySystemBinary("/usr/bin/git");
			copyRequiredLibraries("/usr/bin/git");

			// Setup git helpers (handles both RHEL and Ubuntu)
			setupGitHelpersForCrossPlatform();

			// Setup essential binaries for git operations
			setupGitEssentialBinaries();

			// Setup git templates
			setupGitTemplatesForCrossPlatform();

			// Setup SSL certificates (handles both RHEL and Ubuntu)
			setupSSLCertsForCrossPlatform();

			// Create necessary directories
			createChrootDirectory("/tmp");
			createChrootDirectory("/var/tmp");
			createChrootDirectory("/etc/git");

			classLogger.info("Git setup completed for chroot: " + userChrootFolder);

		} catch (Exception e) {
			classLogger.error("Error setting up git for chroot: " + e.getMessage(), e);
		}
	}

	/**
	 * Setup git helper programs for both RHEL/UBI and Ubuntu systems
	 */
	private void setupGitHelpersForCrossPlatform() {
		try {
			// Try RHEL/UBI path first
			Path gitHelpersHost = Paths.get("/usr/libexec/git-core");
			Path gitHelpersChroot = Paths.get(userChrootFolder).resolve("usr/libexec/git-core");

			if (Files.exists(gitHelpersHost)) {
				copyDirectoryRecursively(gitHelpersHost, gitHelpersChroot);
				classLogger.info("Copied git helpers from RHEL/UBI location: " + gitHelpersHost);
			} else {
				// Fallback to Ubuntu path
				gitHelpersHost = Paths.get("/usr/lib/git-core");
				gitHelpersChroot = Paths.get(userChrootFolder).resolve("usr/lib/git-core");

				if (Files.exists(gitHelpersHost)) {
					copyDirectoryRecursively(gitHelpersHost, gitHelpersChroot);
					classLogger.info("Copied git helpers from Ubuntu location: " + gitHelpersHost);
				} else {
					classLogger.warn("Git helpers directory not found at expected locations");
					// Try to find git helpers using git itself
					findAndCopyGitHelpers();
				}
			}
		} catch (IOException e) {
			classLogger.error("Error setting up git helpers: " + e.getMessage(), e);
		}
	}

	/**
	 * Find git helpers using git --exec-path command
	 */
	private void findAndCopyGitHelpers() {
		try {
			ProcessBuilder pb = new ProcessBuilder("git", "--exec-path");
			Process process = pb.start();

			try (java.io.BufferedReader reader = new java.io.BufferedReader(
					new java.io.InputStreamReader(process.getInputStream()))) {

				String gitExecPath = reader.readLine();
				if (gitExecPath != null && !gitExecPath.trim().isEmpty()) {
					Path gitHelpersHost = Paths.get(gitExecPath.trim());
					Path gitHelpersChroot = Paths.get(userChrootFolder).resolve(gitExecPath.substring(1));

					if (Files.exists(gitHelpersHost)) {
						copyDirectoryRecursively(gitHelpersHost, gitHelpersChroot);
						classLogger.info("Found and copied git helpers from: " + gitExecPath);
					}
				}
			}

			process.waitFor();
		} catch (Exception e) {
			classLogger.warn("Could not find git helpers using git --exec-path: " + e.getMessage());
		}
	}

	/**
	 * Setup essential binaries for git operations on both platforms
	 */
	private void setupGitEssentialBinaries() {
		String[] essentialBinaries = { "/usr/bin/curl", // Required for git-remote-https
				"/usr/bin/ssh", // For git@... URLs
				"/bin/tar", // Archive operations
				"/bin/gzip", // Compression
				"/usr/bin/openssl", // SSL/TLS operations
				"/usr/bin/wget" // Alternative HTTP client
		};

		for (String bin : essentialBinaries) {
			if (Files.exists(Paths.get(bin))) {
				copySystemBinary(bin);
				copyRequiredLibraries(bin);
				classLogger.debug("Copied git dependency: " + bin);
			} else {
				classLogger.debug("Optional git dependency not found: " + bin);
			}
		}

		// Copy git-specific libraries that might be needed
		copyGitSpecificLibraries();
	}

	/**
	 * Copy git-specific libraries for both RHEL and Ubuntu
	 */
	private void copyGitSpecificLibraries() {
		// RHEL/UBI library paths
		String[] rhelLibs = { "/usr/lib64/libcurl.so.4", "/usr/lib64/libssl.so.3", "/usr/lib64/libcrypto.so.3",
				"/usr/lib64/libz.so.1", "/usr/lib64/libgssapi_krb5.so.2" };

		// Ubuntu library paths
		String[] ubuntuLibs = { "/usr/lib/x86_64-linux-gnu/libcurl.so.4", "/usr/lib/x86_64-linux-gnu/libssl.so.3",
				"/usr/lib/x86_64-linux-gnu/libcrypto.so.3", "/usr/lib/x86_64-linux-gnu/libz.so.1" };

		// Try RHEL paths first
		boolean foundRhelLibs = false;
		for (String lib : rhelLibs) {
			if (Files.exists(Paths.get(lib))) {
				copyLibraryIfExists(lib);
				foundRhelLibs = true;
			}
		}

		// If no RHEL libs found, try Ubuntu paths
		if (!foundRhelLibs) {
			for (String lib : ubuntuLibs) {
				copyLibraryIfExists(lib);
			}
		}
	}

	/**
	 * Setup git templates for cross-platform compatibility
	 */
	private void setupGitTemplatesForCrossPlatform() {
		String[] templatePaths = { "/usr/share/git-core/templates", // Common location
				"/usr/local/share/git-core/templates" // Alternative location
		};

		for (String templatePath : templatePaths) {
			Path templatesSource = Paths.get(templatePath);
			if (Files.exists(templatesSource)) {
				Path templatesTarget = Paths.get(userChrootFolder).resolve(templatePath.substring(1));
				try {
					copyDirectoryRecursively(templatesSource, templatesTarget);
					classLogger.info("Copied git templates from: " + templatePath);
					break; // Only copy the first one found
				} catch (IOException e) {
					classLogger.debug("Could not copy git templates from: " + templatePath);
				}
			}
		}
	}

	/**
	 * Setup SSL certificates for both RHEL/UBI and Ubuntu systems
	 */
	private void setupSSLCertsForCrossPlatform() {
		try {
			// Detect OS type and setup accordingly
			boolean isRHELBased = isRHELBasedSystem();
			if (isRHELBased) {
				classLogger.info("Predicted OS as RHEL");
				setupSSLCertsForRHEL();
			} else {
				classLogger.info("Predicted OS as Ubuntu");
				setupSSLCertsForUbuntu();
			}
		} catch (Exception e) {
			classLogger.warn("Could not setup SSL certificates: " + e.getMessage());
			// Try both approaches as fallback
			setupSSLCertsForRHEL();
			setupSSLCertsForUbuntu();
		}
	}

	/**
	 * Detect if the system is RHEL-based
	 */
	private boolean isRHELBasedSystem() {
		// Check for RHEL-specific files/directories
		return Files.exists(Paths.get("/etc/redhat-release")) || Files.exists(Paths.get("/etc/rhel-release"))
				|| Files.exists(Paths.get("/etc/pki"));
	}

	/**
	 * Setup SSL certificates for RHEL/UBI systems
	 */
	private void setupSSLCertsForRHEL() {
		try {
			String[] caCertFiles = { "/etc/pki/tls/certs/ca-bundle.crt",
					"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", "/etc/pki/tls/cert.pem" };

			// Copy CA certificate files
			for (String certPath : caCertFiles) {
				Path sourceCerts = Paths.get(certPath);
				if (Files.exists(sourceCerts)) {
					Path targetCerts = Paths.get(userChrootFolder).resolve(certPath.substring(1));
					Files.createDirectories(targetCerts.getParent());
					Files.copy(sourceCerts, targetCerts, StandardCopyOption.REPLACE_EXISTING);
					classLogger.info("Copied RHEL CA certificates from: " + certPath);
				} else {
					classLogger.warn("Could not find file at " + certPath);
				}
			}

			// Copy certificate directories
			String[] certDirs = { "/etc/pki/tls/certs", "/etc/pki/ca-trust/extracted", "/etc/pki/tls" };

			for (String certDir : certDirs) {
				Path sourceCertDir = Paths.get(certDir);
				if (Files.exists(sourceCertDir)) {
					Path targetCertDir = Paths.get(userChrootFolder).resolve(certDir.substring(1));
					copyDirectoryRecursively(sourceCertDir, targetCertDir);
					classLogger.debug("Copied RHEL certificate directory: " + certDir);
				} else {
					classLogger.warn("Could not find directory at " + certDir);
				}
			}

		} catch (IOException e) {
			classLogger.debug("Could not setup RHEL SSL certificates: " + e.getMessage());
		}
	}

	/**
	 * Setup SSL certificates for Ubuntu systems
	 */
	private void setupSSLCertsForUbuntu() {
		try {
			String[] caCertFiles = { "/etc/ssl/certs/ca-certificates.crt" };

			// Copy CA certificate files
			for (String certPath : caCertFiles) {
				Path sourceCerts = Paths.get(certPath);
				if (Files.exists(sourceCerts)) {
					Path targetCerts = Paths.get(userChrootFolder).resolve(certPath.substring(1));
					Files.createDirectories(targetCerts.getParent());
					Files.copy(sourceCerts, targetCerts, StandardCopyOption.REPLACE_EXISTING,
							StandardCopyOption.COPY_ATTRIBUTES);
					classLogger.info("Copied Ubuntu CA certificates from: " + certPath);
				} else {
					classLogger.warn("Could not find file at " + certPath);
				}
			}

			// Copy certificate directories with symlink preservation
			String[] certDirs = { "/etc/ssl/certs", "/usr/share/ca-certificates" };
			for (String certDir : certDirs) {
				Path sourceCertDir = Paths.get(certDir);
				if (Files.exists(sourceCertDir)) {
					Path targetCertDir = Paths.get(userChrootFolder).resolve(certDir.substring(1));
					copyDirectoryRecursively(sourceCertDir, targetCertDir);
					classLogger.debug("Copied Ubuntu certificate directory (with symlinks): " + certDir);
				} else {
					classLogger.warn("Could not find directory at " + certDir);
				}
			}
		} catch (IOException e) {
			classLogger.debug("Could not setup Ubuntu SSL certificates: " + e.getMessage());
		}
	}

	/**
	 * Set ownership of a directory in chroot (works on both platforms)
	 */
	private void setDirectoryOwnership(String relativePath, String uid, String gid) {
		try {
			Path chrootDir = Paths.get(userChrootFolder, relativePath);
			if (Files.exists(chrootDir)) {
				ProcessBuilder pb = new ProcessBuilder("chown", uid + ":" + gid, chrootDir.toString());
				Process process = pb.start();
				int exitCode = process.waitFor();
				if (exitCode == 0) {
					classLogger.debug("Set ownership " + uid + ":" + gid + " for: " + relativePath);
				} else {
					classLogger.warn("Could not set ownership for: " + relativePath + " (exit code: " + exitCode + ")");
				}
			}
		} catch (Exception e) {
			classLogger.warn("Could not set ownership for: " + relativePath, e);
		}
	}

	/**
	 * Copy an entire directory recursively into the chroot, preserving structure.
	 */
	private void copyDirectoryRecursively(Path source, Path target) throws IOException {
		if (source.toFile().exists()) {
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
		if (injectMode) {
			enqueueOrInject(Utility.normalizePath(sourceDirName), true);
			return;
		}
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

	public void symlinkUserAsset(User user) {
		AuthProvider provider = user.getPrimaryLogin();
		String projectId = user.getAssetProjectId(provider);
		String assetFolder = AssetUtility.getUserAssetAppRootFolder("Asset", projectId);
		classLogger.info("Symlinking user asset folder for projectId=" + projectId);
		symlinkFolder(assetFolder);
	}

	public void symlinkProject(User user, String projectId) {
		classLogger.info("Symlinking project for projectId=" + projectId);

		String projectAppRootFolder = AssetUtility.getProjectAppRootFolder(projectId);

		boolean canEdit = SecurityProjectUtils.userCanEditProject(user, projectId);

		if (injectMode) {
			enqueueOrInject(Utility.normalizePath(projectAppRootFolder), canEdit);
			return;
		}

		if (canEdit) {
			symlinkFolder(projectAppRootFolder);
			return;
		}

		boolean readOnlyCopyEnabled = Boolean
				.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_READ_ONLY_COPY));

		if (readOnlyCopyEnabled) {
			Path projectTarget = Paths.get(userChrootFolder)
					.resolve(Utility.normalizePath(projectAppRootFolder).substring(1));
			if (Files.exists(projectTarget)) {
				classLogger.info("Chrooted project already copied for projectId=" + projectId + " at: " + projectTarget
						+ ", skipping copy and permission patch.");
				return;
			}

			classLogger.info("Symlinking read-only copy for projectId=" + projectId);
			setupCopiedProject(projectId);
			setAllReadExecuteForProject(projectId);
			// below does not work - commenting out for now
			// setExecuteOnlyOnAssetCodeFolders(projectId);
		} else {
			classLogger.info("Symlinking full folder for read-only user, projectId=" + projectId);
			symlinkFolder(projectAppRootFolder);
		}
	}

	private void setOwnerRWX(Path dir) throws IOException {
		Files.setPosixFilePermissions(dir, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
				PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
				PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE));
	}

	private void setOwnerRX(Path dir) throws IOException {
		Files.setPosixFilePermissions(dir,
				EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE,
						PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
						PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE));
	}

	private void setupCopiedProject(String projectId) {
		String sourceDirToCopy = AssetUtility.getProjectAppRootFolder(projectId);
		sourceDirToCopy = Utility.normalizePath(sourceDirToCopy);
		Path projectSource = Paths.get(sourceDirToCopy);
		if (!Files.exists(projectSource)) {
			classLogger.warn("Project app root does not exist for readOnlyCopyProject: " + sourceDirToCopy);
			return;
		}
		Path projectTarget = Paths.get(userChrootFolder).resolve(sourceDirToCopy.substring(1));
		if (Files.exists(projectTarget)) {
			classLogger.info("Chrooted project already copied, skipping copy for: " + projectTarget);
			return;
		}
		try {
			copyDirectoryRecursively(projectSource, projectTarget);
			classLogger.info("Copied project from: " + projectSource);
		} catch (IOException e) {
			classLogger.debug("Could not copy project from: " + projectSource);
		}
	}

	public void setAllReadExecuteForProject(String projectId) {
		String projectAppRootFolder = AssetUtility.getProjectAppRootFolder(projectId);
		projectAppRootFolder = Utility.normalizePath(projectAppRootFolder);
		Path chrootAppRoot = Paths.get(userChrootFolder,
				projectAppRootFolder.startsWith("/") ? projectAppRootFolder.substring(1) : projectAppRootFolder);

		if (!Files.exists(chrootAppRoot)) {
			classLogger.warn("Chrooted app root does not exist for project: " + chrootAppRoot);
			return;
		}

		try {
			Files.walkFileTree(chrootAppRoot, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
					setOwnerRX(dir);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					setOwnerRX(file);
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			classLogger.error("Failed to set r-x permissions on project: " + chrootAppRoot, e);
		}
	}

	public void setExecuteOnlyOnAssetCodeFolders(String projectId) {
		String assetsFolderPath = AssetUtility.getProjectAssetsFolder(projectId);
		assetsFolderPath = Utility.normalizePath(assetsFolderPath);
		Path chrootAssetsFolder = Paths.get(userChrootFolder,
				assetsFolderPath.startsWith("/") ? assetsFolderPath.substring(1) : assetsFolderPath);

		if (!Files.exists(chrootAssetsFolder)) {
			classLogger.warn("Chroot assets folder does not exist: " + chrootAssetsFolder);
			return;
		}

		String[] codeDirs = { "java", "classes", "py" };
		for (String dirName : codeDirs) {
			Path subDir = chrootAssetsFolder.resolve(dirName);
			if (Files.exists(subDir)) {
				try {
					Files.walkFileTree(subDir, new SimpleFileVisitor<Path>() {
						@Override
						public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
								throws IOException {
							Files.setPosixFilePermissions(dir, EnumSet.of(PosixFilePermission.OWNER_EXECUTE,
									PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_EXECUTE));
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
							Files.setPosixFilePermissions(file, EnumSet.of(PosixFilePermission.OWNER_EXECUTE,
									PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_EXECUTE));
							return FileVisitResult.CONTINUE;
						}
					});
				} catch (IOException e) {
					classLogger.warn("Failed to set execute-only on: " + subDir, e);
				}
			}
		}
	}

}
