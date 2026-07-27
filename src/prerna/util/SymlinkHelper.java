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
import java.util.stream.Stream;

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
	 * Construct a SymlinkHelper bound to a user-specific chroot folder and run the
	 * full initialization sequence. The chroot folder is created if missing, the
	 * SEMOSS home folder is created inside it, and the cross-platform chroot
	 * environment (bash, git, libraries, certs) is populated.
	 *
	 * @param targetDirName absolute path of the user chroot folder, conventionally
	 *                      named {@code <username>__<uuid>} (e.g.
	 *                      {@code /opt/kunalppatel9__a123123})
	 */
	public SymlinkHelper(String targetDirName) {
		this.userChrootFolder = targetDirName;
		String sandboxMode = Utility.getDIHelperProperty(Constants.SANDBOX_MODE);
		this.injectMode = "NAMESPACE".equalsIgnoreCase(sandboxMode) || "NSJAIL".equalsIgnoreCase(sandboxMode);

		File targetDir = new File(Utility.normalizePath(userChrootFolder));
		if (!targetDir.exists()) {
			classLogger.info("User chroot folder doesn't exist. Making folder now at: {}", userChrootFolder);
			boolean success = targetDir.mkdir(); // make directory
			classLogger.info("User chroot folder creation at {} {}", userChrootFolder, success);
		}

		// also create the semoss home folder
		String newSemossHomeFolderPath = this.userChrootFolder + "/" + Utility.getBaseFolder();
		File userSemosshomeDir = new File(Utility.normalizePath(newSemossHomeFolderPath));
		if (!userSemosshomeDir.exists()) {
			userSemosshomeDir.mkdirs(); // make user home directory
		}

		long startNanos = System.nanoTime();
		initalizeChrootFolder();
		long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
		classLogger.info("Chroot init completed in {} ms for: {}", elapsedMs, userChrootFolder);
	}

	public synchronized void setInjector(SandboxInjector injector) {
		this.injector = injector;
		if (injector != null && !activeInjects.isEmpty()) {
			for (Map.Entry<String, Boolean> entry : activeInjects.entrySet()) {
				try {
					injector.inject(entry.getKey(), entry.getValue());
				} catch (RuntimeException e) {
					classLogger.warn("Skipping namespace injection for {}", entry.getKey(), e);
				}
			}
		}
	}

	public boolean isInjectMode() {
		return injectMode;
	}

	private synchronized void enqueueOrInject(String absPath, boolean readWrite) {
		if (!isNamespaceInjectablePath(absPath)) {
			classLogger.warn("Skipping namespace injection for path outside SEMOSS home: {}", absPath);
			return;
		}
		Boolean existing = activeInjects.get(absPath);
		if (existing == null || (readWrite && !existing)) {
			activeInjects.put(absPath, readWrite);
		}
		if (injector != null) {
			try {
				injector.inject(absPath, readWrite);
			} catch (RuntimeException e) {
				classLogger.warn("Skipping namespace injection for {}", absPath, e);
			}
		}
	}

	private boolean isNamespaceInjectablePath(String absPath) {
		if (absPath == null || absPath.trim().isEmpty()) {
			return false;
		}
		try {
			Path injectRoot = Paths.get(Utility.getBaseFolder()).toAbsolutePath().normalize();
			Path path = Paths.get(absPath).toAbsolutePath().normalize();
			try {
				injectRoot = injectRoot.toRealPath();
			} catch (IOException ignored) {
				// Fall back to the normalized configured root.
			}
			try {
				if (Files.exists(path)) {
					path = path.toRealPath();
				}
			} catch (IOException ignored) {
				// Fall back to the normalized requested path.
			}
			return path.startsWith(injectRoot);
		} catch (Exception e) {
			classLogger.debug("Unable to determine if path is namespace injectable: {}", absPath, e);
			return false;
		}
	}

	/**
	 * Run the cross-platform chroot initialization sequence: symlink configured
	 * paths (Python base folder, insight cache, and any extras from
	 * {@code CHROOT_SYMLINK_PATHS}), create the standard mount points
	 * ({@code /root}, {@code /home/default}) and the per-user writable directories
	 * ({@code /tmp}, {@code /var/tmp}, {@code /etc/git}), set ownership for the
	 * default home directory, and link the shared bash/git template into the
	 * chroot.
	 * <p>
	 * The bash and git environments are no longer copied per user - they are built
	 * once into a shared template by {@link ChrootTemplate} and symlinked in via
	 * {@link ChrootTemplate#linkInto(String)}, turning seconds of copying into
	 * milliseconds of linking.
	 */
	private void initalizeChrootFolder() {
		timed("configured symlinks", () -> {
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
		});

		timed("/root + /home/default", () -> {
			createChrootDirectory("/root");
			createChrootDirectory("/home/default");
			setDirectoryOwnership("/home/default", "1001", "1001");
		});

		// Per-user writable directories that must NOT come from the shared (read-only)
		// template: git scratch space and a per-user /etc that can hold /etc/git.
		timed("writable dirs", () -> {
			createChrootDirectory("/tmp");
			createChrootDirectory("/var/tmp");
			createChrootDirectory("/etc/git");
		});

		// Link the shared bash/git/cert template instead of copying it per user.
		timed("link bash/git template", () -> ChrootTemplate.linkInto(userChrootFolder));
	}

	/**
	 * Run the given phase body and log its wall-clock duration in milliseconds.
	 * Used for per-phase profiling inside the chroot init sequence.
	 *
	 * @param phase     short label for the log line
	 * @param phaseBody work to run and measure
	 */
	private void timed(String phase, Runnable phaseBody) {
		long t0 = System.nanoTime();
		phaseBody.run();
		classLogger.info("[timing] {} took {} ms", phase, (System.nanoTime() - t0) / 1_000_000L);
	}

	/**
	 * Create a directory (and any missing parents) inside the chroot. Errors are
	 * logged and swallowed.
	 *
	 * @param relativePath path relative to the chroot root; may begin with a slash
	 */
	private void createChrootDirectory(String relativePath) {
		try {
			Path chrootDir = Paths.get(userChrootFolder, relativePath);
			Files.createDirectories(chrootDir);
			classLogger.debug("Created chroot directory: {}", chrootDir);
		} catch (IOException e) {
			classLogger.error("Failed to create chroot directory: {}", relativePath, e);
		}
	}

	/**
	 * Set numeric POSIX ownership on a directory inside the chroot by shelling out
	 * to {@code chown}. Used for paths like {@code /home/default} that must be
	 * owned by the in-chroot UID (typically 1001) regardless of the host UID that
	 * created them. No-op if the path does not exist; failure is logged but not
	 * thrown.
	 *
	 * @param relativePath path relative to the chroot root
	 * @param uid          numeric user ID to set
	 * @param gid          numeric group ID to set
	 */
	private void setDirectoryOwnership(String relativePath, String uid, String gid) {
		try {
			Path chrootDir = Paths.get(userChrootFolder, relativePath);
			if (Files.exists(chrootDir)) {
				Files.setAttribute(chrootDir, "unix:uid", Integer.parseInt(uid));
				Files.setAttribute(chrootDir, "unix:gid", Integer.parseInt(gid));
				classLogger.debug("Set ownership {}:{} for: {}", uid, gid, relativePath);
			}
		} catch (Exception e) {
			classLogger.warn("Could not set ownership for: {}", relativePath, e);
		}
	}

	/**
	 * Copy an entire directory tree from {@code source} to {@code target},
	 * preserving relative structure. Sub-directories are recreated under the target
	 * and files are copied with {@code REPLACE_EXISTING}. No-op if the source does
	 * not exist. Per-file IO failures are wrapped in {@link UncheckedIOException}
	 * so the stream pipeline can propagate them.
	 *
	 * @param source root directory to copy from
	 * @param target root directory to copy into (created as needed)
	 * @throws IOException if walking the source tree fails
	 */
	private void copyDirectoryRecursively(Path source, Path target) throws IOException {
		if (source.toFile().exists()) {
			try (Stream<Path> stream = Files.walk(source)) {
				stream.forEach(src -> {
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
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	/**
	 * Create a symbolic link inside the chroot that points back at a host
	 * directory, mirroring the host path under the chroot root. For example, when
	 * {@code sourceDirName} is {@code /opt/semoss/py}, the link is created at
	 * {@code <chrootRoot>/opt/semoss/py} pointing to the original. The source path
	 * is normalized first, parent directories of the link are created as needed,
	 * and the call is idempotent (skipped if the link already exists). All errors
	 * are logged and swallowed.
	 *
	 * @param sourceDirName absolute host path of the directory to expose inside the
	 *                      chroot
	 */
	public void symlinkFolder(String sourceDirName) {
		sourceDirName = Utility.normalizePath(sourceDirName);
		if (injectMode) {
			enqueueOrInject(sourceDirName, true);
		}
		createChrootSymlink(sourceDirName);
	}

	private void createChrootSymlink(String sourceDirName) {
		classLogger.debug("Making symlink for folder {}", sourceDirName);
		// Convert the source directory and user chroot folder to Path objects
		Path sourceDir = Paths.get(Utility.normalizePath(sourceDirName));
		Path userChrootPath = Paths.get(userChrootFolder);

		classLogger.debug("User chroot path is {}", userChrootFolder);

		// Construct the path for the symbolic link
		Path symlinkPath = userChrootPath.resolve(sourceDirName.substring(1)); // Remove leading slash
		classLogger.debug("Full symlink path is {}", symlinkPath);

		try {
			// Check if the source directory exists
			if (!Files.exists(sourceDir)) {
				throw new IllegalArgumentException("Source directory does not exist: " + sourceDirName);
			}

			// Ensure the parent directories exist for the symlink path
			Files.createDirectories(symlinkPath.getParent());

			// Check if the symlink already exists
			if (Files.exists(symlinkPath)) {
				classLogger.debug("Symbolic link already exists at: {}", symlinkPath);
				// Optionally, delete the existing symlink
				// Files.delete(symlinkPath);
			} else {
				// Create the symbolic link
				Files.createSymbolicLink(symlinkPath, sourceDir);
				classLogger.info("Symbolic link created at: {}", symlinkPath);
			}
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid argument: {}", e.getMessage(), e);
		} catch (IOException e) {
			classLogger.error("Error creating symbolic link: {}", e.getMessage(), e);
		} catch (UnsupportedOperationException e) {
			classLogger.error("Symbolic links are not supported on this file system.", e);
		}
	}

	/**
	 * @return the absolute path of this user's chroot folder (the value passed to
	 *         the constructor)
	 */
	public String getUserChrootFolder() {
		return this.userChrootFolder;
	}

	/**
	 * Recursively delete this user's chroot folder and everything inside it. Errors
	 * are logged and swallowed.
	 */
	public void removeChrootFolder() {
		try {
			FileUtils.deleteDirectory(new File(userChrootFolder));
			classLogger.info("{} Directory and all contents deleted successfully.", userChrootFolder);
		} catch (IOException e) {
			classLogger.error("Error deleting directory: {}", e.getMessage(), e);
		}
	}

	/**
	 * Symlink the given user's personal Asset project folder into the chroot so the
	 * user's saved assets are accessible from inside the sandbox. Looks up the
	 * user's primary login and asset project id, resolves the asset app root folder
	 * via {@link AssetUtility}, then delegates to {@link #symlinkFolder(String)}.
	 *
	 * @param user the user whose asset folder should be exposed inside the chroot
	 */
	public void symlinkUserAsset(User user) {
		AuthProvider provider = user.getPrimaryLogin();
		String projectId = user.getAssetProjectId(provider);
		String assetFolder = AssetUtility.getUserAssetAppRootFolder("Asset", projectId);
		classLogger.info("Symlinking user asset folder for projectId={}", projectId);
		symlinkFolder(assetFolder);
	}

	/**
	 * Expose a project's app root folder inside the chroot with access mode
	 * determined by the user's project permissions:
	 * <ul>
	 * <li>If the user can edit the project, the live project folder is symlinked
	 * into the chroot (writes pass straight through to the host project).</li>
	 * <li>Otherwise, behaviour depends on the {@code CHROOT_READ_ONLY_COPY} flag.
	 * When enabled, a copy of the project is placed inside the chroot and every
	 * file is chmod'd to {@code r-xr-xr-x} so the read-only user cannot modify
	 * project state. When disabled, the project is symlinked as if the user had
	 * edit access.</li>
	 * </ul>
	 * Already-copied targets are detected and skipped on repeat calls.
	 *
	 * @param user      the user whose chroot is being prepared
	 * @param projectId id of the project to expose
	 */
	public void symlinkProject(User user, String projectId) {
		classLogger.info("Symlinking project for projectId={}", projectId);

		String projectAppRootFolder = AssetUtility.getProjectAppRootFolder(projectId);

		boolean canEdit = SecurityProjectUtils.userCanEditProject(user, projectId);

		if (injectMode) {
			enqueueOrInject(Utility.normalizePath(projectAppRootFolder), canEdit);
		}

		if (canEdit) {
			createChrootSymlink(Utility.normalizePath(projectAppRootFolder));
			return;
		}

		boolean readOnlyCopyEnabled = Boolean
				.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_READ_ONLY_COPY));

		if (readOnlyCopyEnabled) {
			Path projectTarget = Paths.get(userChrootFolder)
					.resolve(Utility.normalizePath(projectAppRootFolder).substring(1));
			if (Files.exists(projectTarget)) {
				classLogger.info(
						"Chrooted project already copied for projectId={} at: {}, skipping copy and permission patch.",
						projectId, projectTarget);
				return;
			}

			classLogger.info("Symlinking read-only copy for projectId={}", projectId);
			setupCopiedProject(projectId);
			setAllReadExecuteForProject(projectId);
			// below does not work - commenting out for now
			// setExecuteOnlyOnAssetCodeFolders(projectId);
		} else {
			classLogger.info("Symlinking full folder for read-only user, projectId={}", projectId);
			createChrootSymlink(Utility.normalizePath(projectAppRootFolder));
		}
	}

	/**
	 * Apply {@code rwxr-xr-x} POSIX permissions: owner has full access; group and
	 * others get read and execute.
	 *
	 * @param dir path to chmod
	 * @throws IOException if setting permissions fails
	 */
	private void setOwnerRWX(Path dir) throws IOException {
		Files.setPosixFilePermissions(dir, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
				PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
				PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE));
	}

	/**
	 * Apply {@code r-xr-xr-x} POSIX permissions: read and execute for owner, group,
	 * and others; no write for anyone. Used to lock down a copied project so a
	 * read-only user cannot modify it from inside the chroot.
	 *
	 * @param dir path to chmod
	 * @throws IOException if setting permissions fails
	 */
	private void setOwnerRX(Path dir) throws IOException {
		Files.setPosixFilePermissions(dir,
				EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE,
						PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
						PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE));
	}

	/**
	 * Copy the live project app root into the chroot for a read-only user. Skips
	 * silently if the source does not exist or the target has already been copied.
	 * Part of the read-only-copy strategy in {@link #symlinkProject(User, String)}.
	 *
	 * @param projectId id of the project whose app root should be copied
	 */
	private void setupCopiedProject(String projectId) {
		String sourceDirToCopy = AssetUtility.getProjectAppRootFolder(projectId);
		sourceDirToCopy = Utility.normalizePath(sourceDirToCopy);
		Path projectSource = Paths.get(sourceDirToCopy);
		if (!Files.exists(projectSource)) {
			classLogger.warn("Project app root does not exist for readOnlyCopyProject: {}", sourceDirToCopy);
			return;
		}
		Path projectTarget = Paths.get(userChrootFolder).resolve(sourceDirToCopy.substring(1));
		if (Files.exists(projectTarget)) {
			classLogger.info("Chrooted project already copied, skipping copy for: {}", projectTarget);
			return;
		}
		try {
			copyDirectoryRecursively(projectSource, projectTarget);
			classLogger.info("Copied project from: {}", projectSource);
		} catch (IOException e) {
			classLogger.debug("Could not copy project from: {}", projectSource);
		}
	}

	/**
	 * Walk the chroot copy of the given project and apply {@code r-xr-xr-x}
	 * permissions to every file and directory, locking the read-only user out of
	 * modifying any project content while still allowing traversal and execution.
	 *
	 * @param projectId id of the project whose chroot copy should be made
	 *                  read-only-executable
	 */
	public void setAllReadExecuteForProject(String projectId) {
		String projectAppRootFolder = AssetUtility.getProjectAppRootFolder(projectId);
		projectAppRootFolder = Utility.normalizePath(projectAppRootFolder);
		Path chrootAppRoot = Paths.get(userChrootFolder,
				projectAppRootFolder.startsWith("/") ? projectAppRootFolder.substring(1) : projectAppRootFolder);

		if (!Files.exists(chrootAppRoot)) {
			classLogger.warn("Chrooted app root does not exist for project: {}", chrootAppRoot);
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
			classLogger.error("Failed to set r-x permissions on project: {}", chrootAppRoot, e);
		}
	}

	/**
	 * Walk the chroot copy of the given project's asset code folders ({@code java},
	 * {@code classes}, {@code py}) and strip the read bits, leaving only
	 * {@code --x--x--x} permissions. The intent is to permit execution of compiled
	 * code while preventing inspection of the source.
	 * <p>
	 * Currently does not work in practice - preserved for reference; see the
	 * commented-out call in {@link #symlinkProject(User, String)}.
	 *
	 * @param projectId id of the project whose code folders should be made
	 *                  execute-only
	 */
	public void setExecuteOnlyOnAssetCodeFolders(String projectId) {
		String assetsFolderPath = AssetUtility.getProjectAssetsFolder(projectId);
		assetsFolderPath = Utility.normalizePath(assetsFolderPath);
		Path chrootAssetsFolder = Paths.get(userChrootFolder,
				assetsFolderPath.startsWith("/") ? assetsFolderPath.substring(1) : assetsFolderPath);

		if (!Files.exists(chrootAssetsFolder)) {
			classLogger.warn("Chroot assets folder does not exist: {}", chrootAssetsFolder);
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
					classLogger.warn("Failed to set execute-only on: {}", subDir, e);
				}
			}
		}
	}

}
