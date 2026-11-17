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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.UserAssetUtils;
import prerna.project.api.IProject;
import prerna.util.sandbox.SandboxSettings;

public class SymlinkHelper {

	private final SandboxSettings.Mode sandboxMode;

	private static final Logger classLogger = LogManager.getLogger(SymlinkHelper.class);

	private static final String JAVA_FOLDER = "java";
	private static final String CLASSES_FOLDER = "classes";
	private static final String GIT_FOLDER = ".git";
	private static final String PROJECT_FOLDER_ID_SEPARATOR = "__";

	/**
	 * How long a read-only user's copy of a project is used before the project's
	 * files are looked at again for changes.
	 */
	private static final long COPY_CHECK_GAP_MILLIS = 5_000L;

	/**
	 * The folders of a project that a read-only user's copy leaves out, relative to
	 * the project's app root: the source of its custom reactors, their compiled
	 * classes and its portals, which only the main JVM uses, and the git folder
	 * with the project's history, which a copy nobody can change has no use for.
	 */
	private static final List<Path> FOLDERS_LEFT_OUT_OF_A_COPY = List.of(
			Paths.get(Constants.VERSION_FOLDER, Constants.ASSETS_FOLDER, JAVA_FOLDER),
			Paths.get(Constants.VERSION_FOLDER, Constants.ASSETS_FOLDER, CLASSES_FOLDER),
			Paths.get(Constants.VERSION_FOLDER, Constants.ASSETS_FOLDER, Constants.PORTALS_FOLDER),
			Paths.get(Constants.VERSION_FOLDER, GIT_FOLDER));

	private String userChrootFolder = null;

	// refreshed per call for a helper shared by one user's sessions (OpenShell)
	private volatile User user;
	private final boolean injectMode;
	private SandboxInjector injector;
	private final Map<String, Mount> activeInjects = new LinkedHashMap<>();
	private final Map<String, ProjectCopy> projectCopies = new HashMap<>();
	/**
	 * Paths already handed to the external sandbox, and whether each went as a view-only copy. A
	 * project open, an insight and an MCP call all re-symlink the same folders; without this the
	 * tree would be re-transferred on every request.
	 */
	private final Map<String, Boolean> activeTransports = new ConcurrentHashMap<>();
	private SandboxTransporter transporter;

	/**
	 * Paths this helper has handed to the sandbox, in the order they were first
	 * requested. Read-only view: the fetch-back needs to know which roots exist in
	 * the sandbox so it can reconcile them when the process finishes a block.
	 */
	public List<String> getTransportedPaths() {
		return new ArrayList<>(activeTransports.keySet());
	}

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
		this(targetDirName, null);
	}

	/**
	 * Construct the SymlinkHelper of one user. Knowing its user, the helper puts a
	 * project in the chroot by that user's permission on it whichever folder of the
	 * project it is asked to link; see {@link #symlinkProject(User, String)}.
	 *
	 * @param targetDirName absolute path of the user chroot folder
	 * @param user          the user the chroot folder belongs to, or null
	 */
	public SymlinkHelper(String targetDirName, User user) {
		this.user = user;
		this.userChrootFolder = targetDirName;
		// The mode is resolved once, here, because this object exists only for a
		// user-authored worker. Callers that must NOT be jailed (engine-owned
		// workers, Project python) pass a null helper and never reach this.
		this.sandboxMode = SandboxSettings.mode();
		this.injectMode = this.sandboxMode == SandboxSettings.Mode.NAMESPACE;
		SandboxSettings.logEffectiveOnce();

		File targetDir = new File(Utility.normalizePath(userChrootFolder));
		if (!targetDir.exists()) {
			classLogger.info("User chroot folder doesn't exist. Making folder now at: {}", userChrootFolder);
			if (!targetDir.mkdir()) {
				classLogger.warn("Unable to create the user chroot folder {} - verify the {} directory {} exists",
						userChrootFolder, Constants.CHROOT_DIR, Utility.getDIHelperProperty(Constants.CHROOT_DIR));
			}
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
			for (Map.Entry<String, Mount> entry : activeInjects.entrySet()) {
				try {
					injector.inject(entry.getKey(), entry.getValue().source(), entry.getValue().readWrite());
				} catch (RuntimeException e) {
					classLogger.warn("Skipping namespace injection for {}", entry.getKey(), e);
				}
			}
		}
	}

	/**
	 * Takes a folder back for new opens: withdraws the namespace mount and removes the chroot link.
	 * Files a worker already has open stay open. An OpenShell sandbox keeps what it was sent, so this
	 * does nothing there.
	 *
	 * @param absPath absolute host path
	 * @return true when something was withdrawn
	 */
	public synchronized boolean revoke(String absPath) {
		if (sandboxMode == SandboxSettings.Mode.OPENSHELL || absPath == null) {
			return false;
		}
		String path = Utility.normalizePath(absPath);
		boolean withdrawn = false;
		if (activeInjects.remove(path) != null) {
			withdrawn = true;
			if (injector != null) {
				try {
					injector.remove(path);
				} catch (RuntimeException e) {
					classLogger.warn("Could not withdraw the namespace mount for {}", path, e);
				}
			}
		}
		Path link = Paths.get(userChrootFolder).resolve(path.substring(1));
		try {
			if (Files.isSymbolicLink(link)) {
				Files.delete(link);
				withdrawn = true;
			}
		} catch (IOException e) {
			classLogger.warn("Could not remove the chroot link {}", link, e);
		}
		return withdrawn;
	}

	public boolean isInjectMode() {
		return injectMode;
	}

	/**
	 * Wire the per-mode collaborator that moves paths into an external sandbox.
	 *
	 * <p>
	 * The exact counterpart of {@link #setInjector(SandboxInjector)}: both are set once by
	 * {@code ClientProcessWrapper} when the worker starts, and both are replayed with everything
	 * requested so far, because asset paths are registered before the worker exists.
	 *
	 * @param transporter the transporter for {@link SandboxSettings.Mode#OPENSHELL}
	 */
	public synchronized void setTransport(SandboxTransporter transporter) {
		this.transporter = transporter;
		if (transporter != null && !activeTransports.isEmpty()) {
			for (Map.Entry<String, Boolean> entry : activeTransports.entrySet()) {
				String absPath = entry.getKey();
				try {
					transporter.transport(absPath, entry.getValue());
				} catch (RuntimeException e) {
					classLogger.warn("Skipping sandbox transport for {}", absPath, e);
				}
			}
		}
	}

	/**
	 * Rebinds the user whose permissions decide how a project is placed. A helper that all of
	 * one user's sessions share is rebound to the session that asked most recently.
	 *
	 * @param user the user, or null
	 */
	public void setUser(User user) {
		this.user = user;
	}

	/**
	 * Whether a folder of a project, relative to its app root, is left out of a read-only user's
	 * copy. The OpenShell transport leaves the same folders out of what it sends a view-only user.
	 *
	 * @param relativeFolder the folder relative to the project's app root
	 * @return true when the folder and everything under it is left out
	 */
	static boolean isLeftOutOfCopy(Path relativeFolder) {
		return FOLDERS_LEFT_OUT_OF_A_COPY.contains(relativeFolder);
	}

	/** The user-level jail this helper's worker runs under. */
	public SandboxSettings.Mode getSandboxMode() {
		return this.sandboxMode;
	}

	/**
	 * True when the worker runs inside an external sandbox created ahead of it
	 * (OpenShell), so no in-process jail is applied.
	 */
	public boolean isExternalSandbox() {
		return this.sandboxMode == SandboxSettings.Mode.OPENSHELL;
	}

	/**
	 * The single place a path is handed to the external (OpenShell) sandbox. A path requested before
	 * the worker exists is remembered and replayed by {@link #setTransport(SandboxTransporter)},
	 * exactly as the namespace injector replays {@code activeInjects}.
	 */
	private synchronized void transport(String absPath, boolean viewOnly) {
		// Same scope rule as the namespace injector. Without it the transporter is handed
		// system paths -- /usr/bin, /etc/ssl, /usr/lib/git-core -- because
		// MCPUtility.runPythonTool also symlinks git's own directories. Uploading those is
		// wrong and enormous: observed as several 30s CLI timeouts, a 77s pixel, and the
		// project never arriving. The sandbox image already provides them.
		if (!isNamespaceInjectablePath(absPath)) {
			classLogger.debug("Skipping sandbox transport for path outside SEMOSS home: {}", absPath);
			return;
		}
		Boolean sent = activeTransports.get(absPath);
		if (sent != null && (!sent || viewOnly)) {
			// Already sent once, in full or as the same view-only copy. Only a view-only
			// root that the user can now edit goes again, in full.
			return;
		}
		activeTransports.put(absPath, viewOnly);
		if (transporter != null) {
			try {
				if (!transporter.transport(absPath, viewOnly)) {
					classLogger.warn("Sandbox transport reported failure for {}", absPath);
				}
			} catch (RuntimeException e) {
				classLogger.warn("Sandbox transport failed for {}", absPath, e);
			}
		}
	}

	/**
	 * Mounts a folder in the namespace sandbox at a path, now or as soon as the
	 * sandbox is attached. A path that is mounted differently already, from another
	 * folder or with other access, is mounted again the way it is asked for here.
	 *
	 * @param absPath   the path the folder shows at inside the sandbox
	 * @param source    the folder to mount there; the path itself for the folder at
	 *                  that path
	 * @param readWrite whether the sandbox may write to it
	 */
	private synchronized void enqueueOrInject(String absPath, String source, boolean readWrite) {
		if (!isNamespaceInjectablePath(absPath)) {
			classLogger.warn("Skipping namespace injection for path outside SEMOSS home: {}", absPath);
			return;
		}
		Mount wanted = new Mount(source, readWrite);
		Mount existing = activeInjects.put(absPath, wanted);
		if (injector != null) {
			try {
				if (existing != null && !existing.equals(wanted)) {
					injector.remove(absPath);
				}
				injector.inject(absPath, source, readWrite);
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
	 * Copies a project's app root for a read-only user, keeping the folder
	 * structure and leaving out the custom reactor source, the compiled classes,
	 * the portals and the git folder. The main JVM compiles and loads the reactors
	 * and serves the portals from the project's own folder, and a copy nobody can
	 * change has no use for the project's history, so a user's chroot has no use
	 * for a copy of them. Files are copied with {@code REPLACE_EXISTING}. Nothing
	 * is copied when the app root does not exist.
	 *
	 * @param appRoot the project's app root folder
	 * @param target  the folder to copy into, created as needed
	 * @throws IOException          if walking the app root fails
	 * @throws UncheckedIOException if a folder cannot be created or a file cannot
	 *                              be copied
	 */
	static void copyProjectForReadOnlyUser(Path appRoot, Path target) throws IOException {
		if (!Files.exists(appRoot)) {
			return;
		}
		Files.walkFileTree(appRoot, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
				Path relative = appRoot.relativize(dir);
				if (FOLDERS_LEFT_OUT_OF_A_COPY.contains(relative)) {
					return FileVisitResult.SKIP_SUBTREE;
				}
				try {
					Files.createDirectories(target.resolve(relative));
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
				try {
					Files.copy(file, target.resolve(appRoot.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * Create a symbolic link inside the chroot that points back at a host
	 * directory, mirroring the host path under the chroot root. For example, when
	 * {@code sourceDirName} is {@code /opt/semoss/py}, the link is created at
	 * {@code <chrootRoot>/opt/semoss/py} pointing to the original. The source path
	 * is normalized first, parent directories of the link are created as needed,
	 * and the call is idempotent (skipped if the link already exists). All errors
	 * are logged and swallowed.
	 * <p>
	 * A folder that is a project's app root, or inside one, is not linked on its
	 * own: the whole project is put in the chroot by the user's permission on it,
	 * as {@link #symlinkProject(User, String)} does.
	 *
	 * @param sourceDirName absolute host path of the directory to expose inside the
	 *                      chroot
	 */
	public void symlinkFolder(String sourceDirName) {
		sourceDirName = Utility.normalizePath(sourceDirName);
		ProjectFolder project = sourceDirName == null || this.user == null ? null
				: ProjectFolder.of(Paths.get(sourceDirName), Paths.get(Utility.getBaseFolder()));
		if (project != null) {
			try {
				placeProject(this.user, project.projectId(), project.appRoot().toString());
			} catch (RuntimeException e) {
				classLogger.error("Unable to put project {} in the user's folder for {}", project.projectId(),
						sourceDirName, e);
			}
			return;
		}
		if (sandboxMode == SandboxSettings.Mode.OPENSHELL) {
			// The sandbox has its own filesystem root; a chroot symlink under the host user folder
			// would point at a path the sandbox cannot see, so it is meaningless here.
			transport(sourceDirName, false);
			return;
		}
		if (injectMode) {
			enqueueOrInject(sourceDirName, sourceDirName, true);
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
			classLogger.error("Unable to symlink {} into the chroot {} - the source directory does not exist",
					sourceDirName, userChrootFolder, e);
		} catch (IOException e) {
			classLogger.error("Failed to create the symlink {} pointing at {}", symlinkPath, sourceDirName, e);
		} catch (UnsupportedOperationException e) {
			classLogger.error("Symbolic links are not supported on this file system - unable to link {} into {}",
					sourceDirName, userChrootFolder, e);
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
	public synchronized void removeChrootFolder() {
		for (ProjectCopy copy : this.projectCopies.values()) {
			try {
				makeWritable(copy.folder());
			} catch (IOException | UncheckedIOException e) {
				classLogger.warn("Unable to make the project copy {} writable before it is deleted", copy.folder(), e);
			}
		}
		this.projectCopies.clear();
		try {
			FileUtils.deleteDirectory(new File(userChrootFolder));
			classLogger.info("Deleted the chroot folder {} and all its contents", userChrootFolder);
		} catch (IOException e) {
			classLogger.error("Failed to delete the chroot folder {}", userChrootFolder, e);
		}
	}

	/**
	 * Symlink the given user's personal Asset project folder into the chroot so the
	 * user's saved assets are accessible from inside the sandbox. The asset project
	 * is resolved through {@link User#getAssetProject(AuthProvider)} so that it is
	 * created, pulled and opened before we try to link it - opening the project is
	 * what materializes the asset folders on this instance. The app root folder is
	 * then resolved via {@link AssetUtility} and passed to
	 * {@link #symlinkFolder(String)}.
	 *
	 * @param user the user whose asset folder should be exposed inside the chroot
	 */
	public void symlinkUserAsset(User user) {
		AuthProvider provider = user.getPrimaryLogin();
		String projectId = user.getAssetProjectId(provider);
		if (projectId == null) {
			classLogger.warn("Unable to resolve the user asset project - skipping the user asset folder symlink");
			return;
		}

		String projectName = UserAssetUtils.ASSET_APP_NAME;
		IProject assetProject = user.getAssetProject(provider);
		if (assetProject == null) {
			classLogger.warn("Unable to open the user asset project {} - the asset folder will be created empty",
					projectId);
		} else if (assetProject.getProjectName() != null) {
			projectName = assetProject.getProjectName();
		}

		String assetFolder = AssetUtility.getUserAssetAppRootFolder(projectName, projectId);
		classLogger.info("Symlinking user asset folder for projectId={}", projectId);
		symlinkFolder(assetFolder);
	}

	/**
	 * Puts a project in the chroot by what the user may do with it:
	 * <ul>
	 * <li>A user who can edit the project gets a symlink to the project's own app
	 * root, so their changes are the project's. In the namespace sandbox the app
	 * root is mounted read-write.</li>
	 * <li>Any other user gets a copy, which nobody can write to and which leaves
	 * out the custom reactor source, the compiled classes and the portals, since
	 * only the main JVM uses those, and the git folder. In the namespace sandbox the copy is mounted
	 * read-only at the app root's path. What a project has for the people who only
	 * view it goes in its other folders, such as its public folder.</li>
	 * </ul>
	 * The copy is made again when the project's files have changed, and a change
	 * of the user's permission is followed the next time this is called.
	 *
	 * @param user      the user whose chroot is being prepared
	 * @param projectId id of the project to expose
	 * @throws UncheckedIOException if the copy cannot be made
	 */
	public void symlinkProject(User user, String projectId) {
		placeProject(user, projectId, Utility.normalizePath(AssetUtility.getProjectAppRootFolder(projectId)));
	}

	private synchronized void placeProject(User user, String projectId, String appRoot) {
		if (sandboxMode == SandboxSettings.Mode.OPENSHELL) {
			// The sandbox has its own filesystem; the host chroot (symlink or viewer copy) is
			// invisible to it. A user who can only view the project gets the same copy as in
			// the other modes, without the folders it leaves out. Write-back is gated by the
			// user's edit permission separately.
			transport(appRoot, !SecurityProjectUtils.userCanEditProject(user, projectId));
			return;
		}
		Path inChroot = Paths.get(userChrootFolder).resolve(appRoot.substring(1));
		try {
			if (SecurityProjectUtils.userCanEditProject(user, projectId)) {
				classLogger.info("Symlinking project for an editor, projectId={}", projectId);
				this.projectCopies.remove(projectId);
				removeCopy(inChroot);
				if (injectMode) {
					enqueueOrInject(appRoot, appRoot, true);
				}
				createChrootSymlink(appRoot);
				return;
			}
			refreshProjectCopy(projectId, Paths.get(appRoot), inChroot);
			if (injectMode) {
				enqueueOrInject(appRoot, inChroot.toString(), false);
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Unable to put project " + projectId + " in the user's folder", e);
		}
	}

	/**
	 * Makes a read-only user's copy of a project, or makes it again when the
	 * project's files changed since it was made. The files are looked at for
	 * changes at most once every {@link #COPY_CHECK_GAP_MILLIS}.
	 *
	 * @param projectId the project
	 * @param appRoot   the project's own app root
	 * @param copy      where the copy goes in the chroot
	 * @throws IOException if the project cannot be read or the copy cannot be made
	 */
	private void refreshProjectCopy(String projectId, Path appRoot, Path copy) throws IOException {
		if (!Files.exists(appRoot)) {
			classLogger.warn("Project app root does not exist, so there is nothing to copy: {}", appRoot);
			return;
		}
		long now = System.currentTimeMillis();
		ProjectCopy last = this.projectCopies.get(projectId);
		boolean copied = last != null && Files.isDirectory(copy, LinkOption.NOFOLLOW_LINKS);
		if (copied && now - last.checkedAt() < COPY_CHECK_GAP_MILLIS) {
			return;
		}
		ProjectFiles files = ProjectFiles.of(appRoot);
		if (copied && last.files().equals(files)) {
			this.projectCopies.put(projectId, new ProjectCopy(copy, files, now));
			return;
		}
		classLogger.info("Copying project for a read-only user, projectId={}", projectId);
		copyForViewer(appRoot, copy);
		this.projectCopies.put(projectId, new ProjectCopy(copy, files, now));
	}

	/**
	 * Makes the copy of a project that a user who can only view it gets, in place
	 * of whatever is at that path in the chroot: the symlink they had as an editor,
	 * or an earlier copy, whose folder is kept and filled again so a mount of it
	 * stays good. Nobody can write to the copy.
	 *
	 * @param appRoot the project's own app root
	 * @param copy    where the copy goes in the chroot
	 * @throws IOException if the project cannot be read or the copy cannot be made
	 */
	static void copyForViewer(Path appRoot, Path copy) throws IOException {
		if (Files.isSymbolicLink(copy)) {
			Files.delete(copy);
		} else if (Files.isDirectory(copy, LinkOption.NOFOLLOW_LINKS)) {
			makeWritable(copy);
			FileUtils.cleanDirectory(copy.toFile());
		}
		copyProjectForReadOnlyUser(appRoot, copy);
		makeReadOnly(copy);
	}

	/**
	 * Removes a project copy from the chroot, so the project's own app root can be
	 * linked at that path for a user who can now edit it. A symlink at the path is
	 * left as it is.
	 *
	 * @param inChroot the project's path in the chroot
	 * @throws IOException if the copy cannot be deleted
	 */
	static void removeCopy(Path inChroot) throws IOException {
		if (Files.isDirectory(inChroot, LinkOption.NOFOLLOW_LINKS)) {
			makeWritable(inChroot);
			FileUtils.deleteDirectory(inChroot.toFile());
		}
	}

	/**
	 * Takes write access away from every folder and file of a project copy, for
	 * its owner, its group and everyone else, so nobody changes it from inside the
	 * chroot. Symlinks are not followed.
	 *
	 * @param copy the copy's top folder
	 * @throws IOException if a folder or file cannot be changed
	 */
	static void makeReadOnly(Path copy) throws IOException {
		setPermissions(copy,
				EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE,
						PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
						PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE));
	}

	/**
	 * Gives the owner write access to every folder and file of a project copy
	 * again, so the copy can be replaced or deleted.
	 *
	 * @param copy the copy's top folder
	 * @throws IOException if a folder or file cannot be changed
	 */
	static void makeWritable(Path copy) throws IOException {
		if (!Files.isDirectory(copy, LinkOption.NOFOLLOW_LINKS)) {
			return;
		}
		setPermissions(copy,
				EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
						PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ,
						PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ,
						PosixFilePermission.OTHERS_EXECUTE));
	}

	private static void setPermissions(Path top, Set<PosixFilePermission> permissions) throws IOException {
		Files.walkFileTree(top, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				Files.setPosixFilePermissions(dir, permissions);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (!attrs.isSymbolicLink()) {
					Files.setPosixFilePermissions(file, permissions);
				}
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * A folder mounted in the namespace sandbox.
	 *
	 * @param source    the folder that is mounted
	 * @param readWrite whether the sandbox may write to it
	 */
	private record Mount(String source, boolean readWrite) {
	}

	/**
	 * A project's app root, found from a folder that is the app root or inside it.
	 *
	 * @param projectId the project
	 * @param appRoot   the project's app root folder
	 */
	record ProjectFolder(String projectId, Path appRoot) {

		/**
		 * Finds the project a folder belongs to.
		 *
		 * @param folder     an absolute folder path
		 * @param baseFolder the SEMOSS home, which holds the project folders
		 * @return the project, or null when the folder is not part of a project
		 */
		static ProjectFolder of(Path folder, Path baseFolder) {
			Path projects = baseFolder.resolve(Constants.PROJECT_FOLDER).toAbsolutePath().normalize();
			Path candidate = folder.toAbsolutePath().normalize();
			int depth = projects.getNameCount();
			if (!candidate.startsWith(projects) || candidate.getNameCount() < depth + 2
					|| !Constants.APP_ROOT_FOLDER.equals(candidate.getName(depth + 1).toString())) {
				return null;
			}
			String projectFolder = candidate.getName(depth).toString();
			int idAt = projectFolder.lastIndexOf(PROJECT_FOLDER_ID_SEPARATOR);
			if (idAt < 0) {
				return null;
			}
			return new ProjectFolder(projectFolder.substring(idAt + PROJECT_FOLDER_ID_SEPARATOR.length()),
					projects.resolve(projectFolder).resolve(Constants.APP_ROOT_FOLDER));
		}
	}

	/**
	 * A read-only user's copy of a project.
	 *
	 * @param folder    the copy's top folder in the chroot
	 * @param files     the project's files when the copy was made
	 * @param checkedAt when the project's files were last looked at for changes
	 */
	private record ProjectCopy(Path folder, ProjectFiles files, long checkedAt) {
	}

	/**
	 * What tells one state of a project's files from another: how many files a
	 * read-only copy takes, their total size, and when the newest was changed. The
	 * folders the copy leaves out do not count.
	 *
	 * @param count  the number of files
	 * @param bytes  their total size
	 * @param newest when the newest file or folder was last changed
	 */
	record ProjectFiles(long count, long bytes, long newest) {

		static ProjectFiles of(Path appRoot) throws IOException {
			long[] totals = new long[3];
			Files.walkFileTree(appRoot, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
					if (FOLDERS_LEFT_OUT_OF_A_COPY.contains(appRoot.relativize(dir))) {
						return FileVisitResult.SKIP_SUBTREE;
					}
					totals[2] = Math.max(totals[2], attrs.lastModifiedTime().toMillis());
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
					totals[0]++;
					totals[1] += attrs.size();
					totals[2] = Math.max(totals[2], attrs.lastModifiedTime().toMillis());
					return FileVisitResult.CONTINUE;
				}
			});
			return new ProjectFiles(totals[0], totals[1], totals[2]);
		}
	}

}
