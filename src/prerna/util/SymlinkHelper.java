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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;

public class SymlinkHelper {

	private static final Logger classLogger = LogManager.getLogger(SymlinkHelper.class);

	private String userChrootFolder = null;

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

		long startNanos = System.nanoTime();
		initalizeChrootFolder();
		long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
		classLogger.info("Chroot init completed in {} ms for: {}", elapsedMs, userChrootFolder);
	}

	/**
	 * Run the cross-platform chroot initialization sequence: symlink configured
	 * paths (Python base folder, insight cache, and any extras from
	 * {@code CHROOT_SYMLINK_PATHS}), create the standard mount points
	 * ({@code /root}, {@code /home/default}), set ownership for the default home
	 * directory, and populate the bash and git environments inside the chroot.
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

		timed("bash setup", this::setupBashForChroot);
		timed("git setup (total)", this::setupGitForChroot);
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
		classLogger.debug("[timing] {} took {} ms", phase, (System.nanoTime() - t0) / 1_000_000L);
	}

	/**
	 * Populate the chroot with a minimal bash environment: the bash and sh
	 * binaries, common coreutils commands (ls, echo, mkdir, touch, cp, mv, rm, cat,
	 * pwd), identity commands (whoami, id), and every shared library reported by
	 * {@code ldd} on those binaries. Library lookups and copies are batched via
	 * {@link #copyBinariesAndLibraries(Collection)} so each unique library is
	 * resolved and copied exactly once.
	 */
	private void setupBashForChroot() {
		try {
			// Create essential directories
			createChrootDirectory("bin");
			createChrootDirectory("lib");
			createChrootDirectory("lib64");

			List<String> binaries = List.of("/bin/bash", "/bin/sh", "/usr/bin/coreutils", "/bin/ls", "/bin/echo",
					"/bin/mkdir", "/bin/touch", "/bin/cp", "/bin/mv", "/bin/rm", "/bin/cat", "/bin/pwd",
					"/usr/bin/whoami", "/usr/bin/id");

			copyBinariesAndLibraries(binaries);

			classLogger.info("Bash and essential commands setup completed for chroot: {}", userChrootFolder);
		} catch (Exception e) {
			classLogger.error("Error setting up bash for chroot: {}", e.getMessage(), e);
		}
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
			classLogger.debug("Created chroot directory: " + chrootDir);
		} catch (IOException e) {
			classLogger.error("Failed to create chroot directory: " + relativePath, e);
		}
	}

	/**
	 * Copy a host binary into the chroot at the same relative path and mark it
	 * executable. Creates parent directories as needed. No-op (with a warning) if
	 * the source does not exist.
	 *
	 * @param binaryPath absolute path of the host binary (e.g. {@code /bin/bash})
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
	 * Invoke {@code ldd} against a binary and parse the output into the set of
	 * absolute library paths it depends on. Handles both the
	 * {@code libname => /path/to/lib (0x...)} form and direct
	 * {@code /lib...}-rooted entries (like the ELF interpreter). Returns an empty
	 * set on failure.
	 *
	 * @param binaryPath absolute path of the binary to inspect
	 * @return set of absolute library paths reported by {@code ldd}; empty if
	 *         {@code ldd} failed or the binary has no dynamic dependencies
	 */
	private Set<String> resolveRequiredLibraries(String binaryPath) {
		Set<String> libs = new HashSet<>();
		try {
			ProcessBuilder pb = new ProcessBuilder("ldd", binaryPath);
			Process process = pb.start();

			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				String line;
				while ((line = reader.readLine()) != null) {
					// Parse ldd output to extract library paths
					// Format: libname.so => /path/to/lib (0x...)
					if (line.contains("=>") && line.contains("/")) {
						String[] parts = line.split("=>");
						if (parts.length == 2) {
							String libPath = parts[1].trim().split("\\s+")[0];
							if (libPath.startsWith("/")) {
								libs.add(libPath);
							}
						}
					}
					// Handle direct library paths (like /lib64/ld-linux-x86-64.so.2)
					else if (line.trim().startsWith("/lib")) {
						String libPath = line.trim().split("\\s+")[0];
						libs.add(libPath);
					}
				}
			}

			process.waitFor();
		} catch (Exception e) {
			classLogger.error("Failed to resolve libraries for: {}", binaryPath, e);
		}
		return libs;
	}

	/**
	 * Copy a batch of system binaries and every shared library they depend on into
	 * the chroot. Binaries that do not exist on the host are dropped from the
	 * batch. Library dependencies are resolved via {@code ldd} in parallel and
	 * deduplicated across all binaries, so each unique library is copied exactly
	 * once even when many binaries share it (e.g. libc, ld-linux). The binary and
	 * library copies themselves are also performed in parallel; each destination
	 * path is unique so writes do not collide.
	 *
	 * @param binaryPaths host-absolute binary paths to copy; duplicates and missing
	 *                    entries are tolerated
	 */
	private void copyBinariesAndLibraries(Collection<String> binaryPaths) {
		// Dedupe and keep only binaries that exist on the host
		List<String> existing = new LinkedHashSet<>(binaryPaths).stream().filter(p -> Files.exists(Paths.get(p)))
				.collect(Collectors.toList());

		// Resolve libraries in parallel, dedupe across all binaries
		Set<String> uniqueLibraries = existing.parallelStream().flatMap(b -> resolveRequiredLibraries(b).stream())
				.collect(Collectors.toSet());

		// Copy binaries and libraries in parallel; each path is unique so writes don't
		// collide
		existing.parallelStream().forEach(this::copySystemBinary);
		uniqueLibraries.parallelStream().forEach(this::copyLibraryIfExists);
	}

	/**
	 * Copy a single shared library into the chroot at the same relative path.
	 * Silently no-ops if the source does not exist on the host, or if the target
	 * has already been brought in by a previous call. Errors are logged at debug
	 * and swallowed.
	 *
	 * @param libraryPath absolute path of the host library
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
	 * Populate the chroot with everything needed to run {@code git} clones and
	 * fetches: the git binary, helper programs ({@code git-core}), network/SSL
	 * binaries (curl, ssh, wget, openssl), archive tools (tar, gzip),
	 * cross-platform git templates, CA certificates, and working directories
	 * ({@code /tmp}, {@code /var/tmp}, {@code /etc/git}). The git binary plus its
	 * helper binaries are gathered into a single batch so {@code ldd} dedupes
	 * libraries across them. Works on both RHEL/UBI and Ubuntu hosts.
	 */
	private void setupGitForChroot() {
		try {
			// Git binary + essential helper binaries: one batch, deduped ldd, deduped
			// libraries
			List<String> gitBinaries = List.of("/usr/bin/git", "/usr/bin/curl", // Required for git-remote-https
					"/usr/bin/ssh", // For git@... URLs
					"/bin/tar", // Archive operations
					"/bin/gzip", // Compression
					"/usr/bin/openssl", // SSL/TLS operations
					"/usr/bin/wget"); // Alternative HTTP client
			timed("git binaries + ldd", () -> copyBinariesAndLibraries(gitBinaries));

			timed("git-core helpers", this::setupGitHelpersForCrossPlatform);
			timed("git-specific libs", this::copyGitSpecificLibraries);
			timed("git templates", this::setupGitTemplatesForCrossPlatform);
			timed("ssl certs", this::setupSSLCertsForCrossPlatform);

			// Create necessary directories
			createChrootDirectory("/tmp");
			createChrootDirectory("/var/tmp");
			createChrootDirectory("/etc/git");

			classLogger.info("Git setup completed for chroot: {}", userChrootFolder);

		} catch (Exception e) {
			classLogger.error("Error setting up git for chroot: {}", e.getMessage(), e);
		}
	}

	/**
	 * Locate and copy the {@code git-core} helper directory into the chroot. Tries
	 * the RHEL/UBI path {@code /usr/libexec/git-core} first, then the Ubuntu path
	 * {@code /usr/lib/git-core}, and finally falls back to asking git itself via
	 * {@link #findAndCopyGitHelpers()}.
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
	 * Last-resort discovery of the git helpers directory by running
	 * {@code git --exec-path} and copying whatever it reports into the chroot. Used
	 * when neither the RHEL nor Ubuntu canonical paths exist.
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
	 * Copy git-related shared libraries (libcurl, libssl, libcrypto, libz,
	 * libgssapi_krb5) that {@code ldd} on the git binary may not surface on every
	 * distro (dynamically loaded by curl backends, krb5 plugins, etc.). Tries the
	 * RHEL/UBI {@code /usr/lib64} paths first; if none are found, falls back to the
	 * Ubuntu {@code /usr/lib/x86_64-linux-gnu} paths.
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
	 * Copy the git templates directory (the source for {@code git init}'s default
	 * hooks, info, and exclude files) into the chroot. Tries
	 * {@code /usr/share/git-core/templates} first, then
	 * {@code /usr/local/share/git-core/templates}, and stops at the first location
	 * found.
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
	 * Copy the system CA certificate bundle and trust store into the chroot so git,
	 * curl, and friends can verify HTTPS endpoints. Picks an OS-specific routine
	 * based on {@link #isRHELBasedSystem()}, and on unexpected failure runs both
	 * routines as a fallback.
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
	 * Heuristic check for a RHEL/UBI host. Returns true if any of
	 * {@code /etc/redhat-release}, {@code /etc/rhel-release}, or {@code /etc/pki}
	 * exist on the host.
	 *
	 * @return {@code true} if the host appears to be RHEL-based
	 */
	private boolean isRHELBasedSystem() {
		// Check for RHEL-specific files/directories
		return Files.exists(Paths.get("/etc/redhat-release")) || Files.exists(Paths.get("/etc/rhel-release"))
				|| Files.exists(Paths.get("/etc/pki"));
	}

	/**
	 * Copy the RHEL/UBI CA certificate bundle files
	 * ({@code /etc/pki/tls/certs/ca-bundle.crt} and the extracted PEM bundle) plus
	 * the supporting {@code /etc/pki} trust directories into the chroot. Missing
	 * sources are logged as warnings and skipped.
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
	 * Copy the Ubuntu CA certificate bundle
	 * ({@code /etc/ssl/certs/ca-certificates.crt}) and the supporting trust
	 * directories ({@code /etc/ssl/certs}, {@code /usr/share/ca-certificates}) into
	 * the chroot. The certificate bundle file is copied with
	 * {@link StandardCopyOption#COPY_ATTRIBUTES} so the per-CA symlinks under
	 * {@code /etc/ssl/certs} continue to resolve correctly.
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
			classLogger.info(userChrootFolder + " Directory and all contents deleted successfully.");
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, "Error deleting directory: " + e.getMessage());
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
		classLogger.info("Symlinking user asset folder for projectId=" + projectId);
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
		classLogger.info("Symlinking project for projectId=" + projectId);

		String projectAppRootFolder = AssetUtility.getProjectAppRootFolder(projectId);

		if (SecurityProjectUtils.userCanEditProject(user, projectId)) {
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

	/**
	 * Walk the chroot copy of the given project's asset code folders ({@code java},
	 * {@code classes}, {@code py}) and strip the read bits, leaving only
	 * {@code --x--x--x} permissions. The intent is to permit execution of compiled
	 * code while preventing inspection of the source.
	 * <p>
	 * Currently does not work in practice — preserved for reference; see the
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
