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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Builds a single, shared, read-only "skeleton" chroot (bash + coreutils + git
 * + CA certs) <b>once per JVM</b>, then exposes it to per-user chroots via
 * symlinks instead of copying the whole tree into every user's chroot folder.
 * <p>
 * Historically {@link SymlinkHelper} copied bash and git (running {@code ldd}
 * on every binary and byte-copying each library, helper, template, and
 * certificate) into <i>each</i> user chroot. Because that tree is
 * byte-identical for every user, this class builds it exactly once into
 * {@code <CHROOT_DIR>/.template} and {@link #linkInto(String)} then drops a
 * handful of symlinks into each user chroot - reducing per-user setup from
 * seconds of copying to milliseconds of linking.
 * <p>
 * This works because the runtime uses {@code fakechroot} (an LD_PRELOAD path
 * rewriter), not a real kernel {@code chroot}: symlink targets are resolved by
 * the kernel against the real root, so a link such as
 * {@code <userChroot>/bin -> <CHROOT_DIR>/.template/bin} resolves to the real
 * template files when a command runs under
 * {@code fakechroot fakeroot chroot <userChroot> /bin/bash}. Verified
 * end-to-end (bash + interpreter, coreutils, {@code git --version}, and
 * {@code git clone} over HTTPS) on RHEL- and Ubuntu-style hosts.
 *
 * @see SymlinkHelper
 */
public class ChrootTemplate {

	private static final Logger classLogger = LogManager.getLogger(ChrootTemplate.class);

	/** Folder name (under {@code CHROOT_DIR}) that holds the shared template. */
	public static final String TEMPLATE_DIR_NAME = ".template";

	/**
	 * Marker file written only after a full build completes (see {@link #build()}).
	 */
	private static final String READY_MARKER = ".ready";

	/**
	 * Top-level directories that are whole-directory symlinked from the template
	 * into each user chroot. {@code usr} intentionally covers the git-core helpers
	 * (under {@code /usr/libexec/git-core} or {@code /usr/lib/git-core}) and the
	 * git templates (under {@code /usr/share/git-core/templates}) so they come
	 * along for free. {@code etc} is handled separately (see
	 * {@link #linkInto(String)}) because the user chroot needs a writable, per-user
	 * {@code /etc} (e.g. {@code /etc/git}).
	 */
	private static final Set<String> WHOLE_DIR_LINK_SKIP = Set.of("etc", READY_MARKER);

	/**
	 * Subdirectories under {@code /etc} that must never be linked (per-user
	 * writable).
	 */
	private static final Set<String> ETC_NO_LINK = Set.of("git");

	/**
	 * One-time build future; {@code null} until the first
	 * {@link #warmAsync()}/{@link #awaitReady()}.
	 */
	private static volatile CompletableFuture<Path> BUILD;

	/**
	 * Wall-clock nanos captured when the build was first kicked off (warmAsync or
	 * awaitReady).
	 */
	private static volatile long buildStartNanos;

	/** Ensures the "ready after N ms" total is logged exactly once. */
	private static final AtomicBoolean READY_LOGGED = new AtomicBoolean(false);

	private final Path root;

	/**
	 * Bind a builder instance to the template root directory it will populate.
	 *
	 * @param root absolute path of the shared template directory (typically
	 *             {@link #getTemplateRoot()})
	 */
	private ChrootTemplate(Path root) {
		this.root = root;
	}

	// =====================================================================
	// Public API
	// =====================================================================

	/**
	 * @return the absolute path of the shared template directory
	 *         ({@code <CHROOT_DIR>/.template})
	 */
	public static Path getTemplateRoot() {
		String chrootDir = Utility.getDIHelperProperty(Constants.CHROOT_DIR);
		return Paths.get(chrootDir, TEMPLATE_DIR_NAME);
	}

	/**
	 * Kick off the one-time template build on a background virtual thread without
	 * blocking. Idempotent - safe to call from server startup so the template is
	 * warm before the first user arrives. Subsequent calls (and
	 * {@link #awaitReady()}) join the same build.
	 */
	public static void warmAsync() {
		ensureBuild();
	}

	/**
	 * Block until the shared template is fully built, building it on demand if no
	 * warm-up was started. Cheap once the build has completed (a single future
	 * join).
	 *
	 * @return the template root path
	 * @throws IllegalStateException if the build failed
	 */
	public static Path awaitReady() {
		try {
			Path root = ensureBuild().join();
			if (READY_LOGGED.compareAndSet(false, true)) {
				long elapsedMs = (System.nanoTime() - buildStartNanos) / 1_000_000L;
				classLogger.info("ChrootTemplate ready {} ms after build kickoff", elapsedMs);
			}
			return root;
		} catch (CompletionException e) {
			Throwable cause = e.getCause() != null ? e.getCause() : e;
			throw new IllegalStateException("Chroot template build failed: " + cause.getMessage(), cause);
		}
	}

	/**
	 * Symlink the shared template's subtrees into a user chroot. Creates
	 * whole-directory symlinks for {@code bin/lib/lib64/usr}, then creates a
	 * <i>real</i> per-user {@code /etc} and links only the certificate
	 * subdirectories (e.g. {@code etc/ssl} or {@code etc/pki}) into it so per-user
	 * {@code /etc} content (such as {@code /etc/git}) still works. Blocks on
	 * {@link #awaitReady()} first. Idempotent: existing links are left in place.
	 *
	 * @param userChrootFolder absolute path of the user chroot to populate
	 */
	public static void linkInto(String userChrootFolder) {
		Path tmpl = awaitReady();
		Path userRoot = Paths.get(userChrootFolder);
		try (DirectoryStream<Path> children = Files.newDirectoryStream(tmpl)) {
			for (Path child : children) {
				String name = child.getFileName().toString();
				if (WHOLE_DIR_LINK_SKIP.contains(name)) {
					continue;
				}
				linkOne(userRoot.resolve(name), child);
			}
		} catch (IOException e) {
			classLogger.error("Failed to link template subtrees into {}", userChrootFolder, e);
		}

		// /etc stays a real, per-user directory; only cert subdirs are linked in.
		Path tmplEtc = tmpl.resolve("etc");
		if (Files.isDirectory(tmplEtc)) {
			Path userEtc = userRoot.resolve("etc");
			try {
				Files.createDirectories(userEtc);
				try (DirectoryStream<Path> certDirs = Files.newDirectoryStream(tmplEtc)) {
					for (Path sub : certDirs) {
						String name = sub.getFileName().toString();
						if (ETC_NO_LINK.contains(name)) {
							continue;
						}
						linkOne(userEtc.resolve(name), sub);
					}
				}
			} catch (IOException e) {
				classLogger.error("Failed to link template /etc cert dirs into {}", userChrootFolder, e);
			}
		}
	}

	/**
	 * Create a single symbolic link if it does not already exist. The link target
	 * is the absolute template path, which resolves correctly under fakechroot.
	 */
	private static void linkOne(Path link, Path target) {
		try {
			if (Files.exists(link, LinkOption.NOFOLLOW_LINKS)) {
				return;
			}
			Files.createDirectories(link.getParent());
			Files.createSymbolicLink(link, target);
			classLogger.debug("Linked {} -> {}", link, target);
		} catch (IOException e) {
			classLogger.error("Failed to link {} -> {}: {}", link, target, e.getMessage());
		}
	}

	// =====================================================================
	// One-time build orchestration
	// =====================================================================

	/**
	 * Return the shared one-time build future, creating and starting it on the
	 * first call. The build runs on a background virtual thread; the kickoff
	 * timestamp is recorded (for the "ready after N ms" log) and a kickoff line is
	 * logged. Thread-safe: the future is created at most once under class-level
	 * synchronization, so concurrent callers all join the same build.
	 *
	 * @return the future that completes with the template root once built
	 */
	private static CompletableFuture<Path> ensureBuild() {
		CompletableFuture<Path> f = BUILD;
		if (f != null) {
			return f;
		}
		synchronized (ChrootTemplate.class) {
			if (BUILD == null) {
				buildStartNanos = System.nanoTime();
				classLogger.info("ChrootTemplate build kicked off (target: {})", getTemplateRoot());
				CompletableFuture<Path> cf = new CompletableFuture<>();
				Thread.ofVirtual().name("chroot-template-build").start(() -> {
					try {
						cf.complete(new ChrootTemplate(getTemplateRoot()).build());
					} catch (Throwable t) {
						cf.completeExceptionally(t);
					}
				});
				BUILD = cf;
			}
			return BUILD;
		}
	}

	/**
	 * Build (or reuse) the template tree. A completion marker
	 * ({@value #READY_MARKER}) is written only after a full build succeeds; if it
	 * is present, a previous run already populated this template (e.g. a JVM
	 * restart within the same container) and it is reused as-is. If the directory
	 * exists without the marker, a prior build was interrupted, so the partial tree
	 * is cleared and rebuilt.
	 * <p>
	 * No tool versions are tracked: this runs in a container, so a bash/git upgrade
	 * ships as a fresh image with an empty {@code CHROOT_DIR} that builds cleanly
	 * on first boot.
	 */
	private Path build() throws IOException {
		Path marker = root.resolve(READY_MARKER);
		if (Files.exists(marker)) {
			classLogger.info("Chroot template already present at {}", root);
			return root;
		}
		// No marker: brand new, or a previous build was interrupted. Clear any partial
		// tree and build from scratch.
		if (Files.exists(root)) {
			FileUtils.deleteDirectory(root.toFile());
		}

		long t0 = System.nanoTime();
		Files.createDirectories(root);
		timed("bash tier", this::setupBash);
		timed("git tier", this::setupGit);
		Files.createFile(marker);
		classLogger.info("Chroot template built in {} ms at {}", (System.nanoTime() - t0) / 1_000_000L, root);
		return root;
	}

	/**
	 * Run a build phase and log its wall-clock duration at INFO. Used to profile
	 * the one-time template build (bash tier, git tier, and the git sub-steps).
	 *
	 * @param phase     short label for the log line
	 * @param phaseBody work to run and measure
	 */
	private void timed(String phase, Runnable phaseBody) {
		long t0 = System.nanoTime();
		phaseBody.run();
		classLogger.info("ChrootTemplate [{}] took {} ms", phase, (System.nanoTime() - t0) / 1_000_000L);
	}

	// =====================================================================
	// Bash tier (moved from SymlinkHelper, now writing into the template root)
	// =====================================================================

	/**
	 * Populate the template with a minimal bash environment: the bash and sh
	 * binaries, common coreutils commands (ls, echo, mkdir, touch, cp, mv, rm, cat,
	 * pwd), identity commands (whoami, id), and every shared library {@code ldd}
	 * reports for them. Errors are logged and swallowed so a partial failure does
	 * not abort the whole build.
	 */
	private void setupBash() {
		try {
			createDir("bin");
			createDir("lib");
			createDir("lib64");

			List<String> binaries = List.of("/bin/bash", "/bin/sh", "/usr/bin/coreutils", "/bin/ls", "/bin/echo",
					"/bin/mkdir", "/bin/touch", "/bin/cp", "/bin/mv", "/bin/rm", "/bin/cat", "/bin/pwd",
					"/usr/bin/whoami", "/usr/bin/id");

			copyBinariesAndLibraries(binaries);
			classLogger.info("Bash and essential commands set up in template: {}", root);
		} catch (Exception e) {
			classLogger.error("Error setting up bash in template: {}", e.getMessage(), e);
		}
	}

	// =====================================================================
	// Git tier (moved from SymlinkHelper). NOTE: unlike the old per-user setup,
	// this does NOT create /tmp, /var/tmp, or /etc/git - those are per-user
	// writable directories created by SymlinkHelper, not shared template content.
	// =====================================================================

	/**
	 * Populate the template with everything needed to run {@code git} clones and
	 * fetches: the git binary, network/SSL/archive helpers (curl, ssh, openssl,
	 * wget, tar, gzip), the git-core helper programs, git-specific shared
	 * libraries, the git templates directory, and CA certificates. Each sub-step is
	 * timed.
	 * <p>
	 * Unlike the old per-user setup, this does <b>not</b> create {@code /tmp},
	 * {@code /var/tmp}, or {@code /etc/git}: those are per-user writable
	 * directories created by {@link SymlinkHelper}, not shared (read-only) template
	 * content.
	 */
	private void setupGit() {
		try {
			List<String> gitBinaries = List.of("/usr/bin/git", "/usr/bin/curl", "/usr/bin/ssh", "/bin/tar", "/bin/gzip",
					"/usr/bin/openssl", "/usr/bin/wget");
			timed("git binaries + ldd", () -> copyBinariesAndLibraries(gitBinaries));

			timed("git-core helpers", this::setupGitHelpers);
			timed("git-specific libs", this::copyGitSpecificLibraries);
			timed("git templates", this::setupGitTemplates);
			timed("ssl certs", this::setupSSLCerts);

			classLogger.info("Git set up in template: {}", root);
		} catch (Exception e) {
			classLogger.error("Error setting up git in template: {}", e.getMessage(), e);
		}
	}

	/**
	 * Locate and copy the {@code git-core} helper directory into the template.
	 * Tries the RHEL/UBI path {@code /usr/libexec/git-core} first, then the Ubuntu
	 * path {@code /usr/lib/git-core}, and finally falls back to
	 * {@link #findAndCopyGitHelpers()} (asking git itself).
	 */
	private void setupGitHelpers() {
		try {
			Path gitHelpersHost = Paths.get("/usr/libexec/git-core");
			Path gitHelpersChroot = root.resolve("usr/libexec/git-core");

			if (Files.exists(gitHelpersHost)) {
				copyDirectoryRecursively(gitHelpersHost, gitHelpersChroot);
				classLogger.info("Copied git helpers from RHEL/UBI location: {}", gitHelpersHost);
			} else {
				gitHelpersHost = Paths.get("/usr/lib/git-core");
				gitHelpersChroot = root.resolve("usr/lib/git-core");
				if (Files.exists(gitHelpersHost)) {
					copyDirectoryRecursively(gitHelpersHost, gitHelpersChroot);
					classLogger.info("Copied git helpers from Ubuntu location: {}", gitHelpersHost);
				} else {
					classLogger.warn("Git helpers directory not found at expected locations");
					findAndCopyGitHelpers();
				}
			}
		} catch (IOException e) {
			classLogger.error("Error setting up git helpers: {}", e.getMessage(), e);
		}
	}

	/**
	 * Last-resort discovery of the git helpers directory by running
	 * {@code git --exec-path} and copying whatever it reports into the template.
	 * Used when neither the RHEL nor Ubuntu canonical paths exist.
	 */
	private void findAndCopyGitHelpers() {
		try {
			ProcessBuilder pb = new ProcessBuilder("git", "--exec-path");
			Process process = pb.start();
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				String gitExecPath = reader.readLine();
				if (gitExecPath != null && !gitExecPath.trim().isEmpty()) {
					Path gitHelpersHost = Paths.get(gitExecPath.trim());
					Path gitHelpersChroot = root.resolve(gitExecPath.substring(1));
					if (Files.exists(gitHelpersHost)) {
						copyDirectoryRecursively(gitHelpersHost, gitHelpersChroot);
						classLogger.info("Found and copied git helpers from: {}", gitExecPath);
					}
				}
			}
			process.waitFor();
		} catch (Exception e) {
			classLogger.warn("Could not find git helpers using git --exec-path: {}", e.getMessage());
		}
	}

	/**
	 * Copy git-related shared libraries (libcurl, libssl, libcrypto, libz,
	 * libgssapi_krb5) that {@code ldd} on the git binary may not surface (they are
	 * dynamically loaded by curl backends, krb5 plugins, etc.). Tries the RHEL/UBI
	 * {@code /usr/lib64} paths first; if none are found, falls back to the Ubuntu
	 * {@code /usr/lib/x86_64-linux-gnu} paths.
	 */
	private void copyGitSpecificLibraries() {
		String[] rhelLibs = { "/usr/lib64/libcurl.so.4", "/usr/lib64/libssl.so.3", "/usr/lib64/libcrypto.so.3",
				"/usr/lib64/libz.so.1", "/usr/lib64/libgssapi_krb5.so.2" };
		String[] ubuntuLibs = { "/usr/lib/x86_64-linux-gnu/libcurl.so.4", "/usr/lib/x86_64-linux-gnu/libssl.so.3",
				"/usr/lib/x86_64-linux-gnu/libcrypto.so.3", "/usr/lib/x86_64-linux-gnu/libz.so.1" };

		boolean foundRhelLibs = false;
		for (String lib : rhelLibs) {
			if (Files.exists(Paths.get(lib))) {
				copyLibraryIfExists(lib);
				foundRhelLibs = true;
			}
		}
		if (!foundRhelLibs) {
			for (String lib : ubuntuLibs) {
				copyLibraryIfExists(lib);
			}
		}
	}

	/**
	 * Copy the git templates directory (the source for {@code git init}'s default
	 * hooks, info, and exclude files) into the template. Tries
	 * {@code /usr/share/git-core/templates} first, then
	 * {@code /usr/local/share/git-core/templates}, and stops at the first one
	 * found.
	 */
	private void setupGitTemplates() {
		String[] templatePaths = { "/usr/share/git-core/templates", "/usr/local/share/git-core/templates" };
		for (String templatePath : templatePaths) {
			Path templatesSource = Paths.get(templatePath);
			if (Files.exists(templatesSource)) {
				Path templatesTarget = root.resolve(templatePath.substring(1));
				try {
					copyDirectoryRecursively(templatesSource, templatesTarget);
					classLogger.info("Copied git templates from: {}", templatePath);
					break;
				} catch (IOException e) {
					classLogger.debug("Could not copy git templates from: {}", templatePath);
				}
			}
		}
	}

	/**
	 * Copy the system CA certificate bundle and trust store into the template so
	 * git, curl, and friends can verify HTTPS endpoints. Picks an OS-specific
	 * routine based on {@link #isRHELBasedSystem()}, and on unexpected failure runs
	 * both routines as a fallback.
	 */
	private void setupSSLCerts() {
		try {
			if (isRHELBasedSystem()) {
				classLogger.info("Predicted OS as RHEL");
				setupSSLCertsForRHEL();
			} else {
				classLogger.info("Predicted OS as Ubuntu");
				setupSSLCertsForUbuntu();
			}
		} catch (Exception e) {
			classLogger.warn("Could not setup SSL certificates: {}", e.getMessage());
			setupSSLCertsForRHEL();
			setupSSLCertsForUbuntu();
		}
	}

	/**
	 * Heuristic check for a RHEL/UBI host.
	 *
	 * @return {@code true} if any of {@code /etc/redhat-release},
	 *         {@code /etc/rhel-release}, or {@code /etc/pki} exist on the host
	 */
	private boolean isRHELBasedSystem() {
		return Files.exists(Paths.get("/etc/redhat-release")) || Files.exists(Paths.get("/etc/rhel-release"))
				|| Files.exists(Paths.get("/etc/pki"));
	}

	/**
	 * Copy the RHEL/UBI CA certificate bundle files
	 * ({@code /etc/pki/tls/certs/ca-bundle.crt} and the extracted PEM bundle) plus
	 * the supporting {@code /etc/pki} trust directories into the template. Missing
	 * sources are logged as warnings and skipped.
	 */
	private void setupSSLCertsForRHEL() {
		try {
			String[] caCertFiles = { "/etc/pki/tls/certs/ca-bundle.crt",
					"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", "/etc/pki/tls/cert.pem" };
			for (String certPath : caCertFiles) {
				Path sourceCerts = Paths.get(certPath);
				if (Files.exists(sourceCerts)) {
					Path targetCerts = root.resolve(certPath.substring(1));
					Files.createDirectories(targetCerts.getParent());
					Files.copy(sourceCerts, targetCerts, StandardCopyOption.REPLACE_EXISTING);
					classLogger.info("Copied RHEL CA certificates from: {}", certPath);
				} else {
					classLogger.warn("Could not find file at {}", certPath);
				}
			}

			String[] certDirs = { "/etc/pki/tls/certs", "/etc/pki/ca-trust/extracted", "/etc/pki/tls" };
			for (String certDir : certDirs) {
				Path sourceCertDir = Paths.get(certDir);
				if (Files.exists(sourceCertDir)) {
					Path targetCertDir = root.resolve(certDir.substring(1));
					copyDirectoryRecursively(sourceCertDir, targetCertDir);
					classLogger.debug("Copied RHEL certificate directory: {}", certDir);
				} else {
					classLogger.warn("Could not find directory at {}", certDir);
				}
			}
		} catch (IOException e) {
			classLogger.debug("Could not setup RHEL SSL certificates: {}", e.getMessage());
		}
	}

	/**
	 * Copy the Ubuntu CA certificate bundle
	 * ({@code /etc/ssl/certs/ca-certificates.crt}) and the supporting trust
	 * directories ({@code /etc/ssl/certs}, {@code /usr/share/ca-certificates}) into
	 * the template. The bundle file is copied with
	 * {@link StandardCopyOption#COPY_ATTRIBUTES} so the per-CA symlinks under
	 * {@code /etc/ssl/certs} continue to resolve.
	 */
	private void setupSSLCertsForUbuntu() {
		try {
			String[] caCertFiles = { "/etc/ssl/certs/ca-certificates.crt" };
			for (String certPath : caCertFiles) {
				Path sourceCerts = Paths.get(certPath);
				if (Files.exists(sourceCerts)) {
					Path targetCerts = root.resolve(certPath.substring(1));
					Files.createDirectories(targetCerts.getParent());
					Files.copy(sourceCerts, targetCerts, StandardCopyOption.REPLACE_EXISTING,
							StandardCopyOption.COPY_ATTRIBUTES);
					classLogger.info("Copied Ubuntu CA certificates from: {}", certPath);
				} else {
					classLogger.warn("Could not find file at {}", certPath);
				}
			}

			String[] certDirs = { "/etc/ssl/certs", "/usr/share/ca-certificates" };
			for (String certDir : certDirs) {
				Path sourceCertDir = Paths.get(certDir);
				if (Files.exists(sourceCertDir)) {
					Path targetCertDir = root.resolve(certDir.substring(1));
					copyDirectoryRecursively(sourceCertDir, targetCertDir);
					classLogger.debug("Copied Ubuntu certificate directory (with symlinks): {}", certDir);
				} else {
					classLogger.warn("Could not find directory at {}", certDir);
				}
			}
		} catch (IOException e) {
			classLogger.debug("Could not setup Ubuntu SSL certificates: {}", e.getMessage());
		}
	}

	// =====================================================================
	// Shared copy helpers (write into the template root)
	// =====================================================================

	/**
	 * Create a directory (and any missing parents) inside the template. A leading
	 * slash on {@code relativePath} is tolerated (stripped). Errors are logged and
	 * swallowed.
	 *
	 * @param relativePath path relative to the template root; may begin with a
	 *                     slash
	 */
	private void createDir(String relativePath) {
		try {
			Path dir = root.resolve(relativePath.startsWith("/") ? relativePath.substring(1) : relativePath);
			Files.createDirectories(dir);
			classLogger.debug("Created template directory: {}", dir);
		} catch (IOException e) {
			classLogger.error("Failed to create template directory: {}", relativePath, e);
		}
	}

	/**
	 * Copy the given binaries and every shared library they depend on into the
	 * template, deduplicating libraries across binaries.
	 * <p>
	 * This is intentionally <b>serial</b>. The work is small (sub-second), but
	 * doing it via {@code parallelStream()} dispatches to the common ForkJoinPool
	 * (platform threads) and - combined with one {@code ldd} subprocess fork per
	 * binary - oversubscribes a CPU-constrained container (e.g. k8s CPU limits).
	 * That starves the rest of the JVM (observed as multi-second HikariCP
	 * housekeeper delays and a ~57s build). Serial keeps CPU/fork pressure bounded
	 * and predictable.
	 */
	private void copyBinariesAndLibraries(Collection<String> binaryPaths) {
		// Deduplicate and keep only binaries that exist on the host.
		List<String> existing = new LinkedHashSet<>(binaryPaths).stream().filter(p -> Files.exists(Paths.get(p)))
				.collect(Collectors.toList());

		// Resolve libraries serially, deduped across all binaries.
		Set<String> uniqueLibraries = new HashSet<>();
		for (String binary : existing) {
			uniqueLibraries.addAll(resolveRequiredLibraries(binary));
		}

		for (String binary : existing) {
			copySystemBinary(binary);
		}
		for (String library : uniqueLibraries) {
			copyLibraryIfExists(library);
		}
	}

	/**
	 * Invoke {@code ldd} against a binary and parse its output into the set of
	 * absolute library paths it depends on. Handles both the
	 * {@code libname => /path/to/lib (0x...)} form and direct
	 * {@code /lib...}-rooted entries (such as the ELF interpreter).
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
					if (line.contains("=>") && line.contains("/")) {
						String[] parts = line.split("=>");
						if (parts.length == 2) {
							String libPath = parts[1].trim().split("\\s+")[0];
							if (libPath.startsWith("/")) {
								libs.add(libPath);
							}
						}
					} else if (line.trim().startsWith("/lib")) {
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
	 * Copy a host binary into the template at the same relative path and mark it
	 * executable, creating parent directories as needed. No-op (with a warning) if
	 * the source does not exist.
	 *
	 * @param binaryPath absolute path of the host binary (e.g. {@code /bin/bash})
	 */
	private void copySystemBinary(String binaryPath) {
		try {
			Path sourceBinary = Paths.get(binaryPath);
			if (!Files.exists(sourceBinary)) {
				classLogger.warn("System binary does not exist: {}", binaryPath);
				return;
			}
			Path targetBinary = root.resolve(binaryPath.substring(1));
			Files.createDirectories(targetBinary.getParent());
			Files.copy(sourceBinary, targetBinary, StandardCopyOption.REPLACE_EXISTING);
			targetBinary.toFile().setExecutable(true);
			classLogger.debug("Copied system binary: {} to {}", binaryPath, targetBinary);
		} catch (IOException e) {
			classLogger.error("Failed to copy system binary: {}", binaryPath, e);
		}
	}

	/**
	 * Copy a single shared library into the template at the same relative path.
	 * Silently no-ops if the source does not exist on the host or the target has
	 * already been brought in by a previous call. Errors are logged at debug and
	 * swallowed.
	 *
	 * @param libraryPath absolute path of the host library
	 */
	private void copyLibraryIfExists(String libraryPath) {
		try {
			Path sourceLib = Paths.get(libraryPath);
			if (!Files.exists(sourceLib)) {
				return;
			}
			Path targetLib = root.resolve(libraryPath.substring(1));
			Files.createDirectories(targetLib.getParent());
			if (!Files.exists(targetLib)) {
				Files.copy(sourceLib, targetLib, StandardCopyOption.REPLACE_EXISTING);
				classLogger.debug("Copied library: {} to {}", libraryPath, targetLib);
			}
		} catch (IOException e) {
			classLogger.debug("Could not copy library: {} - {}", libraryPath, e.getMessage());
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

}
