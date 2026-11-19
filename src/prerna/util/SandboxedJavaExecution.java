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
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.security.SensitiveKeys;

/**
 * Static factory class whose methods replace direct file-API calls in bytecode
 * loaded by {@link SemossClassLoader}. All path-accepting APIs are funnelled
 * through {@link #sanitize(String)}, which enforces the per-user sandbox root.
 *
 * <h3>Per-user sandbox</h3>
 * <p>
 * Each user execution thread must set its chroot path before invoking any
 * reactor code and clear it afterward:
 * 
 * <pre>{@code
 * 
 * SandboxedPaths.setSandboxRootForCurrentThread(user.getChrootPath());
 * try {
 * 	// run reactor code
 * } finally {
 * 	SandboxedPaths.clearSandboxRootForCurrentThread();
 * }
 * }</pre>
 * 
 * A global fallback can be set via {@link #setGlobalSandboxRoot(String)} for
 * contexts where no thread-local has been configured.
 *
 * <h3>Symlink support</h3>
 * <p>
 * Path validation uses {@link Path#normalize()}. This prevents {@code ../}
 * traversal while allowing admin-placed symlinks inside the chroot to be
 * followed transparently at I/O time. Reactor code cannot exploit this because
 * {@code Files.createSymbolicLink()} is blocked by the bytecode transformer,
 * closing the user-created symlink escape vector.
 */
public final class SandboxedJavaExecution {

	private static final Logger classLogger = LogManager.getLogger(SandboxedJavaExecution.class);

	/**
	 * How long to wait before saying again that chroot is off.
	 */
	private static final long CHROOT_OFF_WARNING_GAP_MILLIS = 60_000L;

	/**
	 * When the chroot warning was last logged, in milliseconds, or 0 for never.
	 */
	static final AtomicLong CHROOT_OFF_LAST_WARNED = new AtomicLong();

	/**
	 * Per-thread chroot root (set by the executor before running reactor code).
	 * Takes precedence over the global fallback.
	 */
	private static final ThreadLocal<String> THREAD_SANDBOX_ROOT = new ThreadLocal<>();

	/**
	 * Global fallback root used when no thread-local has been set. {@code null}
	 * disables sandboxing (not recommended in production).
	 */
	private static volatile String globalSandboxRoot = null;

	// ------------------------------------------------------------------ System
	// property allowlist

	/**
	 * Standard JDK system properties that are safe to expose to reactor code. These
	 * carry no credentials and do not reveal sensitive server paths. The set is
	 * intentionally conservative - add to {@link #PLATFORM_PROPERTIES} for anything
	 * not listed here.
	 */
	static final Set<String> DEFAULT_SAFE_PROPERTIES = Collections
			.unmodifiableSet(new java.util.HashSet<>(java.util.Arrays.asList(
					// Java runtime identity
					"java.version", "java.specification.version", "java.class.version", "java.vendor",
					"java.vendor.url", "java.vm.version", "java.vm.vendor", "java.vm.name",
					// OS identity
					"os.name", "os.arch", "os.version",
					// Path / encoding conventions (safe - no server-specific paths)
					"file.separator", "path.separator", "line.separator", "file.encoding", "sun.jnu.encoding",
					// Temp dir - reactors may need this; sandbox transforms actual File/Path
					// usage anyway so the value alone cannot bypass path controls
					"java.io.tmpdir",
					// TLS protocol & cipher configuration (not credentials or keystore paths)
					"https.protocols", "https.cipherSuites", "jdk.tls.client.protocols", "jdk.tls.server.protocols",
					"jdk.tls.disabledAlgorithms", "jdk.tls.legacyAlgorithms", "javax.net.ssl.sessionCacheSize",
					"javax.net.ssl.sessionTimeout", "sun.security.ssl.allowUnsafeRenegotiation",
					"sun.security.ssl.allowLegacyHelloMessages"
			// NOT included (sensitive or reveal server topology):
			// user.home, user.dir, user.name
			// java.home, java.class.path, java.library.path
			// javax.net.ssl.trustStore/keyStore (paths)
			// javax.net.ssl.trustStorePassword/keyStorePassword (credentials)
			)));

	/**
	 * Platform-injected additional properties, populated via
	 * {@link #addPlatformProperties(Map)} at startup or classloader construction.
	 *
	 * <p>
	 * Map semantics:
	 * <ul>
	 * <li>Key present, value non-null -> return the provided value directly
	 * (platform override, reactor never sees the real system property).</li>
	 * <li>Key present, value null -> delegate to {@code System.getProperty(key)}
	 * (allow-list the key, expose its real value).</li>
	 * </ul>
	 */
	private static final ConcurrentHashMap<String, String> PLATFORM_PROPERTIES = new ConcurrentHashMap<>();

	private SandboxedJavaExecution() {
	}

	// -------------------------------------------------------- Platform properties

	/**
	 * Registers additional system properties that reactor code is permitted to
	 * read. Call this once at startup or from a classloader constructor before any
	 * reactor code is loaded.
	 *
	 * <p>
	 * Map semantics - for each entry:
	 * <ul>
	 * <li>Non-null value: reactor code always receives this value for the key,
	 * regardless of what {@code System.getProperty} returns (platform
	 * override).</li>
	 * <li>Null value: the real {@code System.getProperty(key)} value is returned
	 * (allow-list the key, expose its actual value).</li>
	 * </ul>
	 *
	 * @param properties additional properties to expose; {@code null} is ignored
	 */
	public static void addPlatformProperties(Map<String, String> properties) {
		if (properties != null) {
			PLATFORM_PROPERTIES.putAll(properties);
		}
	}

	/**
	 * Sandboxed replacement for {@code System.getProperty(String)}. Returns the
	 * property value if the key is in {@link #DEFAULT_SAFE_PROPERTIES} or was
	 * registered via {@link #addPlatformProperties}, otherwise {@code null}.
	 */
	public static String sandboxedGetProperty(String key) {
		if (key == null) {
			return null;
		}
		if (PLATFORM_PROPERTIES.containsKey(key)) {
			String override = PLATFORM_PROPERTIES.get(key);
			return override != null ? override : System.getProperty(key);
		}
		if (DEFAULT_SAFE_PROPERTIES.contains(key)) {
			return System.getProperty(key);
		}
		return null;
	}

	/**
	 * Sandboxed replacement for {@code System.getProperty(String, String)}. Returns
	 * the property value when the key is allowed, otherwise {@code defaultValue}.
	 */
	public static String sandboxedGetProperty(String key, String defaultValue) {
		if (key == null) {
			return defaultValue;
		}
		if (PLATFORM_PROPERTIES.containsKey(key)) {
			String override = PLATFORM_PROPERTIES.get(key);
			if (override != null) {
				return override;
			}
			String real = System.getProperty(key);
			return real != null ? real : defaultValue;
		}
		if (DEFAULT_SAFE_PROPERTIES.contains(key)) {
			String real = System.getProperty(key);
			return real != null ? real : defaultValue;
		}
		return defaultValue;
	}

	/**
	 * Sandboxed replacement for {@code Utility.getDIHelperProperty(String)}.
	 * Returns the RDF_Map value, or null for every key the RDF_Map editor hides:
	 * the secret keys of {@link SensitiveKeys#isSensitiveKey(String)} and the
	 * secret store and keystore settings.
	 *
	 * @param key the RDF_Map key
	 * @return the value, or null for a hidden or missing key
	 */
	public static String sandboxedPlatformProperty(String key) {
		if (key == null || DIHelper.containsSensitiveKey(key)) {
			return null;
		}
		return Utility.getDIHelperProperty(key);
	}

	/**
	 * Sandboxed replacement for {@code Utility.loadProperties(String)}: reads the
	 * file under the same path rule as {@link #file(String)}.
	 *
	 * @param filePath the properties file
	 * @return its values, empty when the file cannot be read
	 */
	public static Properties sandboxedLoadProperties(String filePath) {
		return Utility.loadProperties(filePath == null ? null : sanitize(filePath));
	}

	/**
	 * Sandboxed replacement for {@code Utility.loadProperties(File)}: reads the
	 * file under the same path rule as {@link #file(String)}.
	 *
	 * @param file the properties file
	 * @return its values, empty when the file cannot be read
	 */
	public static Properties sandboxedLoadProperties(File file) {
		return Utility.loadProperties(file == null ? null : new File(sanitize(file.getPath())));
	}

	// ------------------------------------------------------------------ Setup

	/**
	 * Sets the sandbox root for the <em>current thread</em> to the given user
	 * chroot path. Must be matched with {@link #clearSandboxRootForCurrentThread()}
	 * in a {@code finally} block.
	 *
	 * <p>
	 * Typical call site - the executor that launches reactor code:
	 * 
	 * <pre>{@code
	 * SandboxedPaths.setSandboxRootForCurrentThread(user.getChrootPath());
	 * }</pre>
	 *
	 * @param chrootPath absolute path to the user's chroot directory
	 */
	public static void setSandboxRootForCurrentThread(String chrootPath) {
		if (chrootPath == null) {
			THREAD_SANDBOX_ROOT.remove();
		} else {
			THREAD_SANDBOX_ROOT.set(Path.of(chrootPath).toAbsolutePath().normalize().toString());
		}
	}

	/**
	 * The chroot path this thread was given with
	 * {@link #setSandboxRootForCurrentThread(String)}, so work handed to another
	 * thread can be given the same one.
	 *
	 * @return the thread's chroot path, or null when the thread has none
	 */
	static String sandboxRootOfCurrentThread() {
		return THREAD_SANDBOX_ROOT.get();
	}

	/**
	 * Removes the thread-local sandbox root. Always call this in a {@code finally}
	 * block after reactor code finishes to prevent the chroot path leaking to
	 * subsequent work on the same thread.
	 */
	public static void clearSandboxRootForCurrentThread() {
		THREAD_SANDBOX_ROOT.remove();
	}

	/**
	 * Sets a global sandbox root used as a fallback when no thread-local root has
	 * been configured. Pass {@code null} to disable global sandboxing.
	 *
	 * @param root absolute path to the sandbox root
	 */
	public static void setGlobalSandboxRoot(String root) {
		globalSandboxRoot = root == null ? null : Path.of(root).toAbsolutePath().normalize().toString();
	}

	/**
	 * Returns the sandbox root for the current thread: the thread-local value if
	 * set, otherwise the global fallback, otherwise {@code null} (no sandboxing).
	 */
	private static String getEffectiveSandboxRoot() {
		String threadRoot = THREAD_SANDBOX_ROOT.get();
		return threadRoot != null ? threadRoot : globalSandboxRoot;
	}

	// ----------------------------------------------------------- Core sanitize

	/**
	 * Resolves {@code rawPath} inside the effective sandbox root and returns the
	 * normalized path string ready for use in I/O operations.
	 *
	 * <p>
	 * Security guarantee: the normalized path must start with the sandbox root.
	 * This prevents {@code ../} traversal and absolute paths outside the chroot.
	 * Symlinks are not resolved here - they are followed at I/O time by the OS,
	 * which is safe because {@code Files.createSymbolicLink()} is blocked by the
	 * bytecode transformer.
	 *
	 * @param rawPath path as provided by reactor code
	 * @return sanitized path
	 * @throws SecurityException if the path would escape the sandbox root
	 */
	static String sanitize(String rawPath) {
		String root = getEffectiveSandboxRoot();
		if (root == null) {
			if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
				throw new SecurityException("File access in a project reactor needs the user's folder, which this "
						+ "thread does not have; do the file work on the thread that runs the reactor");
			}
			warnChrootOff();
			return rawPath;
		}
		Path base = Path.of(root);
		Path raw = Path.of(rawPath).normalize();
		Path resolved;
		if (raw.isAbsolute()) {
			if (raw.startsWith(root)) {
				// Already within the sandbox (e.g. from File.getAbsolutePath() on a
				// previously sandboxed File) - use as-is.
				resolved = raw;
			} else {
				// Absolute path outside the sandbox root - treat as chroot-relative,
				// mirroring real chroot behaviour where "/" is the sandbox root.
				String relative = raw.toString().substring(1); // strip leading /
				resolved = relative.isEmpty() ? base : base.resolve(relative).normalize();
			}
		} else {
			resolved = base.resolve(rawPath).normalize();
		}
		if (!resolved.startsWith(root)) {
			throw new SecurityException("Path escape attempt blocked by project reactor sandbox: " + rawPath);
		}
		return resolved.toString();
	}

	/**
	 * Logs, at most once a minute, that a project reactor used a file path while
	 * chroot is off. The reactor still runs, which is what a developer machine
	 * needs; the path is used as given.
	 */
	private static void warnChrootOff() {
		long now = System.currentTimeMillis();
		long last = CHROOT_OFF_LAST_WARNED.get();
		if ((last == 0 || now - last >= CHROOT_OFF_WARNING_GAP_MILLIS)
				&& CHROOT_OFF_LAST_WARNED.compareAndSet(last, now)) {
			classLogger.warn(
					"A project reactor used a file path while {} is off, so the path is used as given and is not "
							+ "kept inside the user's folder. This would typically require {} to be enabled for security.",
					Constants.CHROOT_ENABLE, Constants.CHROOT_ENABLE);
		}
	}

	// ------------------------------------------------------------------ File

	public static File file(String path) {
		return new File(sanitize(path));
	}

	public static File file(String parent, String child) {
		return new File(sanitize(parent + File.separator + child));
	}

	public static File file(File parent, String child) {
		return new File(sanitize(parent.getAbsolutePath() + File.separator + child));
	}

	public static File file(URI uri) {
		return new File(sanitize(uri.getPath()));
	}

	/**
	 * Strips the chroot prefix from an absolute path string, returning the
	 * chroot-relative form. E.g. {@code /chroot/user123/data.txt} becomes
	 * {@code /data.txt}. If no sandbox root is active or the path is not under the
	 * root, the value is returned unchanged.
	 */
	private static String stripChrootPrefix(String abs) {
		String root = getEffectiveSandboxRoot();
		if (root == null || !abs.startsWith(root)) {
			return abs;
		}
		String stripped = abs.substring(root.length());
		return stripped.isEmpty() ? "/" : stripped;
	}

	/** Sandboxed replacement for {@code File.getAbsolutePath()}. */
	public static String sandboxedGetAbsolutePath(File file) {
		return stripChrootPrefix(file.getAbsolutePath());
	}

	/**
	 * Sandboxed replacement for {@code File.getCanonicalPath()}. Falls back to
	 * {@code getAbsolutePath()} if the canonical path cannot be resolved.
	 */
	public static String sandboxedGetCanonicalPath(File file) throws java.io.IOException {
		return stripChrootPrefix(file.getCanonicalPath());
	}

	/** Sandboxed replacement for {@code File.toPath()}. */
	public static Path sandboxedFileToPath(File file) {
		return Path.of(stripChrootPrefix(file.getAbsolutePath()));
	}

	/**
	 * Sandboxed replacement for {@code File.getParentFile()}. Returns {@code null}
	 * if the parent would be outside the chroot root, matching the natural JVM
	 * behaviour when {@code getParentFile()} is called on a filesystem root.
	 */
	public static File sandboxedGetParentFile(File file) {
		File parent = file.getAbsoluteFile().getParentFile();
		if (parent == null) {
			return null;
		}
		String root = getEffectiveSandboxRoot();
		if (root == null) {
			return parent;
		}
		return parent.toPath().normalize().startsWith(root) ? parent : null;
	}

	/**
	 * Sandboxed replacement for {@code File.getParent()}. Returns {@code null} if
	 * the parent would be outside the chroot root.
	 */
	public static String sandboxedGetParent(File file) {
		File parent = sandboxedGetParentFile(file);
		return parent != null ? parent.getPath() : null;
	}

	// -------------------------------------------------------- FileInputStream

	public static FileInputStream fileInputStream(String path) throws FileNotFoundException {
		return new FileInputStream(sanitize(path));
	}

	public static FileInputStream fileInputStream(File file) throws FileNotFoundException {
		return new FileInputStream(sanitize(file.getAbsolutePath()));
	}

	/** FileDescriptor carries no path - delegate directly. */
	public static FileInputStream fileInputStream(FileDescriptor fd) {
		return new FileInputStream(fd);
	}

	// ------------------------------------------------------- FileOutputStream

	public static FileOutputStream fileOutputStream(String path) throws FileNotFoundException {
		return new FileOutputStream(sanitize(path));
	}

	public static FileOutputStream fileOutputStream(String path, boolean append) throws FileNotFoundException {
		return new FileOutputStream(sanitize(path), append);
	}

	public static FileOutputStream fileOutputStream(File file) throws FileNotFoundException {
		return new FileOutputStream(sanitize(file.getAbsolutePath()));
	}

	public static FileOutputStream fileOutputStream(File file, boolean append) throws FileNotFoundException {
		return new FileOutputStream(sanitize(file.getAbsolutePath()), append);
	}

	/** FileDescriptor carries no path - delegate directly. */
	public static FileOutputStream fileOutputStream(FileDescriptor fd) {
		return new FileOutputStream(fd);
	}

	// ------------------------------------------------------- Paths / Path NIO

	/**
	 * Sandboxed replacement for {@code Paths.get(String, String...)}. The varargs
	 * array is already constructed by the caller's bytecode.
	 */
	public static Path pathsGet(String first, String[] more) {
		String raw = (more == null || more.length == 0) ? first : Paths.get(first, more).toString();
		return Path.of(sanitize(raw));
	}

	/**
	 * Sandboxed replacement for {@code Path.of(String, String...)}.
	 */
	public static Path pathOf(String first, String[] more) {
		return pathsGet(first, more);
	}

	// ---------------------------------------------------------- Blocked APIs

	/**
	 * Replacement for a call project reactors cannot make, such as
	 * {@code System.loadLibrary} - always throws.
	 *
	 * @param method the method that was called
	 */
	public static void blockCall(String method) {
		throw new SecurityException(method + " is not permitted in project reactors");
	}

	/** Replacement for {@code Runtime.exec(...)} - always throws. */
	public static void blockRuntimeExec() {
		throw new SecurityException("Runtime.exec() is not permitted in project reactors");
	}

	/**
	 * Replacement for {@code System.exit()} and {@code Runtime.halt()} - always
	 * throws.
	 */
	public static void blockExit() {
		throw new SecurityException("System.exit() / Runtime.halt() is not permitted in project reactors");
	}

	// ------------------------------------------------------------- Path values

	/**
	 * A file a project's code is about to use or hand to the JDK, the platform or a
	 * shared library, under the same path rule as {@link #file(String)}.
	 *
	 * @param file the file the project holds
	 * @return the same file when it is inside the user's folder or no folder is
	 *         set, otherwise the file at that path inside the user's folder
	 */
	public static File sandboxedFileValue(File file) {
		if (file == null) {
			return null;
		}
		String sandboxed = sanitize(file.getPath());
		return sandboxed.equals(file.getPath()) ? file : new File(sandboxed);
	}

	/**
	 * A path a project's code is about to hand to the JDK, the platform or a shared
	 * library, under the same path rule as {@link #file(String)}. A path inside
	 * another file system, such as an open zip file, is left as it is.
	 *
	 * @param path the path the project holds
	 * @return the same path when it is inside the user's folder or no folder is
	 *         set, otherwise that path inside the user's folder
	 */
	public static Path sandboxedPathValue(Path path) {
		if (path == null || path.getFileSystem() != FileSystems.getDefault()) {
			return path;
		}
		String sandboxed = sanitize(path.toString());
		return sandboxed.equals(path.toString()) ? path : Path.of(sandboxed);
	}

	/**
	 * A URL a project's code is about to hand to the JDK, the platform or a shared
	 * library, which would fetch what it names: it has to be a local file, and
	 * follows the same path rule as {@link #file(String)}. Any other address is
	 * fetched with HttpHelperUtility, which applies the request lists.
	 *
	 * @param url the URL the project holds
	 * @return the same URL when it names a file inside the user's folder or no
	 *         folder is set, otherwise the URL of that path inside the user's folder
	 * @throws SecurityException if the URL names anything but a local file
	 */
	public static URL sandboxedUrlValue(URL url) {
		if (url == null) {
			return null;
		}
		if (!"file".equalsIgnoreCase(url.getProtocol())) {
			throw new SecurityException("A URL handed to another class in a project reactor has to be a local file; "
					+ "fetch '" + url.getProtocol() + "' addresses with HttpHelperUtility");
		}
		try {
			String path = new File(url.toURI()).getPath();
			String sandboxed = sanitize(path);
			return sandboxed.equals(path) ? url : new File(sandboxed).toURI().toURL();
		} catch (URISyntaxException | MalformedURLException | IllegalArgumentException e) {
			throw new SecurityException("Not a local file: " + url, e);
		}
	}

	/**
	 * The address of a zip or jar file a project's code is about to open as a file
	 * system, such as {@code jar:file:/data/archive.zip}: the file follows the same
	 * path rule as {@link #file(String)}.
	 *
	 * @param uri the address the project passed
	 * @return the same address when the file is inside the user's folder or no
	 *         folder is set, otherwise the address of that path inside the user's
	 *         folder
	 * @throws SecurityException if the address names anything but a zip or jar
	 *                           file on the local disk
	 */
	public static URI sandboxedFileSystemUri(URI uri) {
		if (uri == null) {
			return null;
		}
		String address = uri.toString();
		int inside = address.indexOf("!/");
		String archive = inside < 0 ? address : address.substring(0, inside);
		if (!archive.regionMatches(true, 0, "jar:file:", 0, 9)) {
			throw new SecurityException(
					"Only a zip or jar file on the local disk can be opened as a file system in project reactors");
		}
		try {
			String path = new File(URI.create(archive.substring(4))).getPath();
			String sandboxed = sanitize(path);
			if (sandboxed.equals(path)) {
				return uri;
			}
			return URI.create("jar:" + new File(sandboxed).toURI() + (inside < 0 ? "" : address.substring(inside)));
		} catch (IllegalArgumentException e) {
			throw new SecurityException("Not a zip or jar file on the local disk: " + address, e);
		}
	}

	/**
	 * A path a project's code handed to a platform helper, under the same path rule
	 * as {@link #file(String)}.
	 *
	 * @param path the path the project passed
	 * @return the path inside the user's folder when one is set, otherwise the same
	 *         path
	 */
	public static String sandboxedPath(String path) {
		return path == null ? null : sanitize(path);
	}

	/**
	 * Replacement for {@code System.setProperty/clearProperty/setProperties} -
	 * always throws.
	 */
	public static void blockSystemMutation() {
		throw new SecurityException("Modifying JVM system properties is not permitted in project reactors");
	}

	/** Replacement for {@code Files.createSymbolicLink()} - always throws. */
	public static void blockSymlinkCreation() {
		throw new SecurityException("Creating symbolic links is not permitted in project reactors");
	}

	/** Replacement for {@code FileChannel.open()} - always throws. */
	public static void blockFileChannel() {
		throw new SecurityException("FileChannel.open() is not permitted in project reactors");
	}

	/**
	 * Replacement for {@code URL.openStream()} / {@code URL.openConnection()} -
	 * always throws.
	 */
	public static void blockNetworkAccess() {
		throw new SecurityException("Network access is not permitted in project reactors");
	}

	/**
	 * Fallback when a constructor overload of a sandboxed type has no explicit
	 * factory mapping - always throws.
	 */
	public static void blockUnknownConstructor() {
		throw new SecurityException(
				"Constructor overload not supported in project reactor sandbox - use a mapped overload");
	}
}
