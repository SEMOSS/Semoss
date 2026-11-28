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
package prerna.util.sandbox;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.project.api.IProject;
import prerna.util.Utility;
import prerna.util.openshell.OpenShellSecrets;

/**
 * An app's sandbox secrets for the fakechroot and namespace jails, which have no supervisor to
 * broker them the way {@link OpenShellSecrets} does.
 *
 * <p>
 * The project declares them as it does for OpenShell:
 *
 * <pre>
 * SANDBOX_SECRETS                     [{"name":"DCTM_AUTH","hosts":["dctm.example.com"]}]
 * SANDBOX_SECRET_DCTM_AUTH            the value
 * </pre>
 *
 * When a user's jail loads the project, each declared secret that has a value is written to
 * {@code secrets.json} in a folder of that user's own chroot, by project key, the same shape as
 * the OpenShell placeholder file. The jail sees the folder at {@code <SEMOSS home>/sandbox-secrets}:
 * in fakechroot it is that path inside the chroot, and the namespace jail mounts it there read-only.
 * {@code py/smss_secrets.py} reads it first, so app code is the same on every backend.
 *
 * <p>
 * <b>The code gets the value itself, not a placeholder.</b> Nothing here limits where it is sent:
 * the declared hosts are checked for form, so the same {@code .smss} works on OpenShell, and are not
 * enforced. Every user who can load the app, and so can run Python with it loaded, can read its
 * secrets. Use OpenShell where that matters.
 */
public final class JailSecrets {

	private static final Logger classLogger = LogManager.getLogger(JailSecrets.class);

	/** The folder's name, under the SEMOSS home as the jail sees it. */
	public static final String FOLDER = "sandbox-secrets";
	/** The file {@code py/smss_secrets.py} reads. */
	public static final String FILE = "secrets.json";

	private static final Set<PosixFilePermission> FOLDER_PERMISSIONS = PosixFilePermissions.fromString("rwx------");
	private static final Set<PosixFilePermission> FILE_PERMISSIONS = PosixFilePermissions.fromString("rw-------");

	private JailSecrets() {
		// static utility
	}

	/**
	 * Where the jail sees the secrets folder: {@code <SEMOSS home>/sandbox-secrets}.
	 *
	 * @param baseFolder the SEMOSS home
	 * @return the path
	 */
	public static Path jailFolder(String baseFolder) {
		return Paths.get(baseFolder).toAbsolutePath().normalize().resolve(FOLDER);
	}

	/**
	 * Where the folder is on the host: the jail path, inside the user's chroot folder.
	 *
	 * @param userChrootFolder the user's chroot folder
	 * @param baseFolder       the SEMOSS home
	 * @return the path
	 */
	public static Path hostFolder(String userChrootFolder, String baseFolder) {
		String inJail = jailFolder(baseFolder).toString().replace('\\', '/');
		while (inJail.startsWith("/")) {
			inJail = inJail.substring(1);
		}
		// a Windows drive letter has no place inside a chroot
		inJail = inJail.replaceFirst("^[A-Za-z]:/", "");
		return Paths.get(userChrootFolder).toAbsolutePath().normalize().resolve(inJail);
	}

	/**
	 * Makes the user's secrets folder, owner-only, before any project is loaded. Its presence is
	 * how {@code py/smss_secrets.py} knows it runs in a jail: there it reads only this folder and
	 * never the OpenShell placeholder file, which is in the shared insight cache.
	 *
	 * @param userChrootFolder the user's chroot folder
	 * @return the host folder, or null when it could not be made
	 */
	public static Path ensureFolder(String userChrootFolder) {
		Path folder = hostFolder(userChrootFolder, Utility.getBaseFolder());
		try {
			createFolder(folder);
			return folder;
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Could not make the sandbox secrets folder {}", folder, e);
			return null;
		}
	}

	/**
	 * Writes the project's declared secrets into the user's secrets folder, in place of the ones
	 * written for it before.
	 *
	 * @param userChrootFolder the user's chroot folder
	 * @param projectId        the project, which the user may load
	 * @return the host folder, or null when the project could not be read or the folder is missing
	 */
	public static Path ensureForProject(String userChrootFolder, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			return null;
		}
		Path folder = hostFolder(userChrootFolder, Utility.getBaseFolder());
		try {
			write(folder, projectId, values(projectId, project.getSandboxSecretSettings()));
			return Files.isDirectory(folder) ? folder : null;
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Could not write the sandbox secrets of project {} for the jail", projectId, e);
			return null;
		}
	}

	/**
	 * The declared secrets that have a value, by name. A secret declared without a value, or
	 * declared wrongly, is skipped with a warning.
	 *
	 * @param projectId the project
	 * @param props     its {@link IProject#getSandboxSecretSettings()}
	 * @return name to value, in declared order
	 */
	static Map<String, String> values(String projectId, Properties props) {
		Map<String, String> values = new LinkedHashMap<>();
		if (props == null) {
			return values;
		}
		for (OpenShellSecrets.Declared secret : OpenShellSecrets.parse(projectId,
				props.getProperty(OpenShellSecrets.SECRETS_PROP))) {
			String value = props.getProperty(OpenShellSecrets.VALUE_PROP_PREFIX + secret.name());
			if (value == null || value.trim().isEmpty()) {
				classLogger.warn("Project {} declares sandbox secret {} but sets no {}{}", projectId, secret.name(),
						OpenShellSecrets.VALUE_PROP_PREFIX, secret.name());
				continue;
			}
			values.put(secret.name(), value.trim());
		}
		return values;
	}

	/**
	 * Sets one project's secrets in the folder's file, keeping every other project's. A project with
	 * none is taken out. The folder and file are the owner's alone, and the file is replaced whole,
	 * so code reading it never sees half of it.
	 *
	 * @param folder    the host folder
	 * @param projectId the project
	 * @param values    its secrets by name
	 * @throws IOException when the file cannot be written
	 */
	static synchronized void write(Path folder, String projectId, Map<String, String> values) throws IOException {
		Path file = folder.resolve(FILE);
		Map<String, Map<String, String>> byProject = read(file);
		String key = OpenShellSecrets.projectKey(projectId);
		if (values.isEmpty()) {
			if (byProject.remove(key) == null) {
				// nothing was written for it, so there is nothing to take out
				return;
			}
		} else {
			byProject.put(key, new LinkedHashMap<>(values));
		}
		createFolder(folder);
		Path staged = Files.createTempFile(folder, FILE, ".tmp");
		try {
			setPermissions(staged, FILE_PERMISSIONS);
			Files.writeString(staged, new Gson().toJson(byProject), StandardCharsets.UTF_8);
			try {
				Files.move(staged, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			} catch (AtomicMoveNotSupportedException e) {
				Files.move(staged, file, StandardCopyOption.REPLACE_EXISTING);
			}
		} finally {
			Files.deleteIfExists(staged);
		}
	}

	/** The file's secrets by project key; empty when there is no file or it cannot be read. */
	static Map<String, Map<String, String>> read(Path file) {
		Map<String, Map<String, String>> byProject = new TreeMap<>();
		if (!Files.isRegularFile(file)) {
			return byProject;
		}
		try {
			JsonObject root = JsonParser.parseString(Files.readString(file, StandardCharsets.UTF_8)).getAsJsonObject();
			for (Map.Entry<String, JsonElement> project : root.entrySet()) {
				Map<String, String> values = new LinkedHashMap<>();
				for (Map.Entry<String, JsonElement> secret : project.getValue().getAsJsonObject().entrySet()) {
					values.put(secret.getKey(), secret.getValue().getAsString());
				}
				byProject.put(project.getKey(), values);
			}
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Replacing the unreadable sandbox secrets file {}", file);
			byProject.clear();
		}
		return byProject;
	}

	/** Makes the folder if it is missing, and sets it owner-only either way. */
	static void createFolder(Path folder) throws IOException {
		Files.createDirectories(folder);
		setPermissions(folder, FOLDER_PERMISSIONS);
	}

	private static void setPermissions(Path path, Set<PosixFilePermission> permissions) throws IOException {
		try {
			Files.setPosixFilePermissions(path, permissions);
		} catch (UnsupportedOperationException e) {
			// not a POSIX file system (Windows); the folder is inside the user's chroot folder
		}
	}
}
