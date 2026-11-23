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
package prerna.collaboration;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.Room;
import prerna.project.api.IProject;
import prerna.reactor.agent.runtime.AgentFilesIndex;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.ProjectSyncUtility;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

// A Digital Twin's own files, the public folder of its project: adding one from the chat its owner is having with
// it, and taking out the placeholder.txt the file explorer leaves in a folder it makes, once that folder holds a
// file. Copy and the upload endpoint serve every project and know nothing of twins, so the placeholders go here:
// when a file is added from a chat, and on each pass over the twin's files (WorkTwinIndex), which the twin's
// pages ask for after every change.
public final class WorkTwinFiles {

	private static final Logger classLogger = LogManager.getLogger(WorkTwinFiles.class);

	/** Where a file added from a chat lands: the folder the twin's pages put uploads and drive files in. */
	static final String FOLDER = "files";
	// FileSystemUtil.createNewAssetDirectory puts this name, with this text, into every folder it makes
	private static final String PLACEHOLDER = "placeholder.txt";
	private static final String PLACEHOLDER_TEXT = "placeholder";

	private WorkTwinFiles() {
	}

	/**
	 * Adds a file of a chat to the files of the Digital Twin the chat is with, for
	 * the twin's owner alone. A name the twin already has gets a numbered copy
	 * beside it; nothing is replaced.
	 *
	 * @param user the signed in user
	 * @param room the chat, which names its agent
	 * @param path the file, relative to the chat's folder
	 * @return {@code file}, where the copy is among the twin's files, as the
	 *         agent:/ path its chats read it by
	 * @throws IllegalArgumentException when the chat is not with a twin this user
	 *                                  created and can change, or it has no such
	 *                                  file
	 */
	public static Map<String, Object> add(User user, Room room, String path) {
		String twinId = room == null ? null : agentOf(room);
		// one answer whether the chat has no agent, its agent is not a twin, or the twin is someone else's:
		// only its owner is told that an agent is a twin
		if (!canManage(user, twinId)) {
			throw new IllegalArgumentException("Files can only be added in a chat with your own Digital Twin");
		}
		Path source = chatFile(Path.of(Room.roomFolderPath(room.getId())), path);
		IProject project = Utility.getProject(twinId);
		if (project == null) {
			throw new IllegalArgumentException("The Digital Twin could not be opened");
		}
		Path folder = filesFolder(project);
		String requested = FOLDER + "/" + source.getFileName();
		// a new file among the project's assets, so the rules for writing one apply
		FileSystemUtil.validateAssetFiles(List.of(requested),
				Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE)));
		String saved;
		// as a Copy into the twin holds it, so the two never commit to its repository at once
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(twinId);
		lock.lock();
		try {
			File written = FileSystemUtil.copyResolvedFile(source.toString(), folder.resolve(requested).toString(),
					false);
			saved = FileSystemUtil.siblingPath(requested, written.getName());
			List<String> dropped = List.of();
			try {
				dropped = dropPlaceholders(folder);
			} catch (IOException e) {
				// the file is in the twin; a placeholder that stays is taken out by the next pass
				classLogger.warn("A Digital Twin's placeholder files could not be removed: {}", e.toString());
			}
			record(user, project, folder, List.of(saved), dropped, "add: " + saved + " from a chat with the twin");
		} finally {
			lock.unlock();
		}
		// counts only: file names stay out of the log
		classLogger.info("Digital Twin file added from a chat: bytes={}", source.toFile().length());
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("file", "agent:/" + saved);
		return out;
	}

	/** Whether this person can change the shared copies belonging to this twin. */
	public static boolean canManage(User user, String twinId) {
		return user != null && twinId != null && SecurityProjectUtils.isDigitalTwin(twinId)
				&& SecurityProjectUtils.isProjectCreator(user, twinId)
				&& SecurityProjectUtils.userCanEditProject(user, twinId);
	}

	/** Whether a room still contains the same twin that proposed a pending file change. */
	public static boolean approvalMatches(Room room, Object expectedTwin) {
		return expectedTwin instanceof String id && !id.isBlank() && room != null && id.equals(agentOf(room));
	}

	/** Removes exactly one shared document from the twin bound to its creator's chat. */
	public static Map<String, Object> remove(User user, Room room, String path) {
		String twinId = room == null ? null : agentOf(room);
		if (!canManage(user, twinId)) {
			throw new IllegalArgumentException("Files can only be removed in a chat with your own Digital Twin");
		}
		IProject project = Utility.getProject(twinId);
		if (project == null) {
			throw new IllegalArgumentException("The Digital Twin could not be opened");
		}
		Path folder = filesFolder(project);
		String removed;
		// The indexer must finish writing any in-flight description before the copy and its entry go away.
		synchronized (WorkTwinIndex.lockFor(twinId)) {
			ReentrantLock lock = ProjectSyncUtility.getProjectLock(twinId);
			lock.lock();
			try {
				List<String> changedAssets = new ArrayList<>();
				removed = removeSharedCopy(folder, path, changedAssets);
				record(user, project, folder.getParent(), changedAssets, List.of(removed),
						"remove: " + removed + " from a chat with the twin");
			} catch (IOException e) {
				throw new IllegalStateException("The shared copy could not be removed", e);
			} finally {
				lock.unlock();
			}
		}
		WorkTwinUpdates.dismissRemovedCopy(user, twinId, removed);
		return Map.of("file", path);
	}

	/** Delete a single listed document and drop its description and source association. Caller holds both locks. */
	static String removeSharedCopy(Path folder, String path, List<String> changedAssets) throws IOException {
		String prefix = "agent:/";
		if (path == null || !path.startsWith(prefix)) {
			throw new IllegalArgumentException("Pass an exact agent:/ path to a shared document");
		}
		Path assets = folder.getParent();
		if (Files.isSymbolicLink(folder) || Files.isSymbolicLink(assets) || Files.isSymbolicLink(assets.getParent())) {
			throw new IllegalArgumentException("The shared files folder cannot be a link");
		}
		String name = path.substring(prefix.length());
		if (name.contains("\\") || name.equalsIgnoreCase("About me.md") || name.equals(AgentFilesIndex.FILE_NAME)) {
			throw new IllegalArgumentException("Pass the path of a shared document copy");
		}
		Path target = folder;
		for (String part : name.split("/", -1)) {
			if (part.isBlank() || part.startsWith(".") || part.equalsIgnoreCase(PLACEHOLDER)
					|| part.contains(":") || part.codePoints().anyMatch(Character::isISOControl)) {
				throw new IllegalArgumentException("Pass the path of a shared document copy");
			}
			target = target.resolve(part);
			if (Files.isSymbolicLink(target)) {
				throw new IllegalArgumentException("Pass the path of a shared document copy");
			}
		}
		String asset = Constants.PUBLIC_ASSETS_FOLDER + "/" + name;
		// An earlier removal may have completed locally while the cluster publication failed.
		if (Files.notExists(target, LinkOption.NOFOLLOW_LINKS)) {
			return asset;
		}
		if (!AgentFilesIndex.listFiles(folder).containsKey(name)
				|| !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)
				|| !target.toRealPath().equals(folder.toRealPath().resolve(name))) {
			throw new IllegalArgumentException("No shared document at that path in this Digital Twin");
		}
		Path sources = assets.resolve(WorkTwinUpdates.RECORD);
		Map<Path, String> replacements = new LinkedHashMap<>();
		if (Files.isRegularFile(sources, LinkOption.NOFOLLOW_LINKS)) {
			JSONObject entries = new JSONObject(Files.readString(sources));
			if (entries.remove(asset) != null) {
				replacements.put(sources, entries.toString());
			}
		}
		Map<String, AgentFilesIndex.Entry> index = AgentFilesIndex.read(folder);
		if (index.remove(name) != null) {
			replacements.put(folder.resolve(AgentFilesIndex.FILE_NAME), AgentFilesIndex.toJson(index));
		}
		if (replacements.isEmpty()) {
			Files.delete(target);
			return asset;
		}

		// Stage outside the shared assets. Atomic replacement also avoids writing through a hard link.
		Path staging = Files.createTempDirectory(assets.getParent(), ".twin-remove-");
		List<Path> updated = new ArrayList<>();
		try {
			for (var entry : replacements.entrySet()) {
				Path file = entry.getKey();
				Files.copy(file, staging.resolve(file.getFileName()), LinkOption.NOFOLLOW_LINKS);
				Files.writeString(staging.resolve(file.getFileName() + ".new"), entry.getValue());
			}
			for (Path file : replacements.keySet()) {
				Files.move(staging.resolve(file.getFileName() + ".new"), file,
						StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
				updated.add(file);
			}
			Files.delete(target);
		} catch (IOException | RuntimeException e) {
			for (Path file : updated) {
				Files.move(staging.resolve(file.getFileName()), file,
						StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			}
			FileUtils.deleteQuietly(staging.toFile());
			throw e;
		}
		FileUtils.deleteQuietly(staging.toFile());
		for (Path file : updated) {
			changedAssets.add(assets.relativize(file).toString().replace('\\', '/'));
		}
		return asset;
	}

	/**
	 * Takes the placeholders out of a Digital Twin's files, as
	 * {@link #dropPlaceholders} does, and records it. Tidying is never why
	 * something else fails, so a problem is logged and no more.
	 *
	 * @param user    the signed in user, who the change is recorded as
	 * @param project the twin, which the caller has checked the user may change
	 */
	public static void tidy(User user, IProject project) {
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(project.getProjectId());
		lock.lock();
		try {
			Path folder = filesFolder(project);
			if (!Files.isDirectory(folder)) {
				return;
			}
			List<String> dropped = dropPlaceholders(folder);
			if (!dropped.isEmpty()) {
				record(user, project, folder, List.of(), dropped,
						"remove: placeholder.txt from folders that hold a file");
			}
		} catch (IOException | RuntimeException e) {
			classLogger.warn("A Digital Twin's placeholder files could not be removed: {}", e.toString());
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Deletes the placeholder.txt the file explorer left in each folder of a
	 * folder of agent files that now holds a file a chat is told about
	 * ({@link AgentFilesIndex#listFiles}), in it or below it. An empty folder keeps
	 * its placeholder, which is what keeps the folder. Only the explorer's own
	 * file goes: one named placeholder.txt that says anything else is someone's
	 * file.
	 *
	 * @return the placeholders deleted, by path inside the folder with forward
	 *         slashes
	 */
	static List<String> dropPlaceholders(Path folder) throws IOException {
		Set<String> files = AgentFilesIndex.listFiles(folder).keySet();
		List<String> dropped = new ArrayList<>();
		if (files.isEmpty()) {
			return dropped;
		}
		List<Path> placeholders;
		try (Stream<Path> walk = Files.walk(folder)) {
			placeholders = walk.filter(path -> PLACEHOLDER.equals(path.getFileName().toString())
					&& Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)).toList();
		} catch (UncheckedIOException e) {
			// a folder that could not be walked to its end
			throw e.getCause();
		}
		for (Path placeholder : placeholders) {
			String name = folder.relativize(placeholder).toString().replace('\\', '/');
			// "" at the top of the folder, else the folder's own path with its slash
			String inFolder = name.substring(0, name.length() - PLACEHOLDER.length());
			if (files.stream().anyMatch(file -> file.startsWith(inFolder))
					// the size first, so someone's large file of that name is never read
					&& Files.size(placeholder) == PLACEHOLDER_TEXT.length()
					&& PLACEHOLDER_TEXT.equals(Files.readString(placeholder, StandardCharsets.US_ASCII))) {
				Files.delete(placeholder);
				dropped.add(name);
			}
		}
		return dropped;
	}

	// Recorded the way a Copy into the project is: committed to its repository, where a problem is logged and no
	// more, since the files are already as they should be, then pushed to shared storage where the server is
	// clustered. The whole files folder is pushed, so a placeholder that went here goes from shared storage too.
	// The caller holds the project's lock.
	private static void record(User user, IProject project, Path folder, List<String> added, List<String> dropped,
			String comment) {
		try {
			String gitFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT,
					project.getEngineId(), project.getEngineName());
			String inRepository = folder.toAbsolutePath().toString().replace('\\', '/') + "/";
			GitRepoUtils.addSpecificFiles(gitFolder, added.stream().map(file -> inRepository + file).toList());
			// the files are gone from disk already; this takes them out of what the repository tracks
			GitDestroyer.removeSpecificFiles(gitFolder, false,
					dropped.stream().map(file -> inRepository + file).toList());
			AccessToken login = user.getAccessToken(user.getPrimaryLogin());
			GitRepoUtils.commitAddedFiles(gitFolder, comment, login.getResolvedUsername(), login.getEmail());
		} catch (RuntimeException e) {
			classLogger.error("A change to a Digital Twin's files could not be recorded in git", e);
		}
		ClusterUtil.pushProjectFolder(project, folder.toString());
	}

	private static Path filesFolder(IProject project) {
		return Path.of(EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getEngineId(), project.getEngineName()), Constants.PUBLIC_ASSETS_FOLDER);
	}

	// the agent a chat runs, as its options hold it: an id, or the id and name
	private static String agentOf(Room room) {
		Object workspace = room.getOptionsMap() == null ? null : room.getOptionsMap().get("workspace");
		Object id = workspace instanceof Map<?, ?> named ? named.get("workspace_id") : workspace;
		return id instanceof String text && !text.isBlank() ? text.trim() : null;
	}

	/**
	 * A file in a chat's folder, by the path the chat's file tools know it by. The
	 * real path, so neither .. nor a link inside the folder leads out of it.
	 *
	 * @throws IllegalArgumentException when no file is there, or it is not in the
	 *                                  folder
	 */
	static Path chatFile(Path chatFolder, String path) {
		if (path == null || path.isBlank()) {
			throw new IllegalArgumentException("Pass the path of a file in this chat's folder");
		}
		String clean = path.trim();
		try {
			Path root = chatFolder.toRealPath();
			Path file = root.resolve(clean);
			if (!Files.exists(file)) {
				// an upload stores "R&D.pptx" as "R&amp;D.pptx", and a model asks for the name as it is written
				int name = clean.lastIndexOf('/') + 1;
				file = root.resolve(clean.substring(0, name) + Utility.inputSanitizer(clean.substring(name)));
			}
			file = file.toRealPath();
			if (file.startsWith(root) && Files.isRegularFile(file)) {
				return file;
			}
		} catch (IOException | RuntimeException e) {
			// nothing there, or not a path at all: the answer below
		}
		throw new IllegalArgumentException("No file at " + clean + " in this chat's folder. Pass a path relative "
				+ "to that folder, such as notes/about-me.md; a folder cannot be added.");
	}
}
