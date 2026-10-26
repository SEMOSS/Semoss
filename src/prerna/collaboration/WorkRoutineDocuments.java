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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

/**
 * The owner's reference documents for one of their routines: a folder in their user assets at
 * collaboration/routines/{routineId}. Routines shows it in the user-assets file explorer, where the owner uploads,
 * edits, renames and deletes with the user asset reactors; before a run's agent step starts, the automation copies
 * the folder's files into the run room's documents folder, so a run reads a copy and never the originals.
 */
public final class WorkRoutineDocuments {
	private static final Logger classLogger = LogManager.getLogger(WorkRoutineDocuments.class);

	// under the owner's user assets
	static final String ROOT = "collaboration/routines";
	/** The folder in the routine room the documents are copied to. */
	public static final String FOLDER = "documents";

	private WorkRoutineDocuments() { }

	/** Makes sure the routine's folder exists; its path in the owner's user assets, and the documents in it. */
	public static Map<String, Object> open(User user, String routineId) throws IOException {
		WorkCustomRoutine.requireStored(user, routineId);
		Path folder = folder(user, routineId);
		Files.createDirectories(folder);
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("path", "/" + ROOT + "/" + folder.getFileName());
		result.put("documents", list(folder));
		return result;
	}

	/** Removes a deleted routine's documents; best effort, the routine itself is already gone. */
	public static void deleteAll(User user, String routineId) {
		try {
			Path folder = folder(user, routineId);
			if (!Files.isDirectory(folder, LinkOption.NOFOLLOW_LINKS)) {
				return;
			}
			try (Stream<Path> listed = Files.list(folder)) {
				for (Path file : listed.toList()) {
					Files.deleteIfExists(file);
				}
			}
			Files.deleteIfExists(folder);
			commit(user, "remove: routine documents " + folder.getFileName());
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Could not remove the documents of routine {}", routineId, e);
		}
	}

	/** Copies the documents into the room's documents folder; the names copied, sorted. */
	public static List<String> copyInto(User user, String routineId, String roomFolder) throws IOException {
		WorkCustomRoutine.requireStored(user, routineId);
		return copyInto(folder(user, routineId), Path.of(roomFolder, FOLDER));
	}

	/** The document names, sorted, for the routine's command. */
	public static List<String> names(User user, String routineId) {
		try {
			List<String> names = new ArrayList<>();
			for (Path file : files(folder(user, routineId))) {
				names.add(file.getFileName().toString());
			}
			return names;
		} catch (IOException e) {
			throw new IllegalStateException("Could not read the routine's reference documents", e);
		}
	}

	static List<Map<String, Object>> list(Path folder) throws IOException {
		List<Map<String, Object>> documents = new ArrayList<>();
		for (Path file : files(folder)) {
			Map<String, Object> document = new LinkedHashMap<>();
			document.put("name", file.getFileName().toString());
			document.put("size", Files.size(file));
			document.put("updatedAt", Files.getLastModifiedTime(file).toInstant().toString());
			documents.add(document);
		}
		return documents;
	}

	static List<String> copyInto(Path folder, Path target) throws IOException {
		List<String> names = new ArrayList<>();
		List<Path> files = files(folder);
		if (files.isEmpty()) {
			return names;
		}
		Files.createDirectories(target);
		for (Path file : files) {
			String fileName = file.getFileName().toString();
			Files.copy(file, target.resolve(fileName), StandardCopyOption.REPLACE_EXISTING);
			names.add(fileName);
		}
		return names;
	}

	// regular, visible files, sorted by name; none for a missing folder. Subfolders the owner makes are not copied.
	private static List<Path> files(Path folder) throws IOException {
		if (!Files.isDirectory(folder, LinkOption.NOFOLLOW_LINKS)) {
			return List.of();
		}
		try (Stream<Path> listed = Files.list(folder)) {
			return listed.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
					.filter(p -> !p.getFileName().toString().startsWith(".")).sorted().toList();
		}
	}

	// routine ids are UUIDs, so the folder name can never leave ROOT
	static Path folder(User user, String routineId) {
		IProject project = assetProject(user);
		return Path.of(AssetUtility.getUserAssetFolder(project.getProjectName(), project.getProjectId()), ROOT,
				UUID.fromString(String.valueOf(routineId).trim()).toString());
	}

	private static IProject assetProject(User user) {
		IProject project = user.getAssetProject();
		if (project == null) {
			throw new IllegalArgumentException("Unable to find user asset app");
		}
		return project;
	}

	// the user asset folder is a git repository synced to the cluster; record the removal like DeleteUserAssets does
	private static void commit(User user, String message) {
		IProject project = assetProject(user);
		String gitFolder = AssetUtility.getUserAssetVersionFolder(project.getProjectName(), project.getProjectId());
		try (Git git = Git.open(new File(gitFolder))) {
			git.add().addFilepattern(".").setUpdate(true).call();
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			GitRepoUtils.commitAddedFiles(gitFolder, message, token.getResolvedUsername(), token.getEmail());
		} catch (Exception e) {
			classLogger.error("Could not record the routine document removal in the user asset repository", e);
		}
		ClusterUtil.pushUserAsset(project.getProjectId());
	}
}
