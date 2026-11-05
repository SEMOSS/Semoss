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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.ws.rs.core.Response;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.couch.CouchException;
import prerna.io.connector.couch.CouchUtil;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * Clones an engine of any type into a new private engine owned by the user who
 * clones it. The clone gets a new id and name, and a copy of everything the
 * source keeps on this server:
 * <ul>
 * <li>the engine folder: the H2 or SQLite database file, the OWL, vector index
 * files, python files, a virtual environment, and the version history;</li>
 * <li>the smss file and any secrets held in the secret store, with the source
 * id and folder name replaced by the clone's;</li>
 * <li>the engine metadata (description, tags, markdown and the rest), the model
 * metadata for a model, and an uploaded engine image, wherever images are kept
 * (CouchDB, the cluster image folder, or the engine folder).</li>
 * </ul>
 * Connection settings are copied as they are, so a clone of an engine backed by
 * an external system (a remote database, a pgvector table, a storage bucket, a
 * model endpoint) reads and writes the same external data as the source.
 * Deleting an engine only removes its local files, so deleting the clone leaves
 * that external data in place.
 */
public final class EngineCloneUtility {

	private static final Logger classLogger = LogManager.getLogger(EngineCloneUtility.class);

	/** The file at the root of a python virtual environment. */
	private static final String VENV_CONFIG_FILE = "pyvenv.cfg";
	/**
	 * The folders of a virtual environment whose scripts hold its absolute path.
	 */
	private static final List<String> VENV_SCRIPT_FOLDERS = List.of("bin", "Scripts");
	/** How deep under the engine folder to look for a virtual environment. */
	private static final int VENV_SEARCH_DEPTH = 4;

	private EngineCloneUtility() {

	}

	/**
	 * Clones an engine into a new private engine named {@code cloneName} and owned
	 * by {@code user}, then pushes the clone to cloud storage in a cluster. An
	 * engine that holds file locks, such as an H2 database, is closed while its
	 * files are copied so the copy is consistent, and opened again afterwards.
	 * Callers check that the user may clone the source; this method does no
	 * permission checks. On failure everything written for the clone is removed.
	 *
	 * @param user           the user who owns the clone
	 * @param sourceEngineId the engine to clone
	 * @param cloneName      the name of the clone
	 * @param logger         logger for the progress messages shown to the user
	 * @return the id of the clone
	 */
	public static String cloneEngine(User user, String sourceEngineId, String cloneName, Logger logger) {
		IEngine source = Utility.getEngine(sourceEngineId);
		if (source == null) {
			throw new IllegalArgumentException("Engine " + sourceEngineId + " does not exist or could not be loaded");
		}
		String sourceSmssPath = source.getSmssFilePath();
		if (sourceSmssPath == null || !new File(sourceSmssPath).isFile()) {
			throw new IllegalArgumentException("Engine " + sourceEngineId + " has no smss file to clone");
		}

		IEngine.CATALOG_TYPE engineType = source.getCatalogType();
		String sourceName = source.getEngineName();
		String cloneId = UUID.randomUUID().toString();

		File sourceFolder = new File(EngineUtility.getSpecificEngineBaseFolder(engineType, sourceEngineId, sourceName));
		File cloneFolder = new File(EngineUtility.getSpecificEngineBaseFolder(engineType, cloneId, cloneName));
		File cloneSmss = new File(Utility.normalizePath(EngineUtility.getLocalEngineBaseDirectory(engineType) + "/"
				+ SmssUtilities.getUniqueName(cloneName, cloneId) + Constants.SEMOSS_EXTENSION));
		try {
			UploadUtilities.validateEngine(engineType, user, cloneName, cloneId);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}

		IdentityRewrite rewrite = new IdentityRewrite(sourceEngineId, sourceName, cloneId, cloneName);
		ISecrets secretStore = SecretsFactory.getSecretConnector();
		File cloneImage = null;
		boolean couchImageCopied = false;
		boolean pushStarted = false;
		boolean engineAddedToDIHelper = false;
		boolean success = false;
		try {
			// list the clone before its smss exists so the smss watcher skips it
			addToDIHelperEngines(cloneId);
			engineAddedToDIHelper = true;

			logger.info("Copying the engine files...");
			copyEngineFiles(source, sourceFolder, new File(sourceSmssPath), cloneFolder, cloneSmss);
			logger.info("Done copying the engine files");

			logger.info("Setting the id and name of the clone...");
			rewriteSmss(cloneSmss, rewrite);
			if (secretStore != null) {
				copySecrets(secretStore, engineType, rewrite);
			}
			rewriteVirtualEnvPaths(cloneFolder, sourceFolder, rewrite);
			logger.info("Done setting the id and name of the clone");

			logger.info("Registering the clone...");
			DIHelper.getInstance().setEngineProperty(cloneId + "_" + Constants.STORE, cloneSmss.getAbsolutePath());
			if (IEngine.CATALOG_TYPE.DATABASE == engineType) {
				Utility.synchronizeEngineMetadata(cloneId);
			}
			SecurityEngineUtils.addEngine(cloneId, false, user);
			copyMetadata(engineType, sourceEngineId, cloneId);
			for (AuthProvider login : user.getLogins()) {
				SecurityEngineUtils.addEngineOwner(cloneId, user.getAccessToken(login).getId());
			}
			// the image is not needed for the clone to work, so a failed copy leaves the
			// clone with the stock image
			try {
				if (CouchUtil.COUCH_ENABLED) {
					couchImageCopied = copyCouchImage(engineType, sourceEngineId, cloneId);
				} else if (ClusterUtil.IS_CLUSTER) {
					cloneImage = copyClusterImage(engineType, sourceEngineId, cloneId);
				}
				// otherwise the image sits in the engine folder and was copied with it
			} catch (Exception e) {
				classLogger.warn("Failed to copy the image of engine {} to the clone {}",
						Utility.cleanLogString(sourceEngineId), cloneId, e);
			}
			logger.info("Done registering the clone");

			pushStarted = true;
			try {
				ClusterUtil.pushEngine(cloneId);
			} catch (SemossPixelException e) {
				if (!e.isContinueThreadOfExecution()) {
					throw e;
				}
				// the clone is in cloud storage and only the notice to the other nodes
				// failed, which they do not need for an engine none of them has open
				classLogger.warn("Pushed the clone {} but could not notify the other nodes", cloneId, e);
			}
			success = true;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to clone engine {} into {}", Utility.cleanLogString(sourceEngineId), cloneId, e);
			throw new SemossPixelException("Failed to clone the engine: " + e.getMessage(), false);
		} finally {
			if (!success) {
				cleanUpFailedClone(engineType, rewrite, secretStore, engineAddedToDIHelper, pushStarted, cloneFolder,
						cloneSmss, cloneImage, couchImageCopied);
			}
		}
		return cloneId;
	}

	/**
	 * Copies the engine folder and smss file of the source. A source that holds
	 * file locks is closed under its engine lock for the copy and opened again
	 * after, the same way an export does; when it cannot be closed, nothing is
	 * copied. Symbolic links are copied as links, so a virtual environment keeps
	 * pointing at its base python.
	 *
	 * @param source       the engine to copy
	 * @param sourceFolder the engine folder of the source
	 * @param sourceSmss   the smss file of the source
	 * @param cloneFolder  the engine folder to create for the clone
	 * @param cloneSmss    the smss file to create for the clone
	 * @throws IOException if a file cannot be copied
	 */
	private static void copyEngineFiles(IEngine source, File sourceFolder, File sourceSmss, File cloneFolder,
			File cloneSmss) throws IOException {
		String sourceEngineId = source.getEngineId();
		ReentrantLock lock = null;
		if (source.holdsFileLocks()) {
			lock = EngineSyncUtility.getEngineLock(sourceEngineId);
			lock.lock();
		}
		boolean removed = false;
		try {
			if (lock != null) {
				// close the instance open now, which may not be the one read before the lock
				Object open = DIHelper.getInstance().getEngineProperty(sourceEngineId);
				if (open instanceof IEngine openEngine) {
					classLogger.info("Closing engine {} to copy its files", Utility.cleanLogString(sourceEngineId));
					DIHelper.getInstance().removeEngineProperty(sourceEngineId);
					removed = true;
					// a copy of files the engine still holds would not be consistent
					openEngine.close();
				}
			}
			if (sourceFolder.exists()) {
				PathUtils.copyDirectory(sourceFolder.toPath(), cloneFolder.toPath(), LinkOption.NOFOLLOW_LINKS,
						StandardCopyOption.COPY_ATTRIBUTES);
			} else {
				cloneFolder.mkdirs();
			}
			Files.copy(sourceSmss.toPath(), cloneSmss.toPath(), StandardCopyOption.COPY_ATTRIBUTES);
		} finally {
			try {
				if (removed) {
					classLogger.info("Opening engine {} again after copying its files",
							Utility.cleanLogString(sourceEngineId));
					Utility.getEngine(sourceEngineId);
				}
			} catch (Exception e) {
				// the clone does not depend on the source being open again
				classLogger.error("Failed to open engine {} again after copying its files",
						Utility.cleanLogString(sourceEngineId), e);
			} finally {
				if (lock != null) {
					lock.unlock();
				}
			}
		}
	}

	/**
	 * Points the copied smss file at the clone: every value that holds the source
	 * id or folder name gets the clone's, and the engine id, alias and display name
	 * keys get the clone's id and name. Other keys keep their values; NAME, for
	 * one, can hold a provider's model id rather than the engine's name.
	 *
	 * @param cloneSmss the copied smss file
	 * @param rewrite   the source and clone identities
	 * @throws IOException if the file cannot be read or written
	 */
	private static void rewriteSmss(File cloneSmss, IdentityRewrite rewrite) throws IOException {
		Path smssPath = cloneSmss.toPath();
		String content = Files.readString(smssPath, StandardCharsets.UTF_8);
		Files.writeString(smssPath, rewrite.apply(content), StandardCharsets.UTF_8);

		Map<String, String> identity = new LinkedHashMap<>();
		identity.put(Constants.ENGINE, rewrite.cloneId);
		identity.put(Constants.ENGINE_ALIAS, rewrite.cloneName);
		identity.put(Constants.ENGINE_DISPLAY_NAME, rewrite.cloneName);
		Utility.changePropertiesFileValue(cloneSmss.getAbsolutePath(), identity, false);
	}

	/**
	 * Copies the secrets the secret store holds for the source to the clone, with
	 * the same id and name changes as the smss file. The engine merges these over
	 * its smss values when it opens, so the identity keys are set as well.
	 *
	 * @param secretStore the configured secret store
	 * @param engineType  the type of the source and clone
	 * @param rewrite     the source and clone identities
	 */
	private static void copySecrets(ISecrets secretStore, IEngine.CATALOG_TYPE engineType, IdentityRewrite rewrite) {
		Map<String, Object> sourceSecrets = secretStore.getEngineSecrets(engineType, rewrite.sourceId,
				rewrite.sourceName);
		if (sourceSecrets == null || sourceSecrets.isEmpty()) {
			return;
		}
		Map<String, Object> cloneSecrets = new HashMap<>();
		for (Map.Entry<String, Object> entry : sourceSecrets.entrySet()) {
			Object value = entry.getValue();
			cloneSecrets.put(entry.getKey(), value instanceof String ? rewrite.apply((String) value) : value);
		}
		cloneSecrets.computeIfPresent(Constants.ENGINE, (k, v) -> rewrite.cloneId);
		cloneSecrets.computeIfPresent(Constants.ENGINE_ALIAS, (k, v) -> rewrite.cloneName);
		cloneSecrets.computeIfPresent(Constants.ENGINE_DISPLAY_NAME, (k, v) -> rewrite.cloneName);
		if (!secretStore.writeEngineSecrets(engineType, rewrite.cloneId, rewrite.cloneName, cloneSecrets)) {
			throw new IllegalStateException("Failed to write the secrets of the clone to the secret store");
		}
	}

	/**
	 * Points every python virtual environment inside the copied folder at its new
	 * location. A virtual environment writes its absolute path into
	 * {@code pyvenv.cfg} and into the scripts under {@code bin} or {@code Scripts}
	 * (activate scripts and the shebang line of pip and every console script), so
	 * without this the clone's pip would install into the source environment.
	 *
	 * @param cloneFolder  the copied engine folder
	 * @param sourceFolder the engine folder of the source
	 * @param rewrite      the source and clone identities
	 * @throws IOException if the folder cannot be searched or a file rewritten
	 */
	private static void rewriteVirtualEnvPaths(File cloneFolder, File sourceFolder, IdentityRewrite rewrite)
			throws IOException {
		if (!cloneFolder.isDirectory()) {
			return;
		}
		List<Path> venvConfigs;
		try (Stream<Path> files = Files.find(cloneFolder.toPath(), VENV_SEARCH_DEPTH,
				(path, attrs) -> attrs.isRegularFile() && VENV_CONFIG_FILE.equals(path.getFileName().toString()))) {
			venvConfigs = files.collect(Collectors.toList());
		}
		if (venvConfigs.isEmpty()) {
			return;
		}

		// the scripts hold the folder in the form the OS wrote it
		Map<String, String> folderPaths = new LinkedHashMap<>();
		String sourcePath = sourceFolder.getAbsolutePath();
		String clonePath = cloneFolder.getAbsolutePath();
		folderPaths.put(sourcePath, clonePath);
		folderPaths.put(sourcePath.replace('\\', '/'), clonePath.replace('\\', '/'));

		for (Path venvConfig : venvConfigs) {
			rewriteTextFile(venvConfig, folderPaths, rewrite);
			Path venvRoot = venvConfig.getParent();
			for (String scriptFolder : VENV_SCRIPT_FOLDERS) {
				Path scripts = venvRoot.resolve(scriptFolder);
				if (!Files.isDirectory(scripts, LinkOption.NOFOLLOW_LINKS)) {
					continue;
				}
				try (Stream<Path> scriptFiles = Files.list(scripts)) {
					for (Path script : (Iterable<Path>) scriptFiles::iterator) {
						if (Files.isRegularFile(script, LinkOption.NOFOLLOW_LINKS)) {
							rewriteTextFile(script, folderPaths, rewrite);
						}
					}
				}
			}
			classLogger.info("Pointed the virtual environment at {}", venvRoot);
		}
	}

	/**
	 * Replaces the source folder path with the clone folder path in a text file,
	 * then the source id and folder name. Binary files, meaning files with a zero
	 * byte or that are not valid UTF-8, and files without the source path are left
	 * untouched.
	 * 
	 * @param file        the file to rewrite
	 * @param folderPaths source folder path to clone folder path, in each form
	 * @param rewrite     the source and clone identities
	 * @throws IOException if the file cannot be read or written
	 */
	private static void rewriteTextFile(Path file, Map<String, String> folderPaths, IdentityRewrite rewrite)
			throws IOException {
		byte[] bytes = Files.readAllBytes(file);
		for (byte b : bytes) {
			if (b == 0) {
				return;
			}
		}
		String content;
		try {
			content = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
		} catch (CharacterCodingException e) {
			return;
		}
		String updated = content;
		for (Map.Entry<String, String> folderPath : folderPaths.entrySet()) {
			updated = updated.replace(folderPath.getKey(), folderPath.getValue());
		}
		updated = rewrite.apply(updated);
		if (!updated.equals(content)) {
			Files.writeString(file, updated, StandardCharsets.UTF_8);
		}
	}

	/**
	 * Copies the engine metadata of the source to the clone, and for a model its
	 * model metadata.
	 *
	 * @param engineType     the type of the source and clone
	 * @param sourceEngineId the engine cloned
	 * @param cloneId        the clone
	 */
	private static void copyMetadata(IEngine.CATALOG_TYPE engineType, String sourceEngineId, String cloneId) {
		Map<String, Object> engineMetadata = SecurityEngineUtils.getAggregateEngineMetadata(sourceEngineId, null,
				false);
		if (engineMetadata != null && !engineMetadata.isEmpty()) {
			SecurityEngineUtils.updateEngineMetadata(cloneId, engineMetadata);
		}
		if (IEngine.CATALOG_TYPE.MODEL == engineType) {
			Map<String, Object> modelMetadata = SecurityModelMetadataUtils.getModelMetadata(sourceEngineId);
			if (modelMetadata != null && !modelMetadata.isEmpty()) {
				SecurityModelMetadataUtils.restoreModelMetadata(cloneId, modelMetadata);
			}
		}
	}

	/**
	 * Copies the image uploaded for the source in CouchDB to the clone. A source
	 * without an image document shows the stock image and has nothing to copy.
	 * 
	 * @param engineType     the type of the source and clone
	 * @param sourceEngineId the engine cloned
	 * @param cloneId        the clone
	 * @return true if an image was copied
	 * @throws CouchException if the image cannot be read or written
	 * @throws IOException    if the image cannot be staged in a temporary file
	 */
	private static boolean copyCouchImage(IEngine.CATALOG_TYPE engineType, String sourceEngineId, String cloneId)
			throws CouchException, IOException {
		String partition = EngineUtility.getCouchSelector(engineType);
		String sourceSelector = "{\"selector\": {\"" + partition + "\":{\"$eq\":\"" + sourceEngineId + "\"}}}";
		if (!CouchUtil.documentExists(partition, sourceSelector)) {
			return false;
		}
		Response download = CouchUtil.download(partition, Map.of(partition, sourceEngineId));
		if (!(download.getEntity() instanceof byte[])) {
			return false;
		}
		Path stagedImage = Files.createTempFile(cloneId, ".image");
		try {
			Files.write(stagedImage, (byte[]) download.getEntity());
			CouchUtil.upload(partition, Map.of(partition, cloneId), stagedImage.toFile());
		} finally {
			Files.deleteIfExists(stagedImage);
		}
		return true;
	}

	/**
	 * Copies the image uploaded for the source in the cluster image folder to the
	 * clone and pushes it to cloud storage. A source showing the stock image has
	 * nothing to copy.
	 * 
	 * @param engineType     the type of the source and clone
	 * @param sourceEngineId the engine cloned
	 * @param cloneId        the clone
	 * @return the image written for the clone, or null when the source has none
	 * @throws Exception if the image cannot be read, copied or pushed
	 */
	private static File copyClusterImage(IEngine.CATALOG_TYPE engineType, String sourceEngineId, String cloneId)
			throws Exception {
		File sourceImage = ClusterUtil.getEngineAndProjectImage(sourceEngineId, engineType);
		if (sourceImage == null || !sourceImage.isFile() || !sourceImage.getName().startsWith(sourceEngineId)) {
			return null;
		}
		String cloneImageName = cloneId + sourceImage.getName().substring(sourceEngineId.length());
		File cloneImage = new File(sourceImage.getParentFile(), cloneImageName);
		Files.copy(sourceImage.toPath(), cloneImage.toPath(), StandardCopyOption.REPLACE_EXISTING);
		ClusterUtil.pushEngineAndProjectImage(engineType, cloneImageName);
		return cloneImage;
	}

	/**
	 * Adds an engine id to the list of engines in DIHelper.
	 *
	 * @param engineId the engine to add
	 */
	private static void addToDIHelperEngines(String engineId) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
		if (!(engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
				|| engines.endsWith(";" + engineId))) {
			DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engines + ";" + engineId);
		}
	}

	/**
	 * Removes everything a failed clone wrote: the open clone, its cloud copy, its
	 * files, image, DIHelper entries, local master and security rows, and secrets.
	 * Each step runs even when an earlier one fails.
	 *
	 * @param engineType            the type of the clone
	 * @param rewrite               the source and clone identities
	 * @param secretStore           the configured secret store, or null
	 * @param engineAddedToDIHelper whether the clone was added to DIHelper
	 * @param pushStarted           whether the clone was being pushed to cloud
	 *                              storage
	 * @param cloneFolder           the engine folder of the clone
	 * @param cloneSmss             the smss file of the clone
	 * @param cloneImage            the cluster image of the clone, or null
	 * @param couchImageCopied      whether an image was written to CouchDB
	 */
	private static void cleanUpFailedClone(IEngine.CATALOG_TYPE engineType, IdentityRewrite rewrite,
			ISecrets secretStore, boolean engineAddedToDIHelper, boolean pushStarted, File cloneFolder, File cloneSmss,
			File cloneImage, boolean couchImageCopied) {
		String cloneId = rewrite.cloneId;
		// the cloud push opens the clone, which holds its files open until closed
		if (DIHelper.getInstance().getEngineProperty(cloneId) instanceof IEngine openClone) {
			try {
				openClone.close();
			} catch (Exception e) {
				classLogger.error("Failed to close the failed clone {}", cloneId, e);
			}
		}
		if (pushStarted && ClusterUtil.IS_CLUSTER) {
			try {
				ClusterUtil.deleteEngine(cloneId, engineType);
			} catch (Exception e) {
				classLogger.error("Failed to delete the cloud copy of the failed clone {}", cloneId, e);
			}
		}
		if (cloneImage != null) {
			try {
				ClusterUtil.deleteEngineAndProjectImage(engineType, cloneImage.getName());
			} catch (Exception e) {
				classLogger.error("Failed to delete the cloud image of the failed clone {}", cloneId, e);
			}
		}
		if (couchImageCopied) {
			try {
				String partition = EngineUtility.getCouchSelector(engineType);
				CouchUtil.delete(partition, Map.of(partition, cloneId));
			} catch (Exception e) {
				classLogger.error("Failed to delete the CouchDB image of the failed clone {}", cloneId, e);
			}
		}
		for (File f : new File[] { cloneFolder, cloneSmss, cloneImage }) {
			if (f != null && f.exists()) {
				try {
					FileUtils.forceDelete(f);
				} catch (IOException e) {
					classLogger.error("Failed to delete {} while cleaning up the failed clone {}", f.getAbsolutePath(),
							cloneId, e);
				}
			}
		}
		if (engineAddedToDIHelper) {
			UploadUtilities.removeEngineFromDIHelper(cloneId);
		}
		if (IEngine.CATALOG_TYPE.DATABASE == engineType) {
			try {
				new DeleteFromMasterDB().deleteEngineRDBMS(cloneId);
			} catch (Exception e) {
				classLogger.error("Failed to remove the failed clone {} from the local master", cloneId, e);
			}
		}
		try {
			SecurityEngineUtils.deleteEngine(cloneId);
		} catch (Exception e) {
			classLogger.error("Failed to remove the failed clone {} from the security database", cloneId, e);
		}
		if (secretStore != null) {
			try {
				secretStore.deleteEngineSecrets(engineType, cloneId, rewrite.cloneName);
			} catch (Exception e) {
				classLogger.error("Failed to delete the secrets of the failed clone {}", cloneId, e);
			}
		}
	}

	/**
	 * The source and clone identities, and the text replacement that points a value
	 * at the clone. The folder name ({@code name__id}) is replaced first, then any
	 * remaining source id. The source name on its own is kept, since files inside
	 * the folder keep the names they were copied with (the OWL file of a database,
	 * for example).
	 */
	private static final class IdentityRewrite {

		private final String sourceId;
		private final String sourceName;
		private final String cloneId;
		private final String cloneName;
		private final String sourceUniqueName;
		private final String cloneUniqueName;

		private IdentityRewrite(String sourceId, String sourceName, String cloneId, String cloneName) {
			this.sourceId = sourceId;
			this.sourceName = sourceName;
			this.cloneId = cloneId;
			this.cloneName = cloneName;
			this.sourceUniqueName = SmssUtilities.getUniqueName(sourceName, sourceId);
			this.cloneUniqueName = SmssUtilities.getUniqueName(cloneName, cloneId);
		}

		/**
		 * Replaces the source folder name and id in a value with the clone's.
		 *
		 * @param value the text to rewrite
		 * @return the text pointing at the clone
		 */
		private String apply(String value) {
			return value.replace(this.sourceUniqueName, this.cloneUniqueName).replace(this.sourceId, this.cloneId);
		}
	}
}
