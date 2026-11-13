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

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.couch.CouchUtil;

/**
 * Resolves where engines, their images and user assets live on disk. Every path
 * is built from the base folder the RDF_Map names when it is asked for, so a
 * base folder set after this class loads is the one used.
 */
public class EngineUtility {

	public static final String LOCAL_DATABASE_IMAGE_RELPATH = "images/databases";
	public static final String LOCAL_STORAGE_IMAGE_RELPATH = "images/storages";
	public static final String LOCAL_MODEL_IMAGE_RELPATH = "images/models";
	public static final String LOCAL_VECTOR_IMAGE_RELPATH = "images/vectors";
	public static final String LOCAL_FUNCTION_IMAGE_RELPATH = "images/functions";
	public static final String LOCAL_GUARDRAIL_IMAGE_RELPATH = "images/guardrail";
	public static final String LOCAL_PROJECT_IMAGE_RELPATH = "images/projects";
	public static final String LOCAL_VENV_IMAGE_RELPATH = "images/venv";

	/**
	 * The folder that holds every database, under the base folder this class saw
	 * when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code DATABASE}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String DATABASE_FOLDER = folderAtLoad(Constants.DATABASE_FOLDER);

	/**
	 * The folder that holds every guardrail, under the base folder this class saw
	 * when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code GUARDRAIL}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String GUARDRAIL_FOLDER = folderAtLoad(Constants.GUARDRAIL_FOLDER);

	/**
	 * The folder that holds every function, under the base folder this class saw
	 * when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code FUNCTION}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String FUNCTION_FOLDER = folderAtLoad(Constants.FUNCTION_FOLDER);

	/**
	 * The folder that holds every model, under the base folder this class saw when
	 * it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code MODEL}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String MODEL_FOLDER = folderAtLoad(Constants.MODEL_FOLDER);

	/**
	 * The folder that holds every storage, under the base folder this class saw
	 * when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code STORAGE}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String STORAGE_FOLDER = folderAtLoad(Constants.STORAGE_FOLDER);

	/**
	 * The folder that holds every vector database, under the base folder this class
	 * saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code VECTOR}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String VECTOR_FOLDER = folderAtLoad(Constants.VECTOR_FOLDER);

	/**
	 * The folder that holds every virtual environment, under the base folder this
	 * class saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code VENV}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String VENV_FOLDER = folderAtLoad(Constants.VENV_FOLDER);

	/**
	 * The folder that holds every project, under the base folder this class saw
	 * when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code PROJECT}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String PROJECT_FOLDER = folderAtLoad(Constants.PROJECT_FOLDER);

	/**
	 * The folder that holds every room, under the base folder this class saw when
	 * it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getRoomFolder()}, which reads the base folder on each
	 *             call
	 */
	@Deprecated
	public static final String ROOM_FOLDER = folderAtLoad(Constants.ROOM_FOLDER);

	/**
	 * The folder that holds each user's asset folder, under the base folder this
	 * class saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getUserFolder()}, which reads the base folder on each
	 *             call
	 */
	@Deprecated
	public static final String USER_FOLDER = folderAtLoad(Constants.USER_FOLDER);

	/**
	 * The folder that holds every database image, under the base folder this class
	 * saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code DATABASE}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String DATABASE_IMAGE_FOLDER = folderAtLoad(LOCAL_DATABASE_IMAGE_RELPATH);

	/**
	 * The folder that holds every storage image, under the base folder this class
	 * saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code STORAGE}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String STORAGE_IMAGE_FOLDER = folderAtLoad(LOCAL_STORAGE_IMAGE_RELPATH);

	/**
	 * The folder that holds every model image, under the base folder this class saw
	 * when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code MODEL}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String MODEL_IMAGE_FOLDER = folderAtLoad(LOCAL_MODEL_IMAGE_RELPATH);

	/**
	 * The folder that holds every vector database image, under the base folder this
	 * class saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code VECTOR}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String VECTOR_IMAGE_FOLDER = folderAtLoad(LOCAL_VECTOR_IMAGE_RELPATH);

	/**
	 * The folder that holds every function image, under the base folder this class
	 * saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code FUNCTION}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String FUNCTION_IMAGE_FOLDER = folderAtLoad(LOCAL_FUNCTION_IMAGE_RELPATH);

	/**
	 * The folder that holds every guardrail image, under the base folder this class
	 * saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code GUARDRAIL}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String GUARDRAIL_IMAGE_FOLDER = folderAtLoad(LOCAL_GUARDRAIL_IMAGE_RELPATH);

	/**
	 * The folder that holds every project image, under the base folder this class
	 * saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code PROJECT}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String PROJECT_IMAGE_FOLDER = folderAtLoad(LOCAL_PROJECT_IMAGE_RELPATH);

	/**
	 * The folder that holds every virtual environment image, under the base folder
	 * this class saw when it loaded, or null when no base folder was set yet.
	 *
	 * @deprecated use {@link #getLocalEngineImageDirectory(IEngine.CATALOG_TYPE)}
	 *             with {@code VENV}, which reads the base folder on each call
	 */
	@Deprecated
	public static final String VENV_IMAGE_FOLDER = folderAtLoad(LOCAL_VENV_IMAGE_RELPATH);

	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static String getSpecificEngineBaseFolder(String engineId) {
		IEngine.CATALOG_TYPE catalogType = SecurityEngineUtils.getEngineType(engineId);
		String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
		return EngineUtility.getSpecificEngineBaseFolder(catalogType, engineId, engineName);
	}

	/**
	 * 
	 * @param type
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	public static String getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE type, String engineId, String engineName) {
		return getSpecificEngineBaseFolder(type, SmssUtilities.getUniqueName(engineName, engineId));
	}

	/**
	 * 
	 * @param type
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	public static String getSpecificEngineAppRootFolder(IEngine.CATALOG_TYPE type, String engineId, String engineName) {
		return getSpecificEngineAppRootFolder(type, SmssUtilities.getUniqueName(engineName, engineId));
	}

	/**
	 * 
	 * @param type
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	public static String getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE type, String engineId, String engineName) {
		return getSpecificEngineVersionFolder(type, SmssUtilities.getUniqueName(engineName, engineId));
	}

	/**
	 * 
	 * @param type
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	public static String getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE type, String engineId, String engineName) {
		return getSpecificEngineAssetsFolder(type, SmssUtilities.getUniqueName(engineName, engineId));
	}

	/**
	 * 
	 * @param type
	 * @param engineIdAndName
	 * @return
	 */
	public static String getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE type, String engineIdAndName) {
		String baseEngineFolder = getLocalEngineBaseDirectory(type);
		return baseEngineFolder + "/" + engineIdAndName;
	}

	/**
	 * 
	 * @param type
	 * @param engineIdAndName
	 * @return
	 */
	public static String getSpecificEngineAppRootFolder(IEngine.CATALOG_TYPE type, String engineIdAndName) {
		String baseEngineFolder = getLocalEngineBaseDirectory(type);
		return baseEngineFolder + "/" + engineIdAndName + "/" + Constants.APP_ROOT_FOLDER;
	}

	/**
	 * 
	 * @param type
	 * @param engineIdAndName
	 * @return
	 */
	public static String getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE type, String engineIdAndName) {
		String baseEngineFolder = getLocalEngineBaseDirectory(type);
		return baseEngineFolder + "/" + engineIdAndName + "/" + Constants.APP_ROOT_FOLDER + "/"
				+ Constants.VERSION_FOLDER;
	}

	/**
	 * 
	 * @param type
	 * @param engineIdAndName
	 * @return
	 */
	public static String getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE type, String engineIdAndName) {
		String baseEngineFolder = getLocalEngineBaseDirectory(type);
		return baseEngineFolder + "/" + engineIdAndName + "/" + Constants.APP_ROOT_FOLDER + "/"
				+ Constants.VERSION_FOLDER + "/" + Constants.ASSETS_FOLDER;
	}

	/**
	 * Returns the folder that holds every engine of a type, under the base folder.
	 *
	 * @param type the engine type
	 * @return the folder, such as {@code <base folder>/db} for databases
	 * @throws IllegalArgumentException when the type is null
	 * @throws IllegalStateException    when the RDF_Map has no BASE_FOLDER
	 */
	public static String getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE type) {
		if (type == null) {
			throw new IllegalArgumentException("Unhandled engine type = null");
		}
		String folder = switch (type) {
		case DATABASE -> Constants.DATABASE_FOLDER;
		case STORAGE -> Constants.STORAGE_FOLDER;
		case MODEL -> Constants.MODEL_FOLDER;
		case VECTOR -> Constants.VECTOR_FOLDER;
		case FUNCTION -> Constants.FUNCTION_FOLDER;
		case GUARDRAIL -> Constants.GUARDRAIL_FOLDER;
		case PROJECT -> Constants.PROJECT_FOLDER;
		case VENV -> Constants.VENV_FOLDER;
		};
		return baseFolder() + folder;
	}

	/**
	 * Returns the folder that holds the image of each engine of a type, under the
	 * base folder.
	 *
	 * @param type the engine type
	 * @return the folder, such as {@code <base folder>/images/databases} for
	 *         databases
	 * @throws IllegalArgumentException when the type is null
	 * @throws IllegalStateException    when the RDF_Map has no BASE_FOLDER
	 */
	public static String getLocalEngineImageDirectory(IEngine.CATALOG_TYPE type) {
		if (type == null) {
			throw new IllegalArgumentException("Unhandled engine type = null");
		}
		String folder = switch (type) {
		case DATABASE -> LOCAL_DATABASE_IMAGE_RELPATH;
		case STORAGE -> LOCAL_STORAGE_IMAGE_RELPATH;
		case MODEL -> LOCAL_MODEL_IMAGE_RELPATH;
		case VECTOR -> LOCAL_VECTOR_IMAGE_RELPATH;
		case FUNCTION -> LOCAL_FUNCTION_IMAGE_RELPATH;
		case GUARDRAIL -> LOCAL_GUARDRAIL_IMAGE_RELPATH;
		case PROJECT -> LOCAL_PROJECT_IMAGE_RELPATH;
		case VENV -> LOCAL_VENV_IMAGE_RELPATH;
		};
		return baseFolder() + folder;
	}

	/**
	 * Returns the folder that holds each user's asset folder, under the base
	 * folder.
	 *
	 * @return the folder, {@code <base folder>/user}
	 * @throws IllegalStateException when the RDF_Map has no BASE_FOLDER
	 */
	public static String getUserFolder() {
		return baseFolder() + Constants.USER_FOLDER;
	}

	/**
	 * Returns the folder that holds every room, under the base folder.
	 *
	 * @return the folder, {@code <base folder>/room}
	 * @throws IllegalStateException when the RDF_Map has no BASE_FOLDER
	 */
	public static String getRoomFolder() {
		return baseFolder() + Constants.ROOM_FOLDER;
	}

	/**
	 * Returns the base folder the RDF_Map names, with forward slashes and a
	 * trailing slash.
	 *
	 * @return the base folder
	 * @throws IllegalStateException when the RDF_Map has no BASE_FOLDER
	 */
	private static String baseFolder() {
		String baseFolder = Utility.getBaseFolder();
		if (baseFolder == null) {
			throw new IllegalStateException("BASE_FOLDER is not set in the RDF_Map");
		}
		return withTrailingSlash(baseFolder);
	}

	/**
	 * Returns a folder under the base folder set when this class loads, for the
	 * deprecated folder constants.
	 *
	 * @param relativePath the folder's path under the base folder
	 * @return the folder, or null when no base folder is set yet
	 */
	private static String folderAtLoad(String relativePath) {
		String baseFolder = Utility.getBaseFolder();
		if (baseFolder == null) {
			return null;
		}
		return withTrailingSlash(baseFolder) + relativePath;
	}

	/**
	 * Returns the folder path with forward slashes and a trailing slash.
	 *
	 * @param folder the folder path
	 * @return the normalized path
	 */
	private static String withTrailingSlash(String folder) {
		String normalized = folder.replace("\\", "/");
		return normalized.endsWith("/") ? normalized : normalized + "/";
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static String getCouchSelector(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return CouchUtil.DATABASE;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return CouchUtil.STORAGE;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return CouchUtil.MODEL;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return CouchUtil.VECTOR;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return CouchUtil.FUNCTION;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return CouchUtil.GUARDRAIL;
		} else if (IEngine.CATALOG_TYPE.PROJECT == type) {
			return CouchUtil.PROJECT;
		} else if (IEngine.CATALOG_TYPE.VENV == type) {
			return CouchUtil.VENV;
		}

		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}

	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static String getLocalEngineBaseDirectory(String engineId) {
		IEngine.CATALOG_TYPE catalogType = SecurityEngineUtils.getEngineType(engineId);
		return EngineUtility.getLocalEngineBaseDirectory(catalogType);
	}

}
