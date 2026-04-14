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

public class EngineUtility {

	private static final String BASE_FOLDER;
	static {
		String baseFolder = Utility.getBaseFolder();
		baseFolder = baseFolder.replace("\\", "/");
		if (!baseFolder.endsWith("/")) {
			baseFolder += "/";
		}
		BASE_FOLDER = baseFolder;
	};

	public static final String LOCAL_DATABASE_IMAGE_RELPATH = "images/databases";
	public static final String LOCAL_STORAGE_IMAGE_RELPATH = "images/storages";
	public static final String LOCAL_MODEL_IMAGE_RELPATH = "images/models";
	public static final String LOCAL_VECTOR_IMAGE_RELPATH = "images/vectors";
	public static final String LOCAL_FUNCTION_IMAGE_RELPATH = "images/functions";
	public static final String LOCAL_GUARDRAIL_IMAGE_RELPATH = "images/guardrail";
	public static final String LOCAL_PROJECT_IMAGE_RELPATH = "images/projects";
	public static final String LOCAL_VENV_IMAGE_RELPATH = "images/venv";

	public static final String DATABASE_FOLDER = BASE_FOLDER + Constants.DATABASE_FOLDER;
	public static final String GUARDRAIL_FOLDER = BASE_FOLDER + Constants.GUARDRAIL_FOLDER;
	public static final String FUNCTION_FOLDER = BASE_FOLDER + Constants.FUNCTION_FOLDER;
	public static final String MODEL_FOLDER = BASE_FOLDER + Constants.MODEL_FOLDER;
	public static final String ROOM_FOLDER = BASE_FOLDER + Constants.ROOM_FOLDER;
	public static final String STORAGE_FOLDER = BASE_FOLDER + Constants.STORAGE_FOLDER;
	public static final String VECTOR_FOLDER = BASE_FOLDER + Constants.VECTOR_FOLDER;
	public static final String VENV_FOLDER = BASE_FOLDER + Constants.VENV_FOLDER;

	// project is special engine
	public static final String PROJECT_FOLDER = BASE_FOLDER + Constants.PROJECT_FOLDER;
	public static final String USER_FOLDER = BASE_FOLDER + Constants.USER_FOLDER;

	public static final String DATABASE_IMAGE_FOLDER = BASE_FOLDER + LOCAL_DATABASE_IMAGE_RELPATH;
	public static final String STORAGE_IMAGE_FOLDER = BASE_FOLDER + LOCAL_STORAGE_IMAGE_RELPATH;
	public static final String MODEL_IMAGE_FOLDER = BASE_FOLDER + LOCAL_MODEL_IMAGE_RELPATH;
	public static final String VECTOR_IMAGE_FOLDER = BASE_FOLDER + LOCAL_VECTOR_IMAGE_RELPATH;
	public static final String FUNCTION_IMAGE_FOLDER = BASE_FOLDER + LOCAL_FUNCTION_IMAGE_RELPATH;
	public static final String GUARDRAIL_IMAGE_FOLDER = BASE_FOLDER + LOCAL_GUARDRAIL_IMAGE_RELPATH;
	public static final String PROJECT_IMAGE_FOLDER = BASE_FOLDER + LOCAL_PROJECT_IMAGE_RELPATH;
	public static final String VENV_IMAGE_FOLDER = BASE_FOLDER + LOCAL_PROJECT_IMAGE_RELPATH;

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
	 * 
	 * @param type
	 * @return
	 */
	public static String getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return DATABASE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return STORAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return MODEL_FOLDER;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return VECTOR_FOLDER;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return FUNCTION_FOLDER;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return GUARDRAIL_FOLDER;
		} else if (IEngine.CATALOG_TYPE.PROJECT == type) {
			return PROJECT_FOLDER;
		} else if (IEngine.CATALOG_TYPE.VENV == type) {
			return VENV_FOLDER;
		}

		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static String getLocalEngineImageDirectory(IEngine.CATALOG_TYPE type) {
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return DATABASE_IMAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return STORAGE_IMAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return MODEL_IMAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return VECTOR_IMAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return FUNCTION_IMAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return GUARDRAIL_IMAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.PROJECT == type) {
			return PROJECT_IMAGE_FOLDER;
		} else if (IEngine.CATALOG_TYPE.VENV == type) {
			return VENV_IMAGE_FOLDER;
		}

		throw new IllegalArgumentException("Unhandled engine type = " + type);
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
