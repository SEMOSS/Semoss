package prerna.util;

import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.couch.CouchUtil;

public class EngineUtility {

	private static final String BASE_FOLDER;
	static {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		baseFolder = baseFolder.replace("\\", "/");
		if(!baseFolder.endsWith("/")) {
			baseFolder += "/";
		}
		BASE_FOLDER = baseFolder;
	};
	
	public static final String LOCAL_DATABASE_IMAGE_RELPATH = "images/databases";
	public static final String LOCAL_STORAGE_IMAGE_RELPATH = "images/storages";
	public static final String LOCAL_MODEL_IMAGE_RELPATH = "images/models";
	public static final String LOCAL_VECTOR_IMAGE_RELPATH = "images/vectors";
	public static final String LOCAL_SERVICE_IMAGE_RELPATH = "images/services";
	public static final String LOCAL_PROJECT_IMAGE_RELPATH = "images/projects";

	public static final String DATABASE_FOLDER = BASE_FOLDER + Constants.DATABASE_FOLDER;
	public static final String STORAGE_FOLDER = BASE_FOLDER + Constants.STORAGE_FOLDER;
	public static final String MODEL_FOLDER = BASE_FOLDER + Constants.MODEL_FOLDER;
	public static final String VECTOR_FOLDER = BASE_FOLDER + Constants.VECTOR_FOLDER;
	public static final String SERVICE_FOLDER = BASE_FOLDER + Constants.SERVICE_FOLDER;
	// project is special engine
	public static final String PROJECT_FOLDER = BASE_FOLDER + Constants.PROJECT_FOLDER;
	public static final String USER_FOLDER = BASE_FOLDER + Constants.USER_FOLDER;
	
	public static final String DATABASE_IMAGE_FOLDER = BASE_FOLDER + LOCAL_DATABASE_IMAGE_RELPATH;
	public static final String STORAGE_IMAGE_FOLDER = BASE_FOLDER + LOCAL_STORAGE_IMAGE_RELPATH;
	public static final String MODEL_IMAGE_FOLDER = BASE_FOLDER + LOCAL_MODEL_IMAGE_RELPATH;
	public static final String VECTOR_IMAGE_FOLDER = BASE_FOLDER + LOCAL_VECTOR_IMAGE_RELPATH;
	public static final String SERVICE_IMAGE_FOLDER = BASE_FOLDER + LOCAL_SERVICE_IMAGE_RELPATH;
	public static final String PROJECT_IMAGE_FOLDER = BASE_FOLDER + LOCAL_PROJECT_IMAGE_RELPATH;
	
	/**
	 * 
	 * @param type
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	public static String getEngineVersionFolder(IEngine.CATALOG_TYPE type, String engineId, String engineName) {
		return getEngineVersionFolder(type, SmssUtilities.getUniqueName(engineName, engineId));
	}
	
	/**
	 * 
	 * @param type
	 * @param engineIdAndName
	 * @return
	 */
	public static String getEngineVersionFolder(IEngine.CATALOG_TYPE type, String engineIdAndName) {
		String baseEngineFolder = getLocalEngineBaseDirectory(type);
		return baseEngineFolder + engineIdAndName + "/" + Constants.APP_ROOT_FOLDER + "/" + Constants.VERSION_FOLDER;
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	public static String getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE type) {
		if(IEngine.CATALOG_TYPE.DATABASE == type) {
			return DATABASE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.STORAGE == type) {
			return STORAGE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.MODEL == type) {
			return MODEL_FOLDER;
		} else if(IEngine.CATALOG_TYPE.VECTOR == type) {
			return VECTOR_FOLDER;
		} else if(IEngine.CATALOG_TYPE.SERVICE == type) {
			return SERVICE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.PROJECT == type) {
			return PROJECT_FOLDER;
		}
		
		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	public static String getLocalEngineImageDirectory(IEngine.CATALOG_TYPE type) {
		if(IEngine.CATALOG_TYPE.DATABASE == type) {
			return DATABASE_IMAGE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.STORAGE == type) {
			return STORAGE_IMAGE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.MODEL == type) {
			return MODEL_IMAGE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.VECTOR == type) {
			return VECTOR_IMAGE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.SERVICE == type) {
			return SERVICE_IMAGE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.PROJECT == type) {
			return PROJECT_IMAGE_FOLDER;
		}
		
		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	public static String getCouchSelector(IEngine.CATALOG_TYPE type) {
		if(IEngine.CATALOG_TYPE.DATABASE == type) {
			return CouchUtil.DATABASE;
		} else if(IEngine.CATALOG_TYPE.STORAGE == type) {
			return CouchUtil.STORAGE;
		} else if(IEngine.CATALOG_TYPE.MODEL == type) {
			return CouchUtil.MODEL;
		} else if(IEngine.CATALOG_TYPE.VECTOR == type) {
			return CouchUtil.VECTOR;
		} else if(IEngine.CATALOG_TYPE.SERVICE == type) {
			return CouchUtil.SERVICE;
		} else if(IEngine.CATALOG_TYPE.PROJECT == type) {
			return CouchUtil.PROJECT;
		}
		
		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}
	


	
}