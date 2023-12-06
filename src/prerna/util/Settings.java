package prerna.util;

public class Settings {
	
	public static final String CHECK_MEM = "CHECK_MEM";
	public static final String MEM_PROFILE_SETTINGS = "MEM_PROFILE_SETTINGS";
	public static final String CONSTANT_MEM = "CONSTANT";
	public static final String USER_MEM_LIMIT = "USER_MEM_LIMIT";
	public static final String SMART_SYNC = "SMART_SYNC";
	public static final String RESERVED_JAVA_MEM = "RESERVED_JAVA_MEM";
	public static final String PUBLIC_HOME = "PUBLIC_HOME";
	public static final String PUBLIC_HOME_ENABLE = "PUBLIC_HOME_ENABLE";
	public static final String PORTAL_NAME = "PORTAL_NAME";
	public static final String COPY_PROJECT = "COPY_PROJECT";
	public static final String JAVA_HOME = "JAVA_HOME";
	public static final String MVN_HOME = "MVN_HOME";
	public static final String REPO_HOME = "REPO_HOME";
	public static final String BLOCKING = "BLOCKING";
	public static final String TCP_CLIENT = "TCP_CLIENT";
	public static final String HF_CACHE_DIR = "HF_CACHE_DIR"; // hugging face cache folder	
	public static final String LOAD_DB_ON_SOCKET = "LOAD_DB_ON_SOCKET"; // load the db again on the socket side or give it as engine wrapper	
	public static final String CUSTOM_REACTOR_EXECUTION = "CUSTOM_REACTOR_EXECUTION";
	@Deprecated
	public static final String PY_HOME = "PY_HOME";
	public static final String PYTHONHOME = "PYTHONHOME";
	public static final String PYTHONHOME_SITE_PACKAGES = "PYTHONHOME_SITE_PACKAGES";
	public static final String NATIVE_PY_SERVER = "NATIVE_PY_SERVER";
	
	public static final String PROMPT_STOPPER = "PROMPT_STOPPER";
	public static final String VAR_NAME = "VAR_NAME";
	public static final String TIMEOUT = "TIMEOUT";
	// input usually a list to functions
	public static final String INPUT = "INPUT";
	
	public static final String COUNT = "COUNT";
	public static final String REQUIREMENTS = "REQUIREMENTS"; // this is a JSON of all the requirements for a SMSS engine like GPU requirement etc. etc. 

	// debugging
	public static final String FORCE_PORT = "FORCE_PORT"; // the port to force the connection on

}

