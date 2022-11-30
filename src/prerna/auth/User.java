package prerna.auth;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RRemoteRserve;
import prerna.om.CopyObject;
import prerna.project.api.IProject;
import prerna.sablecc2.reactor.mgmt.MgmtUtil;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MountHelper;
import prerna.util.SemossClassloader;
import prerna.util.Settings;
import prerna.util.Utility;

public class User implements Serializable {

	private static Logger logger = LogManager.getLogger(User.class);
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// store the users insights
	private transient Map<String, List<String>> openInsights = null;
	
	// need to have an access token store
	private transient IRUserConnection rcon; 
	private transient RRemoteRserve rconRemote;

	// python related stuff
	private transient SocketClient tcpServer;
	private transient PyTranslator pyt = null;
	private String port = null;
	public String pyTupleSpace = null;
	public String tupleSpace = null;
	private MountHelper mountHelper = null;
	private String mountTuple = null;

	// keeping this for a later time when personal experimental stuff
	private transient ClassLoader customLoader = null;
	
	private Map<AuthProvider, String> workspaceProjectMap = new HashMap<>();
	private Map<AuthProvider, String> assetProjectMap = new HashMap<>();
	private AuthProvider primaryLogin;
	
	private transient Object assetSyncObject = null;
	private transient Object workspaceSyncObject = null;

	Hashtable<AuthProvider, AccessToken> accessTokens = new Hashtable<>();
	List<AuthProvider> loggedInProfiles = new Vector<>();
	// keeps the secret for every insight
	Hashtable <String, InsightToken> insightSecret = new Hashtable <>();
	// shared sessions
	List<String> sharedSessions = new Vector<>();
	
	private Map<String, String> projectIdMap = new HashMap<>();
	private Map<String, String> engineIdMap = new HashMap<>();
	private Map<String, StringBuffer> varMap = new HashMap<>();
	private Map<String, IStorageEngine> externalMounts = new HashMap<String, IStorageEngine>();
	
	private boolean anonymous;
	private String anonymousId;
	
	public transient CopyObject cp = null;
	private transient CmdExecUtil cmdUtil = null;
	
	private transient Process rProcess = null;
	private transient Process pyProcess = null;
	
	private int rPort = -1;
	private int pyPort = -1;
	
	private int forcePort = -1;
	
	
	public User() {
		// transient objects should be defined in the constructor
		// since if this is serialized we dont want these values to be null
		this.customLoader = new SemossClassloader(this.getClass().getClassLoader());
		this.openInsights = new HashMap<>();
		this.assetSyncObject = new Object();
		this.workspaceSyncObject = new Object();
		// set it in the mgmt utils
		addUserMemory();
	}
	
	/**
	 * Set the access token for a given provider
	 * @param value
	 */
	public void setAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		if(!loggedInProfiles.contains(name)) {
			loggedInProfiles.add(name);
		}
		accessTokens.put(name, value);
		setAnonymous(false);
	}
	
	/**
	 * Set the access token for a given provider
	 * We do not register in the logged in profiles but can still grab
	 * from reactors that utilize them
	 * @param value
	 */
	public void setGlobalAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		accessTokens.put(name, value);
	}
	
	/**
	 * Get the requested access token
	 * @param name
	 * @return
	 */
	public AccessToken getAccessToken(AuthProvider name) {
		return accessTokens.get(name);
	}
	
	/**
	 * Drop the access token for a given provider
	 * @param name			The name of the provider
	 * @return				boolean if the provider was dropped
	 */
	public boolean dropAccessToken(String name) {
		// remove from token map
		AuthProvider tokenKey = AuthProvider.valueOf(name);
		AccessToken token = accessTokens.remove(tokenKey);
		// remove from profiles list
		loggedInProfiles.remove(tokenKey);

		// return false if the token actually wasn't found
		return token != null;
	}
	
	/**
	 * Drop the access token for a given provider
	 * @param tokenKey		The name of the provider
	 * @return				boolean if the provider was dropped
	 */
	public boolean dropAccessToken(AuthProvider tokenKey) {
		// remove from token map
		AccessToken token = accessTokens.remove(tokenKey);
		// remove from profiles list
		loggedInProfiles.remove(tokenKey);
		// return false if the token actually wasn't found
		if(token == null) {
			return false;
		}
		
		// recalculate the primary login
		if(this.primaryLogin == tokenKey && !this.loggedInProfiles.isEmpty()) {
			this.primaryLogin = this.loggedInProfiles.get(0);
		}
		return true;
	}
	
	/**
	 * Get the list of logged in profiles
	 * @return
	 */
	public List<AuthProvider> getLogins() {
		return loggedInProfiles;
	}

	public boolean isLoggedIn() {
		return !this.loggedInProfiles.isEmpty();
	}
	
	public void setAnonymous(boolean anonymous) {
		this.anonymous = anonymous;
	}
	
	public boolean isAnonymous() {
		return this.anonymous;
	}

	public void setAnonymousId(String anonymousId) {
		this.anonymousId = anonymousId;
	}
	
	public String getAnonymousId() {
		return this.anonymousId;
	}
	
	////////////////////////////////////////////////////////////////////////
	
	public AuthProvider getPrimaryLogin() {
		if(this.primaryLogin == null && isLoggedIn()) {
			this.primaryLogin = this.loggedInProfiles.get(0);
		}
		return this.primaryLogin;
	}
	
	public void setPrimaryLogin(AuthProvider primaryLogin) {
		this.primaryLogin = primaryLogin;
	}
	
	public String getWorkspaceProjectId(AuthProvider token) {
		if (this.workspaceProjectMap.get(token) != null) {
			return this.workspaceProjectMap.get(token);
		}

		String projectId = WorkspaceAssetUtils.getUserWorkspaceProject(this, token);
		
		if (projectId != null) {
			this.workspaceProjectMap.put(token, projectId);
		} else {
			try {
				synchronized(workspaceSyncObject) {
					projectId = WorkspaceAssetUtils.getUserWorkspaceProject(this, token);
					if(projectId == null) {
						projectId = WorkspaceAssetUtils.createUserWorkspaceProject(this, token);
					}
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}

			this.workspaceProjectMap.put(token, projectId);
		}

		//TODO actually sync the pull, not sure pull it
		if (ClusterUtil.IS_CLUSTER) {
			try {
				CloudClient.getClient().pullUserAssetOrWorkspace(projectId, false, false);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			} catch (InterruptedException ie) {
				// Restore interrupted state...
				Thread.currentThread().interrupt();
				logger.error(Constants.STACKTRACE, ie);
			}
		}
		
		return this.workspaceProjectMap.get(token);
	}
	
	public String getAssetProjectId(AuthProvider token) {
		if(this.assetProjectMap.get(token) != null) {
			return this.assetProjectMap.get(token);
		}
		String projectId = WorkspaceAssetUtils.getUserAssetProject(this, token);

		if (projectId != null) {
			this.assetProjectMap.put(token, projectId);
		} else {
			try {
				synchronized(assetSyncObject) {
					projectId = WorkspaceAssetUtils.getUserAssetProject(this, token);
					if(projectId == null) {
						projectId = WorkspaceAssetUtils.createUserAssetProject(this, token);
					}
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}

			this.assetProjectMap.put(token, projectId);
		}

		//TODO actually sync the pull, not sure pull it
		if (ClusterUtil.IS_CLUSTER) {
			try {
				CloudClient.getClient().pullUserAssetOrWorkspace(projectId, true, false);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			} catch (InterruptedException ie) {
				// Restore interrupted state...
				Thread.currentThread().interrupt();
				logger.error(Constants.STACKTRACE, ie);
			}
		}

		return this.assetProjectMap.get(token);
	}

	public Map<AuthProvider, String> getWorkspaceEngineMap() {
		return this.workspaceProjectMap;
	}
	
	public Map<AuthProvider, String> getAssetEngineMap() {
		return this.assetProjectMap;
	}
	
	////////////////////////////////////////////////////////////////////////

	public IRUserConnection getRcon() {
		return rcon;
	}

	public void setRcon(IRUserConnection rcon) {
		this.rcon = rcon;
	}	
	
	public RRemoteRserve getRconRemote() {
		return rconRemote; 
	}
	
	public void setRconRemote(RRemoteRserve rconRemote) {
		this.rconRemote = rconRemote;
	}
	
	// add the insight instance id
	public void addInsight(String id, InsightToken token)
	{
		insightSecret.put(id, token);
	}
	
	// get insight token
	public InsightToken getInsight(String id)
	{
		return insightSecret.get(id);
	}
	
	public void addShare(String sessionId)
	{
		if(!sharedSessions.contains(sessionId))
			sharedSessions.add(sessionId);
	}	
	
	public void removeShare(String sessionId)
	{
		sharedSessions.remove(sessionId);
	}
	
	public boolean isShareSession(String sessionId)
	{
		return sharedSessions.contains(sessionId);
	}
	
	public void ctrlC(String source, String showSource)
	{
		this.cp = new CopyObject();
		cp.source = source;
		cp.showSource = showSource;
	}
	
	public CopyObject getCtrlC()
	{
		return cp;
	}
	
	public void ctrlX(String source, String showSource)
	{
		this.cp = new CopyObject();
		cp.source = source;
		cp.showSource = showSource;
		cp.delete = true;
	}
	
	public void escapeCopy()
	{
		this.cp = null;
	}
	
	public void setTupleSpace(String tupleSpace)
	{
		this.tupleSpace = tupleSpace;
	}
	
	public String getTupleSpace()
	{
		return this.tupleSpace;
	}
	
	/**
	 * Store the open insight
	 * @param operation
	 * @param rdbmsId
	 * @param insightId
	 */
	public void addOpenInsight(String engineId, String rdbmsId, String insightId) {
		if(this.openInsights == null) {
			this.openInsights = new HashMap<>();
		}
		String id = getUid(engineId, rdbmsId);
		List<String> openInstances = null;
		if(this.openInsights.containsKey(id)) {
			openInstances = this.openInsights.get(id);
		} else {
			openInstances = new Vector<>();
			this.openInsights.put(id, openInstances);
		}
		openInstances.add(insightId);
	}
	
	/**
	 * Remove open insight
	 * @param engineId
	 * @param rdbmsId
	 * @param insightId
	 */
	public void removeOpenInsight(String engineId, String rdbmsId, String insightId) {
		if(this.openInsights == null) {
			return;
		}
		String id = getUid(engineId, rdbmsId);
		List<String> openInstances = null;
		if(this.openInsights.containsKey(id)) {
			openInstances = this.openInsights.get(id);
			openInstances.remove(insightId);
		}
	}
	
	/**
	 * Grab the open insight ids for a specific saved insight
	 * @param engineId
	 * @param rdbmsId
	 * @return
	 */
	public List<String> getOpenInsightInstances(String engineId, String rdbmsId) {
		String id = getUid(engineId, rdbmsId);
		return this.openInsights.get(id);
	}
	
	private String getUid(String engineId, String rdbmsId) {
		return engineId + "__" + rdbmsId;
	}
	
	/**
	 * Get the user open insights
	 * @return
	 */
	public Map<String, List<String>> getOpenInsights() {
		return openInsights;
	}
	
	/////////////////////////////////////////////////////
	
	/*
	 * Static utility methods
	 */
	
	public static Map<String, String> getLoginNames(User semossUser) {
		Map<String, String> retMap = new HashMap<>();
		if(semossUser == null) {
			return retMap;
		}
		if(semossUser.loggedInProfiles.isEmpty() && AbstractSecurityUtils.anonymousUsersEnabled() && semossUser.isAnonymous()) {
			retMap.put("ANONYMOUS", "Sign In");
		} else {
			for(AuthProvider p : semossUser.loggedInProfiles) {
				String name = semossUser.getAccessToken(p).getName();
				if(name == null) {
					// need to send something
					name = "";
				}
				retMap.put(p.toString().toUpperCase(), name);
			}
		}
		
		return retMap;
	}
	
	public static Map<String, Map<String, String>> getLoginDetails(User semossUser) {
		Map<String, Map<String, String>> retMap = new HashMap<>();
		if(semossUser == null) {
			return retMap;
		}
		if(semossUser.loggedInProfiles.isEmpty() && AbstractSecurityUtils.anonymousUsersEnabled() && semossUser.isAnonymous()) {
			Map<String, String> innerMap = new HashMap<>();
			innerMap.put("id", semossUser.getAnonymousId());
			innerMap.put("name", "Sign In");
			retMap.put("ANONYMOUS", innerMap);
		} else {
			for(AuthProvider p : semossUser.loggedInProfiles) {
				AccessToken token = semossUser.getAccessToken(p);
				String id = token.getId();
				String name = token.getName();
				if(name == null) {
					// need to send something
					name = "";
				}
				
				Map<String, String> innerMap = new HashMap<>();
				innerMap.put("id", id);
				innerMap.put("name", name);
				retMap.put(p.toString(), innerMap);
			}
		}
		
		return retMap;
	}
	
	public static String getSingleLogginName(User semossUser) {
		if(semossUser == null) {
			return "";
		}
		
		if(semossUser.loggedInProfiles.isEmpty() && AbstractSecurityUtils.anonymousUsersEnabled() && semossUser.isAnonymous()) {
			return "ANONYMOUS " + semossUser.anonymousId;
		}
		
		AccessToken token = semossUser.accessTokens.get(semossUser.getPrimaryLogin());
		return token.getId();
	}
	
	public static List<Pair<String, String>> getUserIdAndType(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User cannot be null.");
		}

		if (user.isAnonymous()) {
			throw new IllegalArgumentException("User cannot be anonymous.");
		}

		if (!AbstractSecurityUtils.securityEnabled()) {
			throw new IllegalArgumentException("Security must be enabled.");
		}

		List<Pair<String, String>> creds = new ArrayList<>();
		if (user.getLogins() != null) {
			for (AuthProvider login : user.getLogins()) {
				String userid = user.getAccessToken(login).getId();
				Pair<String, String> added = Pair.of(userid, login.name());
				creds.add(added);
			}
		}

		if (creds.size() == 0) {
			throw new IllegalArgumentException("User needs to be logged in.");
		}

		return creds;
	}

	public static List<Pair<String, String>> getPrimaryUserIdAndType(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User cannot be null.");
		}

		if (user.isAnonymous()) {
			throw new IllegalArgumentException("User cannot be anonymous.");
		}

		if (!AbstractSecurityUtils.securityEnabled()) {
			throw new IllegalArgumentException("Security must be enabled.");
		}

		List<Pair<String, String>> creds = new ArrayList<>();

		AuthProvider login = user.getPrimaryLogin();
		if (login == null) {
			throw new IllegalArgumentException("User must have primary login");
		}
		String userid = user.getAccessToken(login).getId();
		creds.add(Pair.of(userid, login.name()));

		if (creds.size() == 0) {
			throw new IllegalArgumentException("User needs to be logged in.");
		}

		return creds;
	}
	
	/////////////////////////////////////////////////////
	
	public void setTCPServer(SocketClient nc) {
		this.tcpServer = nc;
	}
	
	public SocketClient getTCPServer(boolean create) {
		if((this.tcpServer == null && create) || (this.tcpServer != null && !this.tcpServer.isConnected() && create)) {
			PyUtils.getInstance().userTupleMap.remove(this); // remove it from user tuple map so it will restart
			startTCPServer();
			this.pyt = new TCPPyTranslator();
			((TCPPyTranslator) pyt).nc = this.tcpServer; // starts it
			tcpServer.setUser(this);
		}
		return this.tcpServer;
	}

	public SocketClient getTCPServer(boolean create, int port) {
		// set the port
		forcePort = port;
		// then restart it
		if((this.tcpServer == null && create) || (this.tcpServer != null && !this.tcpServer.isConnected() && create)) {
			PyUtils.getInstance().userTupleMap.remove(this); // remove it from user tuple map so it will restart
			startTCPServer();
			this.pyt = new TCPPyTranslator();
			((TCPPyTranslator) pyt).nc = this.tcpServer; // starts it
			tcpServer.setUser(this);
		}
		return this.tcpServer;
	}

	/**
	 * Storing the engine id to engine name
	 * @param allEngines
	 */
	public void setEngines(List<Map<String, Object>> allEngines) {
		// I still need to check multiple engines with the same name. why not
		if(this.engineIdMap.size() == 0 || 
				(this.engineIdMap.size() != 0 && Integer.parseInt(this.engineIdMap.get("COUNT")) != allEngines.size()))
		{
			this.engineIdMap = new HashMap<>();
			// need to redo
			for(int engineIndex = 0;engineIndex < allEngines.size();engineIndex++) {
				Map <String, Object> engineValues = allEngines.get(engineIndex);
				String engineName = (String)engineValues.get("database_name");
				String engineId = (String)engineValues.get("database_id");
			
				this.engineIdMap.put(engineId, engineName);
			}
			this.engineIdMap.put("COUNT", allEngines.size() + "");
		}
	}

	/**
	 * Storing the project id to project name
	 * @param allProjects
	 */
	public void setProjects(List<Map<String, Object>> allProjects) {
		// I still need to check multiple engines with the same name. why not
		if(this.projectIdMap.size() == 0 || 
				(this.projectIdMap.size() != 0 && Integer.parseInt(this.projectIdMap.get("COUNT")) != allProjects.size()))
		{
			this.projectIdMap = new HashMap<>();
			// need to redo
			for(int projectIndex = 0;projectIndex < allProjects.size();projectIndex++) {
				Map <String, Object> projectValues = allProjects.get(projectIndex);
				String projectName = (String) projectValues.get("project_name");
				String projectId = (String) projectValues.get("project_id");
			
				this.projectIdMap.put(projectId, projectName);
			}
			this.projectIdMap.put("COUNT", allProjects.size() + "");
		}
	}
	
	/**
	 * Add a var using the project id - var name will be cleaned project name
	 * @param projectId
	 * @return	String - the clean project name used as the var name
	 */
	public String addVarMap(String projectId) {
		// initialize in case MyProjects has never been called
		if(this.projectIdMap == null || this.projectIdMap.isEmpty()) {
			setProjects(SecurityProjectUtils.getUserProjectList(this, false, null, null, null));
		}
		String projectName = this.projectIdMap.get(projectId);
		if(projectName == null) {
			return null;
		}
		projectName = Utility.makeAlphaNumeric(projectName);
		if(addVarMap(projectName, projectId, false)) {
			return projectName;
		}
		
		return null;
	}
	
	public boolean addVarMap(String varName, String projectId) {
		return addVarMap(varName, projectId, false);
	}

	/**
	 * 
	 * @param varName
	 * @param projectId
	 * @param override
	 * @return
	 */
	private boolean addVarMap(String varName, String projectId, boolean override) {
		//TODO: what was override supposed to do from an input standpoint???
		//TODO: what was override supposed to do from an input standpoint???
		//TODO: what was override supposed to do from an input standpoint???
		//TODO: what was override supposed to do from an input standpoint???
		
		/*
		 * Previous code would just break when override was set to true...
		 */

		String [] pathTokens = projectId.split("/");
		String subFolder = "";
		if(pathTokens.length > 1) {
			subFolder = projectId.replace(pathTokens[0],"");
			projectId = pathTokens[0];
		}
		
		String projectName = this.projectIdMap.get(projectId);
		if(projectName == null) {
			return false;
		}
		
		// giving it the main folder instead of the version
		String varValue = AssetUtility.getProjectBaseFolder(projectName, projectId) + subFolder;
		varValue = varValue.replace("\\", "/");

		// load in the project and pull the latest folder value
		IProject project = Utility.getProject(projectId);
		ClusterUtil.reactorPullProjectFolder(project, varValue);
		
		StringBuffer oldValue = varMap.get(varName);
		if(oldValue != null) {
			removeVarString(varName, oldValue);
		}
		
		this.varMap.put(varName, new StringBuffer(varValue));
		addVarString(varName);

		return true;
	}
	
	public void removeVarString(String varName, StringBuffer oldValue) {
		StringBuffer retRString = this.varMap.get("R_VAR_STRING");
		StringBuffer retPyString = this.varMap.get("PY_VAR_STRING");

		StringBuffer varValue = oldValue;
		if(oldValue == null) {
			varValue = this.varMap.get(varName);
		}
		
		if(retRString != null) {
			StringBuffer remRString = new StringBuffer("").append(varName).append(" <- '").append(varValue).append("';\n");
			String finalRString = retRString.toString();
			finalRString = finalRString.replace(remRString.toString(), "");
			retRString = new StringBuffer(finalRString);
			if(retRString.length() == 0) {
				varMap.remove("R_VAR_STRING");
			} else {
				varMap.put("R_VAR_STRING", retRString);			
			}
		}
		
		if(retPyString != null) {
			StringBuffer remPyString = new StringBuffer("").append(varName).append(" = '").append(varValue).append("'\n");
			String finalPyString = retPyString.toString();
			finalPyString = finalPyString.replace(remPyString.toString(), "");
			retPyString = new StringBuffer(finalPyString);
			if(retPyString.length() == 0) {
				varMap.remove("PY_VAR_STRING");
			} else {
				varMap.put("PY_VAR_STRING", retPyString);
			}
		}
	}
	
	private void addVarString(String varName) {
		StringBuffer retRString = varMap.get("R_VAR_STRING");
		StringBuffer retPyString = varMap.get("PY_VAR_STRING");
		
		if(retRString == null || retRString.length() == 0) {
			// first time
			retRString = new StringBuffer("");
		}
		if(retPyString == null || retPyString.length() == 0) {
			// first time
			retPyString = new StringBuffer("");
		}
		
		retRString.append(varName).append(" <- '").append(varMap.get(varName)).append("';\n");
		varMap.put("R_VAR_STRING", retRString);			
		retPyString.append(varName).append(" = '").append(varMap.get(varName)).append("'\n");
		varMap.put("PY_VAR_STRING", retPyString);			
	}
	
	private String getVarString(boolean r) {
		if(r) {
			return varMap.get("R_VAR_STRING").toString();
		} else {
			return varMap.get("PY_VAR_STRING").toString();
		}
	}
	
	/**
	 * Returns the paths for the asset folders
	 * @return
	 */
	public Map<String, StringBuffer> getVarMap() {
		return this.varMap;
	}
	
	public void addExternalMount(String name, IStorageEngine mountHelper)
	{
		// name is what is recorded
		externalMounts.put(name, mountHelper);
		// get the user asset folder
		AuthProvider provider = getPrimaryLogin();
		String appId = getAssetProjectId(provider);
		String appName = "Asset";
		String userAssetFolder = AssetUtility.getProjectAssetFolder(appName, appId);

		// if this folder does not exist create it
		File file = new File(userAssetFolder);
		if (!file.exists()) {
			file.mkdir();
		}
		
		// add this mount point to the asset folder
		File mountFile = new File(userAssetFolder + DIR_SEPARATOR + name);
		if(!mountFile.exists())
			mountFile.mkdir();

		addVarMap(name, appName + "__" + appId + "/" + name, true);
		
		// at some point I need to also set a watcher to ferret things back and forth
	}
	
	public Map<String, IStorageEngine> getExternalMounts() {
		return this.externalMounts;
	}

	public PyTranslator getPyTranslator() {
		return getPyTranslator(true);
	}

	public PyTranslator getPyTranslator(boolean create) {
		boolean useNettyPy = DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON) != null
				&& DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON).equalsIgnoreCase("true");
		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is set to false for this instance");
		}
		if(this.pyt == null && create) {
			// all of the logic should go here now ?
			synchronized(this) {
				if (AbstractSecurityUtils.securityEnabled() && PyUtils.pyEnabled()) {		
					if (!useNettyPy) {
						PyExecutorThread jepThread = null;
						if (jepThread == null) {
							jepThread = PyUtils.getInstance().getJep();
							this.pyt = new PyTranslator();
							this.pyt.setPy(jepThread);
							long logSleeper = 1;
							while(!jepThread.isReady())
							{
								try 
								{
									// wait for it to start
									//using this.wait because its recommended vs sleep
									this.wait(logSleeper*1000);
									//Thread.sleep(logSleeper*1000);
									logSleeper++;
								} catch (InterruptedException e) 
								{
									logger.error(Constants.STACKTRACE, e);
								}
							}
							logger.info("Jep Start is Complete");
						}
					}
					// check to see if the py translator needs to be set ?
					// check to see if the py translator needs to be set ?
					else {
						//this.pyt = new TCPPyTranslator();
						//((TCPPyTranslator) pyt).nc = 
						getTCPServer(true); // starts it
					}
				}
			}
		}
		else if(useNettyPy && (tcpServer != null && !tcpServer.isConnected()) )
		{
			// need to check if this is netty py
			//System.err.println("TCP Server has crashed !!");
			//this.pyt = null;
			//this.tcpServer = null;
			PyUtils.getInstance().killTempTupleSpace(this);
		}
		return this.pyt;
	}
	
	/**
	 * Check if the user has access to a database
	 * @param databaseName
	 * @param databaseId
	 * @return
	 */
	public boolean checkDatabaseAccess(String databaseName, String databaseId) {
		return this.engineIdMap.containsKey(databaseId) && 
				this.engineIdMap.get(databaseId).equalsIgnoreCase(databaseName);	
	}

	/**
	 * Set the context for the user based on the path defined in the varMap
	 * @param context
	 */
	public void setContext(String context) {
		
		boolean useNettyPy = DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON) != null
				&& DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON).equalsIgnoreCase("true");
		if(!useNettyPy) {
			//TODO this breaks the git terminal, but right now that is using payload struct only
			return;
		}
		// sets the context space for the user
		String mountDir = this.varMap.get(context) + "";
		// remove the last assets
		mountDir = mountDir.replace("/assets", "");
		
		// also set the cmd context right here
		this.cmdUtil = new CmdExecUtil(context, mountDir, getTCPServer(true));
	}
	
	public CmdExecUtil getCmdUtil() {
		return this.cmdUtil;
	}
	
	
	public MountHelper getUserMountHelper() {
		if(Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
			if(mountHelper == null) {		
				String uniqueUserName =getSingleLogginName(this)+ "-" + UUID.randomUUID().toString();
				String baseMountPath = DIHelper.getInstance().getProperty("CHROOT_DIR");
				mountTuple = baseMountPath + DIR_SEPARATOR + uniqueUserName;
				//unique user is just for testing so when i ls on R, I can see it is me and not someone else
				mountHelper = new MountHelper(mountTuple);
		} 
			return mountHelper;
		} else {
			throw new IllegalArgumentException("Mounting + Chroot is set to false for this instance");
		}
	}
	
	public void startTCPServer() {
		if (tcpServer == null || !tcpServer.isConnected())  // start only if it not already in progress
		{
			logger.info("Starting the TCP Server !! ");
			
			// first preference given to user
			if(forcePort > 0)
				port = forcePort +"";
			
			if(port == null)
				port = DIHelper.getInstance().getProperty(Settings.FORCE_PORT); // this means someone has
																			// started it for debug
			if (port == null) // port has not been forced
			{
				port = Utility.findOpenPort();
				if(Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
					//unique user is just for testing so when i ls on R, I can see it is me and not someone else
					mountHelper = getUserMountHelper();
					
					//maker.mountTarget(userLoginName);
					//pyTupleSpace = mountTuple;
					//pyTupleSpace = mountTuple + DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
					pyTupleSpace = PyUtils.getInstance().startTCPServe(this, mountTuple, DIHelper.getInstance().getProperty(Constants.BASE_FOLDER), port);
				} else {
					if(DIHelper.getInstance().getProperty("PY_TUPLE_SPACE")!=null && !DIHelper.getInstance().getProperty("PY_TUPLE_SPACE").isEmpty()) {
						pyTupleSpace=(DIHelper.getInstance().getProperty("PY_TUPLE_SPACE"));
					} 
					else {
						pyTupleSpace = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
					}
					pyTupleSpace = PyUtils.getInstance().startTCPServe(this, pyTupleSpace, port);
				}
			}
			
			// instrumenting the client class also now
			String pyClient = DIHelper.getInstance().getProperty(Settings.TCP_CLIENT);
			if(pyClient == null || (pyClient=pyClient.trim()).isEmpty()) {
				pyClient = "prerna.tcp.client.SocketClient";
			}
			try
			{
				SocketClient nc = (SocketClient)Class.forName(pyClient).newInstance();
				this.setTCPServer(nc);
	
				nc.connect("127.0.0.1", Integer.parseInt(port), false);
				
				//nc.run(); - you cannot do this because then the client goes into listener mode
				Thread t = new Thread(nc);
				t.start();
	
				while(!nc.isReady())
				{
					synchronized(nc)
					{
						try 
						{
							nc.wait();
							logger.info("Setting the netty client ");
						} catch (InterruptedException e) {
							logger.error(Constants.STACKTRACE, e);
						}								
					}
				}
				
				setPyPort(Integer.parseInt(port));
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}

	public Process getrProcess() {
		return rProcess;
	}

	public void setrProcess(Process rProcess) {
		this.rProcess = rProcess;
	}

	public Process getPyProcess() {
		return pyProcess;
	}

	public void setPyProcess(Process pyProcess) {
		this.pyProcess = pyProcess;
	}

	public int getrPort() {
		return rPort;
	}

	public void setrPort(int rPport) {
		this.rPort = rPport;
	}

	public int getPyPort() {
		return pyPort;
	}

	public void setPyPort(int pyPport) {
		this.pyPort = pyPport;
	}
	
	private void addUserMemory() {
		long memoryInGigs = 0;
		// check if the user has memory
		boolean checkMem = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Settings.CHECK_MEM) + "");
		if(checkMem) {
			long freeMem = MgmtUtil.getFreeMemory();
			String memProfileSettings = DIHelper.getInstance().getProperty(Settings.MEM_PROFILE_SETTINGS);
			if(memProfileSettings.equalsIgnoreCase(Settings.CONSTANT_MEM)) {
				String memLimitSettings = DIHelper.getInstance().getProperty(Settings.USER_MEM_LIMIT);
				memoryInGigs = Integer.parseInt(memLimitSettings);
			}
			
			MgmtUtil.addMemory4User(memoryInGigs);
		}
	}

	public void removeUserMemory() {
		long memoryInGigs = 0;
		// check if the user has memory
		boolean checkMem = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Settings.CHECK_MEM) + "");
		if(checkMem) {
			long freeMem = MgmtUtil.getFreeMemory();
			String memProfileSettings = DIHelper.getInstance().getProperty(Settings.MEM_PROFILE_SETTINGS);
			
			if(memProfileSettings.equalsIgnoreCase(Settings.CONSTANT_MEM)) {
				String memLimitSettings = DIHelper.getInstance().getProperty(Settings.USER_MEM_LIMIT);
				memoryInGigs = Integer.parseInt(memLimitSettings);
			}
			
			MgmtUtil.removeMemory4User(memoryInGigs);
		}
	}
	
	public String [] getUserCredential(AuthProvider prov) {
		// just need some specific one the user is using
		if(prov != null && accessTokens.containsKey(prov)) {
			String[] creds = getUserEmail(accessTokens.get(prov));
			if(creds[1] != null) {
				return creds;
			}
		}

		Enumeration <AuthProvider> accessKeys = accessTokens.keys();
		if(accessKeys.hasMoreElements()) {
			AuthProvider provider = accessKeys.nextElement();
			AccessToken tok = accessTokens.get(provider);
			String[] creds = getUserEmail(tok);
			if(creds[1] != null) {
				return creds;
			}
		}
		
		return new String[] {"anonymous", "anonymous@not_logged_in.com"};
	}
	
	private String[] getUserEmail(AccessToken token) {
		String [] userEmail = new String[2];
		userEmail[0] = token.getUsername();
		userEmail[1] = token.getEmail();
	
		return userEmail;
	}
	
}
