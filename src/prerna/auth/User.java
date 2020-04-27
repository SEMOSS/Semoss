package prerna.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import oracle.net.aso.a;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RRemoteRserve;
import prerna.om.AbstractValueObject;
import prerna.om.CopyObject;
import prerna.pyserve.NettyClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SemossClassloader;
import prerna.util.Utility;

public class User extends AbstractValueObject {
	
	// name of this user in the SEMOSS system if there is one
	
	// need to have an access token store
	private IRUserConnection rcon; 
	private RRemoteRserve rconRemote;
	private NettyClient pyServe;
	
	// keeping this for a later time when personal experimental stuff
	private ClassLoader customLoader = new SemossClassloader(this.getClass().getClassLoader());
	
	private Map<AuthProvider, String> workspaceEngineMap = new HashMap<AuthProvider, String>();
	private Map<AuthProvider, String> assetEngineMap = new HashMap<AuthProvider, String>();
	private AuthProvider primaryLogin;
	
	private transient Object assetSyncObject = new Object();
	private transient Object workspaceSyncObject = new Object();

	Hashtable<AuthProvider, AccessToken> accessTokens = new Hashtable<AuthProvider, AccessToken>();
	List<AuthProvider> loggedInProfiles = new ArrayList<AuthProvider>();
	// keeps the secret for every insight
	Hashtable <String, InsightToken> insightSecret = new Hashtable <String, InsightToken>();
	// shared sessions
	List<String> sharedSessions = new Vector<String>();
	
	Map <String, String> engineIdMap = new HashMap<String, String>();
	
	Map <String, StringBuffer> appMap = new HashMap<String, StringBuffer>();
	
	// gets the tuplespace
	String tupleSpace = null;
	
	private boolean anonymous;
	private String anonymousId;
	
	CopyObject cp = null;
	
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
		if(token == null) {
			return false;
		}
		return true;
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
	
	public String getWorkspaceEngineId(AuthProvider token) {
		if(this.workspaceEngineMap.get(token) == null) {
			String engineId = WorkspaceAssetUtils.getUserWorkspaceApp(this, token);
			if (engineId == null) {
				try {
					synchronized(workspaceSyncObject) {
						engineId = WorkspaceAssetUtils.getUserWorkspaceApp(this, token);
						if(engineId == null) {
							engineId = WorkspaceAssetUtils.createUserWorkspaceApp(this, token);
						}
					}
				} catch (Exception e) {
					// TODO >>>timb: WORKSPACE - How to deal with this exception properly?
					e.printStackTrace();
				}
			}
			this.workspaceEngineMap.put(token, engineId);
		}
		return this.workspaceEngineMap.get(token);
	}
	
	public String getAssetEngineId(AuthProvider token) {
		if(this.assetEngineMap.get(token) == null) {
			String engineId = WorkspaceAssetUtils.getUserAssetApp(this, token);
			if (engineId == null) {
				try {
					synchronized(assetSyncObject) {
						engineId = WorkspaceAssetUtils.getUserAssetApp(this, token);
						if(engineId == null) {
							engineId = WorkspaceAssetUtils.createUserAssetApp(this, token);
						}
					}
				} catch (Exception e) {
					// TODO >>>timb: WORKSPACE - How to deal with this exception properly?
					e.printStackTrace();
				}
			}
			//TODO actually sync the pull, not sure pull it
			if (ClusterUtil.IS_CLUSTER) {
				try {
					CloudClient.getClient().pullApp(engineId);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			this.assetEngineMap.put(token, engineId);
		}
		return this.assetEngineMap.get(token);
	}

	public Map<AuthProvider, String> getWorkspaceEngineMap() {
		return this.workspaceEngineMap;
	}
	
	public Map<AuthProvider, String> getAssetEngineMap() {
		return this.assetEngineMap;
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
	
	/////////////////////////////////////////////////////
	
	/*
	 * Static utility methods
	 */
	
	public static Map<String, String> getLoginNames(User semossUser) {
		Map<String, String> retMap = new HashMap<String, String>();
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
	
	public static String getSingleLogginName(User semossUser) {
		if(semossUser.loggedInProfiles.isEmpty() && AbstractSecurityUtils.anonymousUsersEnabled() && semossUser.isAnonymous()) {
			return "ANONYMOUS " + semossUser.anonymousId;
		}
		
		AccessToken token = semossUser.accessTokens.get(semossUser.getPrimaryLogin());
		return token.getId();
	}
	
	public void setPyServe(NettyClient nc)
	{
		this.pyServe = nc;
	}
	
	public NettyClient getPyServe()
	{
		return this.pyServe;
	}
	
	public void setEngines(List<Map<String, Object>> allEngines)
	{
		// I still need to check multiple engines with the same name. why not
		if(engineIdMap.size() == 0 || (engineIdMap.size() != 0 && Integer.parseInt(engineIdMap.get("COUNT")) != allEngines.size()))
		{
			engineIdMap = new HashMap<String, String>();
			// need to redo
			for(int engineIndex = 0;engineIndex < allEngines.size();engineIndex++)
			{
				Map <String, Object> engineValues = allEngines.get(engineIndex);
				String engineName = (String)engineValues.get("app_name");
				String engineId = (String)engineValues.get("app_id");
				
			
				engineIdMap.put(engineName, engineId);
			}
			engineIdMap.put("COUNT", allEngines.size() + "");
		}
		
		
	}
	
	public boolean addVarMap(String varName, String appName)
	{
		// convert the app name to alpha
		String semossAppName = Utility.makeAlphaNumeric(appName);

		String engineId = engineIdMap.get(semossAppName);
		if(engineId == null)
			return false;
		
		String varValue = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + appName + "__" + engineId + "/version/assets";
		
		varValue = varValue.replace("\\", "/");
		
		StringBuffer oldValue = appMap.get(varName);
		
		appMap.put(varName, new StringBuffer(varValue));
		
		if(oldValue != null)
			removeVarString(varName, oldValue);
		addVarString(varName, oldValue);
		
		return true;
	}
	
	public void removeVarString(String varName, StringBuffer oldValue)
	{
		StringBuffer retRString = appMap.get("R_VAR_STRING");
		StringBuffer retPyString = appMap.get("PY_VAR_STRING");

		StringBuffer varValue = oldValue;
		if(oldValue == null)
			varValue = appMap.get(varName);
		if(retRString != null)
		{
			StringBuffer remRString = new StringBuffer("").append(varName).append(" <- '").append(varValue).append("';\n");
			String finalRString = retRString.toString();
			finalRString = finalRString.replace(remRString.toString(), "");
			retRString = new StringBuffer(finalRString);
			if(retRString.length() == 0)
				appMap.remove("R_VAR_STRING");
			else
				appMap.put("R_VAR_STRING", retRString);			
		}
		
		if(retPyString != null)
		{
			StringBuffer remPyString = new StringBuffer("").append(varName).append(" = '").append(varValue).append("'\n");
			String finalPyString = retPyString.toString();
			finalPyString = finalPyString.replace(remPyString.toString(), "");
			retPyString = new StringBuffer(finalPyString);
			if(retPyString.length() == 0)
				appMap.remove("PY_VAR_STRING");
			else
				appMap.put("PY_VAR_STRING", retPyString);			
		}
	}
	
	
	public void addVarString(String varName, StringBuffer oldValue)
	{
		StringBuffer retRString = appMap.get("R_VAR_STRING");
		StringBuffer retPyString = appMap.get("PY_VAR_STRING");
		
		if(retRString == null)
			// first time
			retRString = new StringBuffer("");

		retRString.append(varName).append(" <- '").append(appMap.get(varName)).append("';\n");
		appMap.put("R_VAR_STRING", retRString);			
		

		if(retPyString == null || retPyString.length() == 0)
			// first time
			retPyString = new StringBuffer("");

		retPyString.append(varName).append(" = '").append(appMap.get(varName)).append("'\n");
		appMap.put("PY_VAR_STRING", retPyString);			
	}
	
	public String getVarString(boolean r)
	{
		if(r)
			return appMap.get("R_VAR_STRING").toString();
		else
			return appMap.get("PY_VAR_STRING").toString();
			
	}
	
	public Map getAppMap()
	{
		return this.appMap;
	}
	
	
}
