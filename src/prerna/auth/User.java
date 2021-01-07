package prerna.auth;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.py.FilePyTranslator;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RRemoteRserve;
import prerna.om.AbstractValueObject;
import prerna.om.CopyObject;
import prerna.pyserve.NettyClient;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SemossClassloader;
import prerna.util.Utility;

public class User extends AbstractValueObject implements Serializable {

	private static Logger logger = LogManager.getLogger(User.class);
	
	// name of this user in the SEMOSS system if there is one
	
	// need to have an access token store
	private IRUserConnection rcon; 
	private RRemoteRserve rconRemote;

	// python related stuff
	private NettyClient pyServe;
	private PyTranslator pyt = null;
	private String port = "undefined";
	public String pyTupleSpace = null;
	
	// keeping this for a later time when personal experimental stuff
	private ClassLoader customLoader = new SemossClassloader(this.getClass().getClassLoader());
	
	private Map<AuthProvider, String> workspaceEngineMap = new HashMap<>();
	private Map<AuthProvider, String> assetEngineMap = new HashMap<>();
	private AuthProvider primaryLogin;
	
	private transient Object assetSyncObject = new Object();
	private transient Object workspaceSyncObject = new Object();

	Hashtable<AuthProvider, AccessToken> accessTokens = new Hashtable<>();
	List<AuthProvider> loggedInProfiles = new ArrayList<>();
	// keeps the secret for every insight
	Hashtable <String, InsightToken> insightSecret = new Hashtable <>();
	// shared sessions
	List<String> sharedSessions = new Vector<>();
	
	Map <String, String> engineIdMap = new HashMap<>();
	
	Map <String, StringBuffer> appMap = new HashMap<>();

	Map <String, IStorageEngine> externalMounts = new HashMap<String, IStorageEngine>();

	// gets the tuplespace
	String tupleSpace = null;
	
	private boolean anonymous;
	private String anonymousId;
	
	CopyObject cp = null;

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private CmdExecUtil cmdUtil = null;

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
	
	public String getWorkspaceEngineId(AuthProvider token) {
		if (this.workspaceEngineMap.get(token) != null) {
			return this.workspaceEngineMap.get(token);
		}

		String engineId = WorkspaceAssetUtils.getUserWorkspaceApp(this, token);
		
		if (engineId != null) {
			this.workspaceEngineMap.put(token, engineId);
		} else {
			try {
				synchronized(workspaceSyncObject) {
					engineId = WorkspaceAssetUtils.getUserWorkspaceApp(this, token);
					if(engineId == null) {
						engineId = WorkspaceAssetUtils.createUserWorkspaceApp(this, token);
					}
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}

			this.workspaceEngineMap.put(token, engineId);
		}

		return this.workspaceEngineMap.get(token);
	}
	
	public String getAssetEngineId(AuthProvider token) {
		if(this.assetEngineMap.get(token) != null) {
			return this.assetEngineMap.get(token);
		}
		String engineId = WorkspaceAssetUtils.getUserAssetApp(this, token);

		if (engineId != null) {
			this.assetEngineMap.put(token, engineId);
		} else {
			try {
				synchronized(assetSyncObject) {
					engineId = WorkspaceAssetUtils.getUserAssetApp(this, token);
					if(engineId == null) {
						engineId = WorkspaceAssetUtils.createUserAssetApp(this, token);
					}
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}

			this.assetEngineMap.put(token, engineId);
		}

		//TODO actually sync the pull, not sure pull it
		if (ClusterUtil.IS_CLUSTER) {
			try {
				CloudClient.getClient().pullApp(engineId);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			} catch (InterruptedException ie) {
				// Restore interrupted state...
				Thread.currentThread().interrupt();
				logger.error(Constants.STACKTRACE, ie);
			}
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
			engineIdMap = new HashMap<>();
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
		return addVarMap(varName, appName, false);
	}

	private boolean addVarMap(String varName, String appName, boolean override)
	{
		// convert the app name to alpha
		String [] pathTokens = appName.split("/");
		String subFolder = "";
		if(pathTokens.length > 1)
		{
			subFolder = appName.replace(pathTokens[0],"");
			appName = pathTokens[0];
		}
		
		String semossAppName = appName;

		//String semossAppName = Utility.makeAlphaNumeric(appName);

		String engineId = semossAppName;
		if(!override)
		{
			engineId = engineIdMap.get(semossAppName);
			appName = appName + "__" + engineId;
		}
		else
			engineId = "";
		if(engineId == null) // there is a possibility that this is a mount path at this point
			return false;
		String varValue = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + appName + "/version/assets" + subFolder;


		varValue = varValue.replace("\\", "/");

		StringBuffer oldValue = appMap.get(varName);
		
		appMap.put(varName, new StringBuffer(varValue));
		
		IEngine engine = Utility.getEngine(engineId);
		ClusterUtil.reactorPullFolder(engine, varValue);
		
		
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
	
	
	private void addVarString(String varName, StringBuffer oldValue)
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
	
	private String getVarString(boolean r)
	{
		if(r)
			return appMap.get("R_VAR_STRING").toString();
		else
			return appMap.get("PY_VAR_STRING").toString();
			
	}
	
	public Map<String, StringBuffer> getAppMap()
	{
		return this.appMap;
	}
	
	public void addExternalMount(String name, IStorageEngine mountHelper)
	{
		// name is what is recorded
		externalMounts.put(name, mountHelper);
		// get the user asset folder
		AuthProvider provider = getPrimaryLogin();
		String appId = getAssetEngineId(provider);
		String appName = "Asset";
		String userAssetFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "version";
				//+ DIR_SEPARATOR + "assets";

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
	
	public Map<String, IStorageEngine> getExternalMounts()
	{
		return this.externalMounts;
	}

	public PyTranslator getPyTranslator()
	{
		return getPyTranslator(true);
	}

	public PyTranslator getPyTranslator(boolean create)
	{
		if(this.pyt == null && create)
		{
			// all of the logic should go here now ?
			synchronized(this)
			{
				if (AbstractSecurityUtils.securityEnabled() && PyUtils.pyEnabled()) 
				{
		
					boolean useFilePy = DIHelper.getInstance().getProperty("USE_PY_FILE") != null
							&& DIHelper.getInstance().getProperty("USE_PY_FILE").equalsIgnoreCase("true");
					boolean useTCP = DIHelper.getInstance().getProperty("USE_TCP_PY") != null
							&& DIHelper.getInstance().getProperty("USE_TCP_PY").equalsIgnoreCase("true");
					useTCP = useFilePy; // forcing it to be same as TCP
		
					if (!useFilePy) {
						PyExecutorThread jepThread = null;
						if (jepThread == null) {
							jepThread = PyUtils.getInstance().getJep();
							pyt = new PyTranslator();
							pyt.setPy(jepThread);
							int logSleeper = 1;
							while(!jepThread.isReady())
							{
								try {
									// wait for it to start
									Thread.sleep(logSleeper*1000);
									logSleeper++;
								} catch (InterruptedException e) {
									logger.error(Constants.STACKTRACE, e);
								}
							}
							logger.info("Jep Start is Complete");
						}
					}
					// check to see if the py translator needs to be set ?
					// check to see if the py translator needs to be set ?
					else if (useFilePy) {
						if (useTCP) 
						{
							port = DIHelper.getInstance().getProperty("FORCE_PORT"); // this means someone has
																							// started it for debug
							if (port == null) // port has not been forced
							{
									port = Utility.findOpenPort();
								if(DIHelper.getInstance().getProperty("PY_TUPLE_SPACE")!=null && !DIHelper.getInstance().getProperty("PY_TUPLE_SPACE").isEmpty()) {
									pyTupleSpace=(DIHelper.getInstance().getProperty("PY_TUPLE_SPACE"));
								} else {
								pyTupleSpace = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
								}
								pyTupleSpace = PyUtils.getInstance().startPyServe(this, pyTupleSpace, port);
							}
							
							NettyClient nc = new NettyClient();
							nc.connect("127.0.0.1", Integer.parseInt(port), false);
							
							//nc.run(); - you cannot do this because then the client goes into listener mode
							Thread t = new Thread(nc);
							t.start();
							this.setPyServe(nc);
							pyt = new TCPPyTranslator();
							((TCPPyTranslator) pyt).nc = nc;
						} else {
							PyUtils.getInstance().getTempTupleSpace(this, DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR));
							pyt = new FilePyTranslator();
						}
					}
				}
			}
		}
		return this.pyt;
	}
	
	// need something that just connects a netty client and gives back
	
	
	
	public boolean checkAppAccess(String appName, String appId)
	{
		return (engineIdMap.containsKey(appName) && engineIdMap.get(appName).equalsIgnoreCase(appId));	
	}
	
	public void setContext(String context)
	{
		// sets the context space for the user
		// also set rhe cmd context right here
		String mountDir = appMap.get(context) + "";

		// remove the last assets
		mountDir = mountDir.replace("/assets", "");
		
		this.cmdUtil = new CmdExecUtil(context, mountDir);
	}
	
	public CmdExecUtil getCmdUtil()
	{
		return this.cmdUtil;
	}
	
}
