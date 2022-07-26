package prerna.project.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;
import org.xeustechnologies.jcl.JarClassLoader;

import com.google.gson.Gson;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.ProjectHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.legacy.playsheets.LegacyInsightDatabaseUtility;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SemossClassloader;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.git.GitPushUtils;

public class Project implements IProject {

	private static final Logger logger = LogManager.getLogger(Project.class);
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static final String QUESTION_PARAM_KEY = "@QUESTION_VALUE@";
	private static final String GET_ALL_INSIGHTS_QUERY = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID ORDER BY ID";
	private static final String GET_ALL_PERSPECTIVES_QUERY = "SELECT DISTINCT QUESTION_PERSPECTIVE FROM QUESTION_ID ORDER BY QUESTION_PERSPECTIVE";
	private static final String GET_INSIGHT_INFO_QUERY = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, QUESTION_LAYOUT, "
			+ "QUESTION_ORDER, DATA_TABLE_ALIGN, QUESTION_DATA_MAKER, CACHEABLE, CACHE_MINUTES, CACHE_CRON, CACHE_ENCRYPT, "
			+ "QUESTION_PKQL FROM QUESTION_ID WHERE ID IN (" + QUESTION_PARAM_KEY + ") ORDER BY QUESTION_ORDER";

	private String projectId;
	private String projectName;
	private String projectGitProvider;
	private String projectGitRepo;
	private AuthProvider gitProvider;
	
	private Properties prop = null;
	private String projectSmssFilePath = null;
	private boolean isAsset = false;
	private ProjectProperties projectProperties = null;
	
	private RDBMSNativeEngine insightRdbms;
	private String insightDatabaseLoc;

	/**
	 * Hash for the specific engine reactors
	 */
	private Map <String, Class> projectSpecificHash = null;
	
	/**
	 * Custom class loader
	 */
	private SemossClassloader engineClassLoader = new SemossClassloader(this.getClass().getClassLoader());
	private JarClassLoader mvnClassLoader = null;
	
	// publish cache
	private boolean publish = false;
	
	@Override
	public void openProject(String projectSmssFilePath) {
		this.projectSmssFilePath = projectSmssFilePath;
		this.prop = Utility.loadProperties(projectSmssFilePath);
		this.projectId = prop.getProperty(Constants.PROJECT);
		this.projectName = prop.getProperty(Constants.PROJECT_ALIAS);
		
		if(prop.containsKey(Constants.PROJECT_GIT_PROVIDER) && prop.containsKey(Constants.PROJECT_GIT_CLONE)) {
			this.projectGitProvider = prop.getProperty(Constants.PROJECT_GIT_PROVIDER);
			this.projectGitRepo = prop.getProperty(Constants.PROJECT_GIT_CLONE);
			this.gitProvider = AuthProvider.getProviderFromString(projectGitProvider);

			String versionDir = AssetUtility.getProjectVersionFolder(this.projectName, this.projectId, true);
			if(!AssetUtility.isGit(versionDir)) {
				User user = ThreadStore.getUser();
				String token = null;
				if(user != null && user.getAccessToken(this.gitProvider) != null) {
					token = user.getAccessToken(this.gitProvider).getAccess_token();
				}
				NounMetadata retNoun = GitPushUtils.clone(versionDir, this.projectGitRepo, token, this.gitProvider, false);
				if(retNoun.getNounType() == PixelDataType.ERROR) {
					throw new SemossPixelException(retNoun);
				}
			}
		}
		
		this.isAsset = Boolean.parseBoolean(prop.getProperty(Constants.IS_ASSET_APP));
		if(!isAsset) {
			loadInsightsRdbms();
		}
		
		this.projectProperties = new ProjectProperties(this.projectName, this.projectId);
	}
	
	@Override
	public Properties getProp() {
		return this.prop;
	}
	
	@Override
	public boolean isAsset() {
		return this.isAsset;
	}
	
	@Override
	public ProjectProperties getProjectProperties() {
		return this.projectProperties;
	}

	/**
	 * Load the insights database
	 */
	protected void loadInsightsRdbms() {
		// load the rdbms insights db
		this.insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(this.prop).getAbsolutePath();
		
		// if it is not defined directly in the smss
		// we will not create an insights database
		if(insightDatabaseLoc != null) {
			this.insightRdbms = ProjectHelper.loadInsightsEngine(this.prop, logger);
		}
		
//		// yay! even more updates
//		if(this.insightRdbms != null) {
//			// update explore an instance query!!!
//			updateExploreInstanceQuery(this.insightRdbms);
//		}
	}
	
	@Override
	public RDBMSNativeEngine getInsightDatabase() {
		return this.insightRdbms;
	}
	
	@Override
	public void setInsightDatabase(RDBMSNativeEngine insightDatabase) {
		this.insightRdbms = insightDatabase;
	}
	
	
	/**
	 * Sets the unique id for the project 
	 * @param projectId - id to set the project 
	 */
	@Override
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}
	
	@Override
	public String getProjectId() {
		return this.projectId;
	}
	
	@Override
	public String getProjectName() {
		return this.projectName;
	}
	
	@Override
	public String getProjectGitProvider() {
		return this.projectGitProvider;
	}
	
	@Override
	public String getProjectGitRepo() {
		return this.projectGitRepo;
	}
	
	@Override
	public AuthProvider getGitProvider() {
		return this.gitProvider;
	}
	
	@Override
	public Vector<String> getPerspectives() {
		Vector<String> perspectives = Utility.getVectorOfReturn(GET_ALL_PERSPECTIVES_QUERY, insightRdbms, false);
		if(perspectives.contains("")){
			int index = perspectives.indexOf("");
			perspectives.set(index, "Semoss-Base-Perspective");
		}
		return perspectives;
	}

	@Override
	public Vector<String> getInsights(String perspective) {
		String insightsInPerspective = null;
		if(perspective.equals("Semoss-Base-Perspective")) {
			perspective = null;
		}
		if(perspective != null && !perspective.isEmpty()) {
			insightsInPerspective = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE = '" + perspective + "' ORDER BY QUESTION_ORDER";
		} else {
			insightsInPerspective = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE IS NULL ORDER BY QUESTION_ORDER";
		}
		return Utility.getVectorOfReturn(insightsInPerspective, insightRdbms, false);
	}

	@Override
	public Vector<String> getInsights() {
		return Utility.getVectorOfReturn(GET_ALL_INSIGHTS_QUERY, insightRdbms, false);
	}
	
	@Override
	public void closeProject() {
		if(this.insightRdbms != null) {
			logger.debug("closing the insight engine ");
			this.insightRdbms.closeDB();
		}
		
		// remove the symbolic link
		if(this.projectId != null && this.projectName != null) {
			String public_home = DIHelper.getInstance().getProperty(Constants.PUBLIC_HOME);
			if(public_home != null) {
				String fileName = public_home + java.nio.file.FileSystems.getDefault().getSeparator() 
						+ SmssUtilities.getUniqueName(this.projectName, this.projectId);
				File file = new File(Utility.normalizePath(fileName));
				try {
					if(file.exists() && Files.isSymbolicLink(Paths.get(Utility.normalizePath(fileName))))
						FileUtils.forceDelete(file);
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	@Override
	public void deleteProject() {
		String folderName = SmssUtilities.getUniqueName(this.projectName, this.projectId);
		logger.debug("Closing " + folderName);
		this.closeProject();

		if(this.insightDatabaseLoc != null) {
			File insightFile = new File(this.insightDatabaseLoc);
			if(insightFile.exists()) {
				logger.info("Deleting insight file " + insightFile.getAbsolutePath());
				try {
					FileUtils.forceDelete(insightFile);
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// this check is to ensure we are deleting the right folder
		String folderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + Constants.PROJECT_FOLDER + DIR_SEPARATOR + folderName;
		File folder = new File(folderPath);
		if(folder.exists() && folder.isDirectory()) {
			logger.debug("folder getting deleted is " + folder.getAbsolutePath());
			try {
				FileUtils.deleteDirectory(folder);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e.getMessage());
			}
		}

		logger.debug("Deleting smss " + this.projectSmssFilePath);
		File smssFile = new File(this.projectSmssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			e.printStackTrace();
		}

		//remove from DIHelper
		String projectIds = (String)DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		projectIds = projectIds.replace(";" + this.projectId, "");
		// in case we are at the start
		projectIds = projectIds.replace(this.projectId + ";", "");
		DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectIds);
		DIHelper.getInstance().removeProjectProperty(this.projectId);
	}
	
	@Override
	public Vector<Insight> getInsight(String... questionIDs) {
		String idString = "";
		int numIDs = questionIDs.length;
		Vector<Insight> insightV = new Vector<Insight>(numIDs);
		List<Integer> counts = new Vector<Integer>(numIDs);
		for(int i = 0; i < numIDs; i++) {
			String id = questionIDs[i];
			try {
				idString = idString + "'" + id + "'";
				if(i != numIDs - 1) {
					idString = idString + ", ";
				}
				counts.add(i);
			} catch(NumberFormatException e) {
				System.err.println(">>>>>>>> FAILED TO GET ANY INSIGHT FOR ARRAY :::::: "+ questionIDs[i]);
			}
		}
		
		if(!idString.isEmpty()) {
			String query = GET_INSIGHT_INFO_QUERY.replace(QUESTION_PARAM_KEY, idString);
			logger.info("Running insights query " + Utility.cleanLogString(query));
			
			IRawSelectWrapper wrap = null;
			try {
				wrap = WrapperManager.getInstance().getRawWrapper(insightRdbms, query);
				while (wrap.hasNext()) {
					IHeadersDataRow dataRow = wrap.next();
					Object[] values = dataRow.getValues();
//					Object[] rawValues = dataRow.getRawValues();

					String rdbmsId = values[0] + "";
					String insightName = values[1] + "";
					
					String insightMakeup = (String) values[2];
//					Clob insightMakeup = (Clob) values[2];
//					InputStream insightMakeupIs = null;
//					if(insightMakeup != null) {
//						try {
//							insightMakeupIs = insightMakeup.getAsciiStream();
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//					}
					String layout = values[4] + "";
					String dataTableAlign = values[6] + "";
					String dataMakerName = values[7] + "";
					boolean cacheable = (boolean) values[8];
					Integer cacheMinutes = (Integer) values[9];
					if(cacheMinutes == null) {
						cacheMinutes = -1;
					}
					String cacheCron = (String) values[10];
					Boolean cacheEncrypt = (Boolean) values[11];
					if(cacheEncrypt == null) {
						cacheEncrypt = false;
					}
					Object[] pixel = null;
					// need to know if we have an array
					// or a clob
					if(insightRdbms.getQueryUtil().allowArrayDatatype()) {
						pixel = (Object[]) values[12];
					} else {
//						Clob pixelArray = (Clob) values[9];
//						InputStream pixelArrayIs = null;
//						if(pixelArray != null) {
//							try {
//								pixelArrayIs = pixelArray.getAsciiStream();
//							} catch (SQLException e) {
//								e.printStackTrace();
//							}
//						}

						// flush input stream to string
						String pixelArray = (String) values[12];
						Gson gson = new Gson();
						InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(pixelArray.getBytes()));
						pixel = gson.fromJson(reader, String[].class);
					}
					
					String perspective = values[3] + "";
					String order = values[5] + "";
					
					Insight in = null;
					if(pixel == null || pixel.length == 0) {
						in = new OldInsight(this, dataMakerName, layout);
						in.setRdbmsId(rdbmsId);
						in.setInsightName(insightName);
						((OldInsight) in).setOutput(layout);
						((OldInsight) in).setMakeup(insightMakeup);
//						in.setPerspective(perspective);
//						in.setOrder(order);
						((OldInsight) in).setDataTableAlign(dataTableAlign);
						// adding semoss parameters to insight
						((OldInsight) in).setInsightParameters(LegacyInsightDatabaseUtility.getParamsFromInsightId(this.insightRdbms, rdbmsId));
						in.setIsOldInsight(true);
					} else {
						in = new Insight(this.projectId, this.projectName, rdbmsId, cacheable, cacheMinutes, cacheCron, cacheEncrypt, pixel.length);
						in.setInsightName(insightName);
						List<String> pixelList = new Vector<String>(pixel.length);
						for(int i = 0; i < pixel.length; i++) {
							String pixelString = pixel[i].toString();
							List<String> breakdown;
							try {
								breakdown = PixelUtility.parsePixel(pixelString);
								pixelList.addAll(breakdown);
							} catch (ParserException | LexerException | IOException e) {
								logger.error(Constants.STACKTRACE, e);
								throw new IllegalArgumentException("Error occured parsing the pixel expression");
							}
						}
						in.setPixelRecipe(pixelList);
					}
					insightV.insertElementAt(in, counts.remove(0));
				}
			} catch(IllegalArgumentException e1) {
				throw e1;
			} catch (Exception e1) {
				logger.error(Constants.STACKTRACE, e1);
			} 
			finally {
				if(wrap != null) {
					wrap.cleanUp();
				}
			}
		}
		return insightV;
	}

	@Override
	public String getInsightDefinition() {
		StringBuilder stringBuilder = new StringBuilder();
		// call script command to get everything necessary to recreate rdbms engine on the other side//
		ISelectWrapper wrap = null;
		try {
			wrap = WrapperManager.getInstance().getSWrapper(insightRdbms, "SCRIPT");
			String[] names = wrap.getVariables();
			while(wrap.hasNext()) {
				ISelectStatement ss = wrap.next();
				System.out.println(ss.getRPropHash().toString());//
				stringBuilder.append(ss.getVar(names[0]) + "").append("%!%");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrap != null) {
				wrap.cleanUp();
			}
		}
		return stringBuilder.toString();
	}

	/*
	 * Methods that exist only to automate changes to databases
	 */
	
//	@Deprecated
//	private void updateExploreInstanceQuery(RDBMSNativeEngine insightRDBMS) {
//		// if solr doesn't have this engine
//		// do not add anything yet
//		// let it get added later
//		if(!SecurityUpdateUtils.containsDatabaseId(this.projectId) 
//				|| this.projectId.equals(Constants.LOCAL_MASTER_DB_NAME)
//				|| this.projectId.equals(Constants.SECURITY_DB)) {
//			return;
//		}
//		boolean tableExists = false;
//		ResultSet rs = null;
//		try {
//			rs = insightRDBMS.getConnectionMetadata().getTables(null, null, "QUESTION_ID", null);
//			if (rs.next()) {
//				  tableExists = true;
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if(rs != null) {
//					rs.close();
//				}
//			} catch(SQLException e) {
//				e.printStackTrace();
//			}
//		}
//		
//		if(tableExists) {
//			String exploreLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "ExploreInstanceDefaultWidget.json";
//			File exploreF = new File(exploreLoc);
//			if(!exploreF.exists()) {
//				// ughhh... cant do anything for ya buddy
//				return;
//			}
//			String newPixel = "AddPanel(0); Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
//			try {
//				newPixel += new String(Files.readAllBytes(exploreF.toPath())).replaceAll("\n|\r|\t", "").replaceAll("\\s\\s+", "").replace("<<ENGINE>>", this.projectId);
//			} catch (IOException e2) {
//				// can't help ya
//				return;
//			}
//			newPixel += "} </encode>\" ) ;";
//			
//			// for debugging... delete from question_id where question_name = 'New Explore an Instance'
//			InsightAdministrator admin = new InsightAdministrator(insightRDBMS);
//			IRawSelectWrapper it1 = null;
//			String oldId = null;
//			try {
//				it1 = WrapperManager.getInstance().getRawWrapper(insightRDBMS, "select id from question_id where question_name='Explore an instance of a selected node type'");
//				while(it1.hasNext()) {
//					// drop the old insight
//					oldId = it1.next().getValues()[0].toString();
//				}
//			} catch(Exception e) {
//				// if we have a db that doesn't actually have this table (forms, local master, etc.)
//			} finally {
//				if(it1 != null) {
//					it1.cleanUp();
//				}
//			}
//			
//			if(oldId != null) {
//				// update with the latest explore an instance
//				admin.updateInsight(oldId, "Explore an instance of a selected node type", "Graph", new String[]{newPixel});
//			}
//		}
//	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////// Load project specific reactors //////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	
	public IReactor getReactor(String className) 
	{	
		// try to get to see if this class already exists
		// no need to recreate if it does
		
		// get the prop file and find the parent

		File dbDirectory = new File(this.projectSmssFilePath);
		System.err.println(".");

		String dbFolder = this.projectName + "_" + dbDirectory.getParent()+ "/" + this.projectId;

		dbFolder = this.projectSmssFilePath.replaceAll(".smss", "");
		
		IReactor retReac = null;
		//String key = db + "." + insightId ;
		String key = this.projectId ;
		if(projectSpecificHash == null)
			projectSpecificHash = new HashMap<String, Class>();
		
		int randomNum = 0;
		//ReactorFactory.compileCache.remove(this.projectId);
		// compile the classes
		// TODO: do this evaluation automatically see if java folder is older than classes folder 
		if(!ReactorFactory.compileCache.containsKey(this.projectId))
		{
			String classesFolder = AssetUtility.getProjectAssetFolder(this.projectName, this.projectId) + "/classes";
			File classesDir = new File(classesFolder);
			if(classesDir.exists() && classesDir.isDirectory())
			{
				try {
					//FileUtils.cleanDirectory(classesDir);
					//classesDir.mkdir();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			int status = Utility.compileJava(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId), getCP());
			if(status == 0)
			{
				ReactorFactory.compileCache.put(this.projectId, Boolean.TRUE);
				
				if(ReactorFactory.randomNumberAdder.containsKey(this.projectId))
					randomNum = ReactorFactory.randomNumberAdder.get(this.projectId);				
				randomNum++;
				ReactorFactory.randomNumberAdder.put(this.projectId, randomNum);
				
				// add it to the key so we can reload
				key = this.projectId + randomNum;
	
				projectSpecificHash.clear();
			}
			// avoid loading everytime since it is an error
		}

		
		if(projectSpecificHash.size() == 0)
		{
			//compileJava(insightDirector.getParentFile().getAbsolutePath());
			// delete the classes directory first
			
			// need to pass the engine name also
			// so that the directory can be verified
			projectSpecificHash = Utility.loadReactors(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId), key);
			projectSpecificHash.put("loaded", "TRUE".getClass());
		}
		try
		{
			if(projectSpecificHash.containsKey(className.toUpperCase())) {
				Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
				retReac = (IReactor) thisReactorClass.newInstance();
				return retReac;
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
			
		return retReac;
	}


	public IReactor getReactor(String className, SemossClassloader customLoader) {	
		String projectBaseFolder = AssetUtility.getProjectBaseFolder(this.projectName, this.projectId);
		
		String pomFile = projectBaseFolder + File.separator + "version" + File.separator + "assets" + File.separator + "java" + File.separator + "pom.xml";
		
		if(new File(pomFile).exists()) {// this is maven
			return getReactorMvn(className, null);
		}
		else // keep the old processing
		{
			// try to get to see if this class already exists
			// no need to recreate if it does
			SemossClassloader cl = engineClassLoader;
			if(customLoader != null) {
				cl = customLoader;
			}
			cl.setFolder(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId) + "/classes");
			
			IReactor retReac = null;
			//String key = db + "." + insightId ;
			String key = this.projectId ;
			
			int randomNum = 0;
			
			if(!ReactorFactory.compileCache.containsKey(this.projectId))
			{
				engineClassLoader = new SemossClassloader(this.getClass().getClassLoader());
				cl = engineClassLoader;
				cl.uncommitEngine(this.projectId);
				
				String classesFolder = AssetUtility.getProjectAssetFolder(this.projectName, this.projectId) + "/classes";
				
				File classesDir = new File(classesFolder);
				if(classesDir.exists() && classesDir.isDirectory())
				{
					try {
						//FileUtils.cleanDirectory(classesDir);
						//classesDir.mkdir();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				int status = Utility.compileJava(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId), getCP());
				if(status == 0) {
					ReactorFactory.compileCache.put(this.projectId, Boolean.TRUE);
					if(ReactorFactory.randomNumberAdder.containsKey(this.projectId)) {
						randomNum = ReactorFactory.randomNumberAdder.get(this.projectId);			
					}
					randomNum++;
					ReactorFactory.randomNumberAdder.put(this.projectId, randomNum);
					// add it to the key so we can reload
					key = this.projectId + randomNum;	
				} else {
					ReactorFactory.compileCache.put(this.projectId, Boolean.FALSE);
				}
			}
			
			if(!cl.isCommitted(this.projectId))
			{
				//compileJava(insightDirector.getParentFile().getAbsolutePath());
				// delete the classes directory first
				projectSpecificHash = Utility.loadReactors(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId), key, cl);
				cl.commitEngine(this.projectId);
			}
			try
			{
				if(projectSpecificHash.containsKey(className.toUpperCase())) {
					Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
					retReac = (IReactor) thisReactorClass.newInstance();
					return retReac;
				}
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			return retReac;
		}
	}
	

	private String getCP()
	{
		String envClassPath = null;
		
		StringBuilder retClassPath = new StringBuilder("");
		ClassLoader cl = getClass().getClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        if(System.getProperty("os.name").toLowerCase().contains("win")) {
        for(URL url: urls){
        	String thisURL = URLDecoder.decode((url.getFile().replaceFirst("/", "")));
        	if(thisURL.endsWith("/"))
        		thisURL = thisURL.substring(0, thisURL.length()-1);

        	retClassPath
        		//.append("\"")
        		.append(thisURL)
        		//.append("\"")
        		.append(";");
        	
        }
        } else {
            for(URL url: urls){
            	String thisURL = URLDecoder.decode((url.getFile()));
            	if(thisURL.endsWith("/"))
            		thisURL = thisURL.substring(0, thisURL.length()-1);

            	retClassPath
            		//.append("\"")
            		.append(thisURL)
            		//.append("\"")
            		.append(":");
            }
        }
 
        envClassPath = "\"" + retClassPath.toString() + "\"";
        
        return envClassPath;
	}
	
	// create a symbolic link to the version directory
	public boolean publish(String public_home, String appId)
	{
		// find what is the final URL
		// this is the base url plus manipulations
		// find what the tomcat deploy directory is
		// no easy way to find other than may be find the classpath ? - will instrument this through RDF Map
		boolean enableForApp = false;
		enableForApp = (prop != null && prop.containsKey(Settings.PUBLIC_HOME_ENABLE) && (prop.get(Settings.PUBLIC_HOME_ENABLE)+ "").equalsIgnoreCase("true"));
		try {
			if(public_home != null && enableForApp && !publish)
			{
				String appHome = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + java.nio.file.FileSystems.getDefault().getSeparator() + "db" + java.nio.file.FileSystems.getDefault().getSeparator();
				
				Path sourcePath = Paths.get(AssetUtility.getProjectAssetFolder(this.projectName, appId));
				Path targetPath = Paths.get(public_home + java.nio.file.FileSystems.getDefault().getSeparator() + appId);
	
				File file = new File(public_home + java.nio.file.FileSystems.getDefault().getSeparator() + appId);

				boolean copy = DIHelper.getInstance().getProperty(Settings.COPY_APP) != null && DIHelper.getInstance().getProperty(Settings.COPY_APP).equalsIgnoreCase("true");
				
				// this is purely for testing purposes - this is because when eclipse publishes it wipes the directory and removes the actual db
				if(copy) 
				{
					if(!file.exists())
						file.mkdir();
					
					FileUtils.copyDirectory(sourcePath.toFile(), file);
					
				}
				// this is where we create symbolic link
				else if(!file.exists() &&  !Files.isSymbolicLink(targetPath))
				{
					Files.createSymbolicLink(targetPath, sourcePath);
				}
				file.deleteOnExit();
				publish = true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return enableForApp && publish;
	}
	
	// new reactor method that uses maven
	// the end user will execute maven. No automation is required there
	// need to compare target directory date with current
	// if so create a new classloader and load it
	private IReactor getReactorMvn(String className, JarClassLoader customLoader) 
	{	
		IReactor retReac = null;
		
		// if there is no java.. dont even bother with this
		// no need to spend time on any of this
		if(! (new File(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId) + File.separator + "assets" + File.separator + "java").exists()))
			return retReac;
			
		// try to get to see if this class already exists
		// no need to recreate if it does
		JarClassLoader cl = mvnClassLoader;
		if(customLoader != null)
			cl = customLoader;
		
		// get the prop file and find the parent
		File dbDirectory = new File(this.projectSmssFilePath);
		//System.err.println("..");
		String dbFolder = this.projectSmssFilePath.replaceAll(".smss", "");
				
		//String key = db + "." + insightId ;
		String key = this.projectId ;
		
		int randomNum = 0;
		
		//ReactorFactory.compileCache.remove(this.projectId);
		// this is the routine to compile the java classes
		// this is always user triggered
		// not sure we need to compile again
		// eval reload tried to see if the mvn dependency was created after the compile
		// if not it will reload
		// make the classloader
		if(mvnClassLoader == null || evalMvnReload())
		{
			mvnClassLoader = null;
			makeMvnClassloader();
			cl = mvnClassLoader;
			projectSpecificHash = Utility.loadReactorsMvn(AssetUtility.getProjectBaseFolder(this.projectName, this.projectId), key, cl, "target" + File.separator + "classes");
			ReactorFactory.compileCache.put(this.projectId, true);
		}

		// now that you have the reactor
		// create the reactor
		try
		{
			if(projectSpecificHash != null && projectSpecificHash.containsKey(className.toUpperCase())) 
			{
				Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
				retReac = (IReactor) thisReactorClass.newInstance();
				return retReac;
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return retReac;
	}
	
	private void makeMvnClassloader() {
		if(mvnClassLoader == null) // || if the classes folder is newer than the dependency file name
		{
			// now load the classloader
			// add the jars
			// locate all the reactors
			// and keep access to it

			mvnClassLoader = new JarClassLoader();
			// get all the new jars first
			// to add to the classloader
			String appRoot = AssetUtility.getProjectBaseFolder(this.projectName, this.projectId);
			String versionFolder = AssetUtility.getProjectAssetFolder(this.projectName, this.projectId);
			
			String mvnHome = System.getProperty(Settings.MVN_HOME);
			if(mvnHome == null)
				mvnHome = DIHelper.getInstance().getProperty(Settings.MVN_HOME);
			if(mvnHome == null)
				throw new IllegalStateException("Maven home should be defined in RDF_MAP / environment");
			
			// classes are in 
			// appRoot / classes
			// get the libraries
			// run maven dependency:list to get all the dependencies and process
			List <String> classpaths = composeClasspath(appRoot, versionFolder, mvnHome);
			if(classpaths != null) {
				for(int classPathIndex = 0;classPathIndex < classpaths.size();classPathIndex++) {
					// add all the libraries
					mvnClassLoader.add(classpaths.get(classPathIndex));
				}
			}
			
			// lastly add the classes folder
			mvnClassLoader.add(appRoot + File.pathSeparator + "classes/");
		}
	}
	
	private List <String> composeClasspath(String appRoot, String versionFolder, String mvnHome)
	{
		// java files are in /version/assets/java
		String pomFile  =  versionFolder + File.separator + "assets" + File.separator + "java" + File.separator + "pom.xml" ; // it is sitting in the asset root/version/assets/java
		InvocationRequest request = new DefaultInvocationRequest();
		//request.
		request.setPomFile( new File(pomFile) );
        String outputFile = appRoot + File.separator + "mvn_dep.output"; // need to change this java
        
        request.setMavenOpts("-DoutputType=graphml -DoutputFile=" + outputFile + " -DincludeScope=runtime ");
		request.setGoals( Collections.singletonList("dependency:list" ) );

		Invoker invoker = new DefaultInvoker();

		invoker.setMavenHome(new File(Utility.normalizePath(mvnHome)));
		BufferedReader br = null;
		try {
			InvocationResult result = invoker.execute( request );
			 
			if ( result.getExitCode() != 0 )
			{
			    throw new IllegalStateException( "Build failed." );
			}
			
			// otherwise we have the list
			String repoHome = System.getProperty(Settings.REPO_HOME);
			if(repoHome == null)
				repoHome = DIHelper.getInstance().getProperty(Settings.REPO_HOME);
			if(repoHome == null)
			    throw new IllegalStateException( "Repository Location is not known" );

			// now process the dependency list
			// and then delete it
			
			List <String> finalCP = new Vector<String>();
			File classesFile = new File(outputFile);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(classesFile)));
			String data = null;
			while((data = br.readLine()) != null)
			{
				if(data.endsWith("compile"))
				{
					String [] pathTokens = data.split(":");
					
					String baseDir = pathTokens[0];
					String packageName = pathTokens[1];
					String version = pathTokens[3];
					
					baseDir = repoHome + "/" + baseDir.replace(".", "/").trim();
					finalCP.add(baseDir + File.separator + packageName + File.separator + version + File.separator + packageName + "-" + version + ".jar");
				}
			}

			return finalCP;
			
		} catch (MavenInvocationException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(br != null) {
		          try {
		            br.close();
		          } catch(IOException e) {
		        	  logger.error(Constants.STACKTRACE, e);
		          }
		        }
		}


        return null;
	}
	
	private boolean evalMvnReload() {
		String appRoot = AssetUtility.getProjectBaseFolder(this.projectName, this.projectId);
		
		// need to see if the mvn_dependency file is older than target
		// if so reload
		File classesDir = new File(appRoot + File.separator + "target");
		File mvnDepFile = new File(appRoot + File.separator + "mvn_dep.output");
		
		if(!mvnDepFile.exists()) {
			return true;
		}
		
		if(!classesDir.exists()) {
			return false;
		}
			
		long classModifiedLong = classesDir.lastModified();
		long mvnDepModifiedLong = mvnDepFile.lastModified();
		
		return classModifiedLong > mvnDepModifiedLong;
	}
	
}
