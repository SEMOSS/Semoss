package prerna.project.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xml.sax.InputSource;

import com.google.gson.Gson;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.IReactor;
import prerna.reactor.ProjectCustomReactorCompilator;
import prerna.reactor.frame.r.util.TCPRTranslator;
import prerna.reactor.legacy.playsheets.LegacyInsightDatabaseUtility;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.ParserException;
import prerna.tcp.client.CustomReactorWrapper;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SemossClassloader;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;

public class Project implements IProject {

	private static final Logger classLogger = LogManager.getLogger(Project.class);
	
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
	private IProject.PROJECT_TYPE projectType;
	
	private Properties smssProp = null;
	private String projectSmssFilePath = null;
	
	private String projectBaseFolder = null;
	private String projectVersionFolder = null;
	private String projectAssetFolder = null;
	private String projectPortalFolder = null;
	
	private boolean isAsset = false;
	private ProjectProperties projectProperties = null;
	
	private RDBMSNativeEngine insightRdbms;
	private String insightDatabaseLoc;
	
	private Boolean execReactorOnSocket = null;

	/**
	 * Hash for the specific engine reactors
	 */
	private Map <String, Class<IReactor>> projectSpecificHash = null;
	
	/**
	 * Custom class loader
	 */
	private SemossClassloader projectClassLoader = new SemossClassloader(this.getClass().getClassLoader());
	private JarClassLoader mvnClassLoader = null;
	// maven not set
	private boolean mvnDefined = false;
	private SemossDate lastReactorCompilationDate = null;
	
	// publish portals
	private static final String PORTAL_INDEX_SCRIPT_ID = "semoss-env";
	private boolean hasPortal = false;
	private String portalName = null;
	private SemossDate lastPortalPublishDate = null;
	private boolean publishedPortal = false;
	private boolean republishPortal = false;
	
	// project specific analytics thread
	private transient ClientProcessWrapper cpw = new ClientProcessWrapper();
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		open(Utility.loadProperties(projectSmssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		this.projectId = this.smssProp.getProperty(Constants.PROJECT);
		this.projectName = this.smssProp.getProperty(Constants.PROJECT_ALIAS);
		
		this.isAsset = Boolean.parseBoolean(this.smssProp.getProperty(Constants.IS_ASSET_APP));
		if(this.isAsset) {
			this.projectBaseFolder = AssetUtility.getUserAssetAndWorkspaceBaseFolder(this.projectName, this.projectId);
			this.projectVersionFolder = AssetUtility.getUserAssetAndWorkspaceVersionFolder(this.projectName, this.projectId);
			this.projectAssetFolder = AssetUtility.getUserAssetAndWorkspaceAssetFolder(this.projectName, this.projectId);
		} else {
			this.projectBaseFolder = AssetUtility.getProjectBaseFolder(this.projectName, this.projectId);
			this.projectVersionFolder = AssetUtility.getProjectVersionFolder(this.projectName, this.projectId);
			this.projectAssetFolder = AssetUtility.getProjectAssetFolder(this.projectName, this.projectId);
			this.projectPortalFolder = AssetUtility.getProjectPortalsFolder(this.projectName, this.projectId);
		}
		
		if(this.smssProp.containsKey(Constants.PROJECT_GIT_PROVIDER) && this.smssProp.containsKey(Constants.PROJECT_GIT_CLONE)) {
			this.projectGitProvider = this.smssProp.getProperty(Constants.PROJECT_GIT_PROVIDER);
			this.projectGitRepo = this.smssProp.getProperty(Constants.PROJECT_GIT_CLONE);
			this.gitProvider = AuthProvider.getProviderFromString(projectGitProvider);

			if(!AssetUtility.isGit(projectVersionFolder)) {
				User user = ThreadStore.getUser();
				String token = null;
				if(user != null && user.getAccessToken(this.gitProvider) != null) {
					token = user.getAccessToken(this.gitProvider).getAccess_token();
				}
				NounMetadata retNoun = GitPushUtils.clone(this.projectVersionFolder, this.projectGitRepo, token, this.gitProvider, false);
				if(retNoun.getNounType() == PixelDataType.ERROR) {
					throw new SemossPixelException(retNoun);
				}
			}
		} 
		// initialize the default git
		else if(!AssetUtility.isGit(this.projectVersionFolder)) {
			GitRepoUtils.init(this.projectVersionFolder);
		}
		
		this.hasPortal = Boolean.parseBoolean(this.smssProp.getOrDefault(Settings.PUBLIC_HOME_ENABLE, "false")+ "");

		// project type is new
		// if has portal
		// will assume code if not provided
		// else will assume it is insight
		// TODO: potentially remove hasportal entirely
		String projectTypeStr = this.smssProp.getProperty(Constants.PROJECT_ENUM_TYPE);
		// is portal enabled in SMSS?
		if(this.hasPortal) {
			if(projectTypeStr == null) {
				this.projectType = IProject.PROJECT_TYPE.CODE;
			} else {
				this.projectType = IProject.PROJECT_TYPE.valueOf(projectTypeStr);
			}
		} else if(projectTypeStr != null) {
			this.projectType = IProject.PROJECT_TYPE.valueOf(projectTypeStr);
		} else {
			this.projectType = IProject.PROJECT_TYPE.INSIGHTS;
		}
		
		if(!isAsset) {
			loadInsightsRdbms();
		}
		
		this.projectProperties = new ProjectProperties(this.projectAssetFolder, this.projectName, this.projectId);
		
		// load any assets that are already compiled
		loadCompiledProjectReactors();
	}
	
	@Override
	public Properties getSmssProp() {
		return this.smssProp;
	}
	
	@Override
	public void setSmssProp(Properties smssProp) {
		this.smssProp = smssProp;
	}
	
	@Override
	public String getSmssFilePath() {
		return this.projectSmssFilePath;
	}
	
	@Override
	public void setSmssFilePath(String smssFilePath) {
		this.projectSmssFilePath = smssFilePath;
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
	 * @throws Exception 
	 */
	protected void loadInsightsRdbms() throws Exception {
		// load the rdbms insights db
		this.insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(this.smssProp).getAbsolutePath();
		
		// if it is not defined directly in the smss
		// we will not create an insights database
		if(insightDatabaseLoc != null) {
			this.insightRdbms = ProjectHelper.loadInsightsEngine(this.smssProp, classLogger);
		}
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
	public void setProjectName(String projectName) {
		this.projectName = projectName;
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
	public String getPortalName() {
		return this.portalName;
	}
	
	@Override
	public boolean isHasPortal() {
		return hasPortal;
	}

	@Override
	public void setHasPortal(boolean hasPortal) {
		this.hasPortal = hasPortal;
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
	public void close() {
		if(this.insightRdbms != null) {
			classLogger.debug("closing the insight engine ");
			try {
				this.insightRdbms.close();
			} catch (IOException e) {
				classLogger.warn("Error on closing insights database");
				classLogger.error(Constants.STACKTRACE, e);
			}
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
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// TODO: do we want to close the py process everything time we close?
		// we close when we push insights (cause of the insights database) or update smss
		
//		try {
//			if(tcpClient != null) {
//				// this should destroy the process as well
//				tcpClient.stopPyServe(this.tcpServerDirectory);
//			}
//		} catch(Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		// but just in case above doesn't destroy it
//		try {
//			if(tcpServerProcess != null) {
//				tcpServerProcess.destroy();
//			}
//		} catch(Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			try {
//				tcpServerProcess.destroy();
//			} catch(Exception e1) {
//				classLogger.error(Constants.STACKTRACE, e1);
//			}
//		}
	}

	@Override
	public void delete() {
		String folderName = SmssUtilities.getUniqueName(this.projectName, this.projectId);
		classLogger.debug("Closing " + folderName);
		this.close();

		if(this.insightDatabaseLoc != null) {
			File insightFile = new File(this.insightDatabaseLoc);
			if(insightFile.exists()) {
				classLogger.info("Deleting insight file " + insightFile.getAbsolutePath());
				try {
					FileUtils.forceDelete(insightFile);
				} catch(IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// this check is to ensure we are deleting the right folder
		String folderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + Constants.PROJECT_FOLDER + DIR_SEPARATOR + folderName;
		File folder = new File(folderPath);
		if(folder.exists() && folder.isDirectory()) {
			classLogger.debug("folder getting deleted is " + folder.getAbsolutePath());
			try {
				FileUtils.deleteDirectory(folder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e.getMessage());
			}
		}

		classLogger.debug("Deleting smss " + this.projectSmssFilePath);
		File smssFile = new File(this.projectSmssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public boolean holdsFileLocks() {
		return true;
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
			classLogger.info("Running insights query " + Utility.cleanLogString(query));
			
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
//							logger.error(Constants.STACKTRACE, e);
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
//								logger.error(Constants.STACKTRACE, e);
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
								classLogger.error(Constants.STACKTRACE, e);
								throw new IllegalArgumentException("Error occurred parsing the pixel expression");
							}
						}
						in.setPixelRecipe(pixelList);
					}
					insightV.insertElementAt(in, counts.remove(0));
				}
			} catch(IllegalArgumentException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} 
			finally {
				if(wrap != null) {
					try {
						wrap.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrap != null) {
				try {
					wrap.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
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
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			try {
//				if(rs != null) {
//					rs.close();
//				}
//			} catch(SQLException e) {
//				logger.error(Constants.STACKTRACE, e);
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
	
	// this is not used anymore
//	
//	public IReactor getReactor(String className) 
//	{	
//		// try to get to see if this class already exists
//		// no need to recreate if it does
//		
//		// get the prop file and find the parent
//
//		File dbDirectory = new File(this.projectSmssFilePath);
//		System.err.println(".");
//
//		String dbFolder = this.projectName + "_" + dbDirectory.getParent()+ "/" + this.projectId;
//
//		dbFolder = this.projectSmssFilePath.replaceAll(".smss", "");
//		
//		IReactor retReac = null;
//		//String key = db + "." + insightId ;
//		String key = this.projectId ;
//		if(projectSpecificHash == null)
//			projectSpecificHash = new HashMap<String, Class>();
//		
//		int randomNum = 0;
//		//ReactorFactory.compileCache.remove(this.projectId);
//		// compile the classes
//		// TODO: do this evaluation automatically see if java folder is older than classes folder 
//		if(!ReactorFactory.compileCache.containsKey(this.projectId))
//		{
//			String classesFolder = AssetUtility.getProjectAssetFolder(this.projectName, this.projectId) + "/classes";
//			File classesDir = new File(classesFolder);
//			if(classesDir.exists() && classesDir.isDirectory())
//			{
//				try {
//					//FileUtils.cleanDirectory(classesDir);
//					//classesDir.mkdir();
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					logger.error(Constants.STACKTRACE, e);
//				}
//			}
//			int status = Utility.compileJava(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId), getCP());
//			if(status == 0)
//			{
//				ReactorFactory.compileCache.put(this.projectId, Boolean.TRUE);
//				
//				if(ReactorFactory.randomNumberAdder.containsKey(this.projectId))
//					randomNum = ReactorFactory.randomNumberAdder.get(this.projectId);				
//				randomNum++;
//				ReactorFactory.randomNumberAdder.put(this.projectId, randomNum);
//				
//				// add it to the key so we can reload
//				key = this.projectId + randomNum;
//	
//				projectSpecificHash.clear();
//			}
//			// avoid loading everytime since it is an error
//		}
//
//		
//		if(projectSpecificHash.size() == 0)
//		{
//			//compileJava(insightDirector.getParentFile().getAbsolutePath());
//			// delete the classes directory first
//			
//			// need to pass the engine name also
//			// so that the directory can be verified
//			projectSpecificHash = Utility.loadReactors(AssetUtility.getProjectAssetFolder(this.projectName, this.projectId), key);
//			projectSpecificHash.put("loaded", "TRUE".getClass());
//		}
//		try
//		{
//			if(projectSpecificHash.containsKey(className.toUpperCase())) {
//				Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
//				retReac = (IReactor) thisReactorClass.newInstance();
//				return retReac;
//			}
//		} catch (InstantiationException e) {
//			logger.error(Constants.STACKTRACE, e);
//		} catch (IllegalAccessException e) {
//			logger.error(Constants.STACKTRACE, e);
//		}
//			
//		return retReac;
//	}
	
	/**
	 * 
	 */
	public void compileReactors(SemossClassloader customLoader) {
		File javaDirectory = new File(this.projectAssetFolder + DIR_SEPARATOR + "java");
		
		// if there is no java.. dont even bother with this
		// no need to spend time on any of this
		if( !javaDirectory.exists() ) {
			return;
		}
		
		File[] jars = javaDirectory.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		});
		File pomFile = new File(javaDirectory.getAbsolutePath() + DIR_SEPARATOR + "pom.xml");

		boolean loadJars = jars != null && jars.length > 0;
		boolean hasPom = pomFile.exists() && pomFile.isFile();
		
		if(loadJars) {
			compileReactorFromJars(jars);
		} else if(hasPom) {
			compileReactorsFromPom(pomFile);
		}
		// keep the old processing
		else {
			compileReactorsFromJavaFiles(customLoader);
		}
		
		this.lastReactorCompilationDate = new SemossDate(Utility.getLocalDateTimeUTC(LocalDateTime.now()));
		classLogger.info("Project '" + projectId + "' has new last compilation date = " + this.lastReactorCompilationDate);
	}
	
	private void compileReactorsFromJavaFiles(SemossClassloader customLoader) {
		String classesFolder = this.projectAssetFolder + "/classes";
		File classesDir = new File(classesFolder);
		if(classesDir.exists() && classesDir.isDirectory()) {
			try {
				//FileUtils.cleanDirectory(classesDir);
				//classesDir.mkdir();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		SemossClassloader cl = projectClassLoader;
		if(customLoader != null) {
			cl = customLoader;
		}
		cl.setFolder(classesFolder);
		
		// have the classes been loaded already?
		if(ProjectCustomReactorCompilator.needsCompilation(this.projectId)) {
			projectClassLoader = new SemossClassloader(this.getClass().getClassLoader());
			cl = projectClassLoader;
			cl.uncommitEngine(this.projectId);

			int status = Utility.compileJava(this.projectAssetFolder, getCP());
			if(status == 0) {
				ProjectCustomReactorCompilator.setCompiled(this.projectId);
			} else {
				ProjectCustomReactorCompilator.setFailed(this.projectId);
			}
		}
		
		if(!cl.isCommitted(this.projectId)) {
			//compileJava(insightDirector.getParentFile().getAbsolutePath());
			// delete the classes directory first
			projectSpecificHash = Utility.loadReactors(this.projectAssetFolder, cl);
			cl.commitEngine(this.projectId);
		}
	}
	
	/**
	 * 
	 */
	public IReactor getReactor(String className, SemossClassloader customLoader) {
		SemossDate lastCompiledDateInSecurity = SecurityProjectUtils.getReactorCompilationTimestamp(this.projectId);
		boolean outOfDate = false;
		if(lastCompiledDateInSecurity != null && this.lastReactorCompilationDate != null) {
			outOfDate = lastCompiledDateInSecurity.getLocalDateTime().isAfter(this.lastReactorCompilationDate.getLocalDateTime());
		}
		// just pull to make sure we have the latest in case project was loaded
		// but not published
		if(outOfDate || this.lastReactorCompilationDate == null) {
			classLogger.info("Pulling Java folder for project = " + this.projectId + ". Current reactors out of date = " + outOfDate + ". Last compilation date = " + this.lastReactorCompilationDate);
			ClusterUtil.pullProjectFolder(this, this.projectVersionFolder, Constants.ASSETS_FOLDER + "/" + "java");
			this.clearClassCache();
		}
		
		IReactor retReac = null;
		// if we are not out of date, we can see if this exists
		if(!outOfDate && this.lastReactorCompilationDate != null && projectSpecificHash != null) {
			try {
				if(projectSpecificHash.containsKey(className.toUpperCase())) {
					Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
					retReac = (IReactor) thisReactorClass.newInstance();
				}
			} catch (InstantiationException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IllegalAccessException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			// else we will see if we have java
			File javaDirectory = new File(this.projectAssetFolder + DIR_SEPARATOR + "java");
			
			// if there is no java.. dont even bother with this
			// no need to spend time on any of this
			if( !javaDirectory.exists() ) {
				// dont need to keep setting this 
				if(this.lastReactorCompilationDate == null) {
					this.lastReactorCompilationDate = new SemossDate(Utility.getLocalDateTimeUTC(LocalDateTime.now()));
					classLogger.info("Project '" + projectId + "' does not have a Java folder. Will still set the last compilation date = " + this.lastReactorCompilationDate);
				}
				return null;
			}
			
			File[] jars = javaDirectory.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".jar");
				}
			});
			File pomFile = new File(javaDirectory.getAbsolutePath() + DIR_SEPARATOR + "pom.xml");

			boolean loadJars = jars != null && jars.length > 0;
			boolean hasPom = pomFile.exists() && pomFile.isFile();
			
			if(loadJars) {
				retReac =  getReactorFromJars(className, jars);
			} else if(hasPom) {
				retReac = getReactorsFromPom(className, pomFile);
			}
			// keep the old processing
			else {
				compileReactorsFromJavaFiles(customLoader);
				try {
					if(projectSpecificHash.containsKey(className.toUpperCase())) {
						Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
						retReac = (IReactor) thisReactorClass.newInstance();
					}
				} catch (InstantiationException e) {
					classLogger.error(Constants.STACKTRACE, e);
				} catch (IllegalAccessException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}

			this.lastReactorCompilationDate = new SemossDate(Utility.getLocalDateTimeUTC(LocalDateTime.now()));
			classLogger.info("Project '" + projectId + "' has new last compilation date = " + this.lastReactorCompilationDate);
		}
		
		boolean useNettyPy = DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON) != null
				&& DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON).equalsIgnoreCase("true");
		if (!useNettyPy) {
			return retReac;
		}
		
		// secondary check to execute reactor here
		if(executeReactorOnSocket() && ( 
				(
				DIHelper.getInstance().getLocalProp("core") == null || 
				DIHelper.getInstance().getLocalProp("core").toString().equalsIgnoreCase("true")
				) 
				&& retReac != null)
				) 
		{
		
			// need to convert this to reactor wrapper before I give it to be executed			
			CustomReactorWrapper wrapper = new CustomReactorWrapper();
			wrapper.realReactor = retReac;
			wrapper.reactorCallName = className;
			return wrapper;
		} else {
			return retReac;
		}
	}
	
	@Override
	public TreeSet<String> getAvailableReactors() {
		if(this.projectSpecificHash == null) {
			return new TreeSet<>();
		}
		
		return new TreeSet<>(this.projectSpecificHash.keySet());
	}
	
	private boolean executeReactorOnSocket()
	{
		if(this.execReactorOnSocket == null)
		{
			this.execReactorOnSocket= (DIHelper.getInstance().getProperty(Settings.CUSTOM_REACTOR_EXECUTION) != null)
					&& (DIHelper.getInstance().getProperty(Settings.CUSTOM_REACTOR_EXECUTION).toString().equalsIgnoreCase("SOCKET"));
		}
		return execReactorOnSocket;
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
	
	@Override
	public boolean requirePublish(boolean pullFromCloud) {
		// check in security DB when we last published
		SemossDate lastPublishedDateInSecurity = SecurityProjectUtils.getPortalPublishedTimestamp(this.projectId);
		boolean outOfDate = false;
		if(lastPublishedDateInSecurity != null && this.lastPortalPublishDate != null) {
			outOfDate = lastPublishedDateInSecurity.getLocalDateTime().isAfter(this.lastPortalPublishDate.getLocalDateTime());
		}
		if(outOfDate || this.lastPortalPublishDate == null) {
			// just pull to make sure we have the latest in case project was loaded
			// but not published
			if(pullFromCloud) {
				classLogger.info("Pulling Portals folder for project = " + this.projectId + ". Current portal out of date = " + outOfDate + ". Last portal publish date = " + this.lastPortalPublishDate);
				ClusterUtil.pullProjectFolder(this, this.projectPortalFolder);
			}
		}
		
		// if this are true we want to republish
		// we just add the additional logic above if we have to pull from cloud
		return this.republishPortal || outOfDate || (this.hasPortal && !this.publishedPortal);
	}
	
	@Override
	/**
	 * Publish the portals folder to public_home
	 */
	public boolean publish(String publicHomeFilePath, boolean pullFromCloud) {
		if(publicHomeFilePath == null) {
			return false;
		}
		
		// find what is the final URL
		// this is the base url plus manipulations
		// find what the tomcat deploy directory is
		// no easy way to find other than may be find the classpath ? - will instrument this through RDF Map
		boolean requirePublish = requirePublish(pullFromCloud);
		try {
			if(requirePublish) {
				Path sourcePortalsProjectPath = Paths.get(this.projectPortalFolder);
				Path targetPublicHomeProjectPortalsPath = Paths.get(publicHomeFilePath + DIR_SEPARATOR + this.projectId + DIR_SEPARATOR + Constants.PORTALS_FOLDER);
	
				File targetPublicHomeProjectPortalsDir = targetPublicHomeProjectPortalsPath.toFile();
				// if the target directory exists
				// we have to delete it before 
				if(targetPublicHomeProjectPortalsDir.exists() && targetPublicHomeProjectPortalsDir.isDirectory()) {
					FileUtils.deleteDirectory(targetPublicHomeProjectPortalsDir);
				}
				
				rewritePortalIndexHtml(this.projectPortalFolder + DIR_SEPARATOR + "index.html");
				
				// do we physically copy of link?
				// first smss file
				// second rdf map
				boolean copy = true;
				if(smssProp != null && smssProp.getProperty(Settings.COPY_PROJECT) != null) {
					copy = Boolean.parseBoolean(smssProp.getProperty(Settings.COPY_PROJECT) + "");
				} else if(DIHelper.getInstance().getProperty(Settings.COPY_PROJECT) != null) {
					copy = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Settings.COPY_PROJECT) + "");	
				}
				
				// this is purely for testing purposes - this is because when eclipse publishes it wipes the directory and removes the actual db
				if(copy) {
					if(!targetPublicHomeProjectPortalsDir.exists()) {
						targetPublicHomeProjectPortalsDir.mkdir();
					}
					FileUtils.copyDirectory(sourcePortalsProjectPath.toFile(), targetPublicHomeProjectPortalsDir);
				}
				// this is where we create symbolic link
				else if(!targetPublicHomeProjectPortalsDir.exists() && !Files.isSymbolicLink(targetPublicHomeProjectPortalsPath)) {
					Files.createSymbolicLink(targetPublicHomeProjectPortalsPath, sourcePortalsProjectPath);
				}
				targetPublicHomeProjectPortalsDir.deleteOnExit();
				this.publishedPortal = true;
				this.republishPortal = false;
				this.lastPortalPublishDate = new SemossDate(Utility.getLocalDateTimeUTC(LocalDateTime.now()));
				classLogger.info("Project '" + SmssUtilities.getUniqueName(this.projectName, this.projectId) + "' has new last portal published date = " + this.lastPortalPublishDate);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			this.publishedPortal = false;
			this.lastPortalPublishDate = null;
		}
		
		return this.publishedPortal;
	}
	
	private void rewritePortalIndexHtml(String indexHtmlPath) {
		/*
		 * <script>
		        window.SEMOSS = {
		            "APP": "<project_id>",
		            "MODULE": "/{route - optional}/{context - usually just Monolith}"
		        }
		    </script>
		 */
		// add the route if this is server deployment
		File indexHtmlF = new File(indexHtmlPath);
		if(!indexHtmlF.exists() || !indexHtmlF.isFile()) {
			return;
		}
		
		String module = Utility.getApplicationRouteAndContextPath();
		org.jsoup.nodes.Document document;
		try {
			document = Jsoup.parse(indexHtmlF, "UTF-8");
			String scriptContent = "{\"APP\": \""+projectId+"\",\"MODULE\": \""+module+"\"}";
			Element autoGenScript = document.getElementById(PORTAL_INDEX_SCRIPT_ID);
			if(autoGenScript == null) {
				document.selectFirst("head")
					.child(0)
					.before("<script id=\""+PORTAL_INDEX_SCRIPT_ID+"\" type=\"application/json\">"+scriptContent+"</script>");
			} else {
				autoGenScript.html(scriptContent);
			}
			
			String newHtml = document.html();
			try(FileWriter fw = new FileWriter(indexHtmlF, false)) {
				fw.write(newHtml);
				fw.flush();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public void setRepublish(boolean republish) {
		this.republishPortal = republish;
	}
	
	@Override
	public boolean isPublished() {
		return this.publishedPortal;
	}
	
	@Override
	public SemossDate getLastPublishDate() {
		return this.lastPortalPublishDate;
	}
	
	/**
	 * 
	 * @param pomFile
	 */
	private void compileReactorsFromPom(File pomFile) {
		if(mvnClassLoader == null || evalMvnReload()) {
			mvnClassLoader = null;
			makeMvnClassloader(pomFile);
			if(!mvnDefined) {
				// no point none of the stuff is set anyways
				return;
			}
			// try to load it directly from assets
			String targetFolder = getTargetFolder(pomFile);
			targetFolder = targetFolder + DIR_SEPARATOR + "classes"; // target folder is relative to java folder for the main assets
			projectSpecificHash = Utility.loadReactorsFromPom(pomFile.getParent(), mvnClassLoader, targetFolder);
			ProjectCustomReactorCompilator.setCompiled(this.projectId);
		}
	}
	
	/**
	 * 
	 * @param className
	 * @param pomFile
	 * @return
	 */
	private IReactor getReactorsFromPom(String className, File pomFile) {
		compileReactorsFromPom(pomFile);
		IReactor retReac = null;
		try
		{
			if(projectSpecificHash != null && projectSpecificHash.containsKey(className.toUpperCase())) 
			{
				Class<IReactor> thisReactorClass = projectSpecificHash.get(className.toUpperCase());
				retReac = (IReactor) thisReactorClass.newInstance();
				return retReac;
			}
		} catch (InstantiationException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return retReac;
	}
	
	/**
	 * 
	 * @param jars
	 */
	private void compileReactorFromJars(File[] jars) {
		// have the classes been loaded already?
		if(ProjectCustomReactorCompilator.needsCompilation(this.projectId)) {
			projectClassLoader = new SemossClassloader(this.getClass().getClassLoader());
			URL[] urls = new URL[jars.length];
			for(int i = 0; i < jars.length; i++) {
				try {
					urls[i] = jars[i].toURI().toURL();
				} catch (MalformedURLException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to load jar file : " + jars[i].getName());
				}
			}
			projectSpecificHash = Utility.loadReactorsFromJars(urls);
			ProjectCustomReactorCompilator.setCompiled(this.projectId);
		}
	}
	
	/**
	 * 
	 * @param className
	 * @param jars
	 * @return
	 */
	private IReactor getReactorFromJars(String className, File[] jars) {	
		compileReactorFromJars(jars);
		
		IReactor retReac = null;
		try {
			if(projectSpecificHash.containsKey(className.toUpperCase())) {
				Class<IReactor> thisReactorClass = projectSpecificHash.get(className.toUpperCase());
				retReac = (IReactor) thisReactorClass.newInstance();
			}
		} catch (InstantiationException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return retReac;
	}

	// new reactor method that uses maven
	// the end user will execute maven. No automation is required there
	// need to compare target directory date with current
	// if so create a new classloader and load it
	private IReactor getPortalReactorMvn(String className, File pomFile, JarClassLoader customLoader) 
	{	
		// run through every portal and load
		
		IReactor retReac = null;
		
		// if there is no java.. dont even bother with this
		// no need to spend time on any of this
		if(! (new File(this.projectAssetFolder + DIR_SEPARATOR + "java").exists()))
			return retReac;
			
		// try to get to see if this class already exists
		// no need to recreate if it does
		JarClassLoader cl = mvnClassLoader;
		if(customLoader != null) {
			cl = customLoader;
		}
		
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
			makeMvnClassloader(pomFile);
			cl = mvnClassLoader;
			// try to load it directly from assets
			projectSpecificHash = Utility.loadReactorsFromPom(this.projectBaseFolder, cl, "target" + DIR_SEPARATOR + "classes");
			// if not load it from the 
			Map <String, Class<IReactor>> versionHash = Utility.loadReactorsFromPom(this.projectAssetFolder + DIR_SEPARATOR + "java", cl, "target" + DIR_SEPARATOR + "classes");
			projectSpecificHash.putAll(versionHash);
			ProjectCustomReactorCompilator.setCompiled(this.projectId);
		}

		// now that you have the reactor
		// create the reactor
		try
		{
			if(projectSpecificHash != null && projectSpecificHash.containsKey(className.toUpperCase())) 
			{
				Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
				retReac = (IReactor) thisReactorClass.newInstance();
				
				// need to convert this to reactor wrapper before I give it to be executed
				CustomReactorWrapper wrapper = new CustomReactorWrapper();
				wrapper.realReactor = retReac;
				
				return wrapper;
			}
		} catch (InstantiationException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return retReac;
	}

	
	private void makeMvnClassloader(File pomFile) {
		if(mvnClassLoader == null) // || if the classes folder is newer than the dependency file name
		{
			// now load the classloader
			// add the jars
			// locate all the reactors
			// and keep access to it

			mvnClassLoader = new JarClassLoader();
			// get all the new jars first
			// to add to the classloader
			String mvnHome = System.getProperty(Settings.MVN_HOME);
			if(mvnHome == null) {
				mvnHome = DIHelper.getInstance().getProperty(Settings.MVN_HOME);
			}
			if(mvnHome == null) {
				mvnDefined = true;
				return;
			}
			
			// classes are in 
			// appRoot / classes
			// get the libraries
			// run maven dependency:list to get all the dependencies and process
			List <String> classpaths = composeClasspath(pomFile, mvnHome);
			if(classpaths != null) {
				for(int classPathIndex = 0;classPathIndex < classpaths.size();classPathIndex++) {
					// add all the libraries
					mvnClassLoader.add(classpaths.get(classPathIndex));
				}
			}
		}
	}
	
	private List <String> composeClasspath(File pomFile, String mvnHome)
	{
		BufferedReader br = null;
		try 
		{
	        File outputFile = new File(pomFile.getParent() + DIR_SEPARATOR + "mvn_dep.output"); // need to change this java
			boolean built = false;
	        if(mvnHome != null)
			{
		        
		        // run this only if mvn dependencies have been wiped out
		        if(outputFile.exists()) {
		        	built = true;
		        } else {
					InvocationRequest request = new DefaultInvocationRequest();
					//request.
					request.setPomFile( pomFile );
			        request.setMavenOpts("-DoutputType=graphml -DoutputFile=\"" + outputFile.getAbsolutePath() + "\" -DincludeScope=runtime ");
					request.setGoals( Collections.singletonList("dependency:list" ) );
		
					Invoker invoker = new DefaultInvoker();
					invoker.setWorkingDirectory(pomFile.getParentFile());
					invoker.setMavenHome(new File(Utility.normalizePath(mvnHome)));
					InvocationResult result = invoker.execute( request );
					 
					if ( result.getExitCode() != 0 )
					{
					    built = false;
					    //throw new IllegalStateException( "Build failed." );
					}
		        }
			}
	        
	        if(!built) { // may be maven is not set but mvn as a executor is available
				// need to make the modification to this
				CmdExecUtil ceu = new CmdExecUtil(projectName, pomFile.getParent(), null);
				// mvn dependency:list -DoutputType=graphml -DoutputFile=./mvn_dep.output -DincludeScope=runtime -f pom.xml
				ceu.executeCommand("mvn dependency:list -DoutputType=graphml -DoutputFile=\"" + outputFile.getAbsolutePath() + "\" -DincludeScope=runtime -f \"" + pomFile + "\"");
			}
			// now process the dependency list 
			// and then delete it
			// otherwise we have the list
			String repoHome = System.getProperty(Settings.REPO_HOME);
			if(repoHome == null) {
				repoHome = DIHelper.getInstance().getProperty(Settings.REPO_HOME);
			}
			if(repoHome == null) {
				mvnDefined = true;
				return null;
			}
			
			List <String> finalCP = new Vector<String>();
			br = new BufferedReader(new InputStreamReader(new FileInputStream(outputFile)));
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
					finalCP.add(baseDir + DIR_SEPARATOR + packageName + DIR_SEPARATOR + version + DIR_SEPARATOR + packageName + "-" + version + ".jar");
				}
			}

			return finalCP;
			
		} catch (MavenInvocationException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (FileNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(br != null) {
				try {
					br.close();
				} catch(IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
        return null;
	}
	
	public void clearClassCache() {
		// clear the local hash
		if(projectSpecificHash != null) {
			this.projectSpecificHash.clear();
		}
		// recompile within reactor factory
		ProjectCustomReactorCompilator.reset(this.projectId);
		File mvnDepFile = new File(this.projectBaseFolder + DIR_SEPARATOR + "mvn_dep.output");
		// delete the maven dep file
		if(mvnDepFile.exists()) {
			mvnDepFile.delete();
		}
		
		// set the classloader to null
		mvnClassLoader = null;
	}
	
	private boolean evalMvnReload() {
		// need to see if the mvn_dependency file is older than target
		// if so reload
		File classesDir = new File(this.projectBaseFolder + DIR_SEPARATOR + "target");
		File mvnDepFile = new File(this.projectBaseFolder + DIR_SEPARATOR + "mvn_dep.output");
		
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
		
	// get the target folder
	public String getTargetFolder(File pomFile) {
		String targetFolder = null;
		try {
			InputSource is = new InputSource(new FileInputStream(pomFile));
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			
			org.w3c.dom.Document d = builder.parse(is);
			
			XPathFactory xpathfactory = XPathFactory.newInstance();
			XPath xpath = xpathfactory.newXPath();

			XPathExpression expr = xpath.compile("//project/build/directory/text()");
			Object result = expr.evaluate(d, XPathConstants.NODESET);
			org.w3c.dom.NodeList nodes = (org.w3c.dom.NodeList) result;
			for (int i = 0; i < nodes.getLength(); i++) {
			  targetFolder = nodes.item(i).getNodeValue();
			}
		} catch (Exception e) {
      	  classLogger.error(Constants.STACKTRACE, e);
		}
	    return targetFolder;
	}
	
	/**
	 * load any existing reactor class files as is
	 */
	private void loadCompiledProjectReactors() {
		String pomFile = this.projectBaseFolder + DIR_SEPARATOR + Constants.VERSION_FOLDER + DIR_SEPARATOR + Constants.ASSETS_FOLDER 
				+ DIR_SEPARATOR + "java" + DIR_SEPARATOR + "pom.xml";
		
		if(new File(pomFile).exists()) {
			// this is maven

			//TODO: need to figure out how we see if maven is already compiled and exists
			//TODO: need to figure out how we see if maven is already compiled and exists
			//TODO: need to figure out how we see if maven is already compiled and exists
			//TODO: need to figure out how we see if maven is already compiled and exists

		}
		else // load from existing classes folder - might be outdated but only doing this when the project is first loaded
		{
			String classesFolder = this.projectAssetFolder + "/classes";
			File classesDir = new File(classesFolder);
			if(classesDir.exists() && classesDir.isDirectory()) {
				SemossClassloader cl = this.projectClassLoader;
				cl.setFolder(classesFolder);
				this.projectSpecificHash = Utility.loadReactors(this.projectAssetFolder, cl);
				if(this.projectSpecificHash != null && !this.projectSpecificHash.isEmpty()) {
					ProjectCustomReactorCompilator.setCompiled(this.projectId);
					lastReactorCompilationDate = new SemossDate(Utility.getLocalDateTimeUTC(LocalDateTime.now()));
				}
			}
		}
	}
	
	@Override
	public ClientProcessWrapper getClientProcessWrapper() {
		return this.cpw;
	}
	
	@Override
	public SocketClient getProjectTcpClient() {
		return getProjectTcpClient(true);
	}
	
	@Override
	public SocketClient getProjectTcpClient(boolean create) {
		return getProjectTcpClient(create, -1);
	}
	
	@Override
	public SocketClient getProjectTcpClient(boolean create, int port) {
		if(!create) {
			return this.cpw.getSocketClient();
		}
		
		if(this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return this.cpw.getSocketClient();
		}
		
		createProjectTcpServer(port);
		return this.cpw.getSocketClient();
	}
	
	/**
	 * 
	 * @return
	 */
	public TCPRTranslator getProjectRTranslator() {
		if(this.cpw.getSocketClient() == null) {
			createProjectTcpServer(-1);
		} else if( !this.cpw.getSocketClient().isConnected()) {
			this.cpw.shutdown();
			try {
				this.cpw.reconnect();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Failed to start TCP Server for Project = " + SmssUtilities.getUniqueName(this.projectName, this.projectId));
			}
		}
		TCPRTranslator rJavaTranslator = new TCPRTranslator();
		rJavaTranslator.setClient(this.cpw.getSocketClient());
		return rJavaTranslator;
	}

	/**
	 * 
	 * @return
	 */
	public TCPPyTranslator getProjectPyTranslator() {
		if(this.cpw.getSocketClient() == null) {
			createProjectTcpServer(-1);
		} else if( !this.cpw.getSocketClient().isConnected()) {
			this.cpw.shutdown();
			try {
				this.cpw.reconnect();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Failed to start TCP Server for Project = " + SmssUtilities.getUniqueName(this.projectName, this.projectId));
			}
		}
		TCPPyTranslator pyJavaTranslator = new TCPPyTranslator();
		pyJavaTranslator.setSocketClient(this.cpw.getSocketClient());
		return pyJavaTranslator;
	}
	
	/**
	 * 
	 */
	private synchronized void createProjectTcpServer(int port) {
		if(this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			boolean nativePyServer = false;
			// first is it defined in smss
			String nativePyServerStr = this.smssProp.getProperty(Settings.NATIVE_PY_SERVER);
			// if not, grab from rdf map
			if(nativePyServerStr == null || (nativePyServerStr=nativePyServerStr.trim()).isEmpty()) {
				nativePyServerStr = DIHelper.getInstance().getProperty(Settings.NATIVE_PY_SERVER);
			}
			if(nativePyServerStr != null && !(nativePyServerStr=nativePyServerStr.trim()).isEmpty()) {
				nativePyServer = Boolean.parseBoolean(nativePyServerStr);
			}
			
			boolean debug = false;
			if(port < 0) {
				String forcePort = this.projectProperties.getProperty(Settings.FORCE_PORT);
				// port has not been forced
				if(forcePort != null && !(forcePort=forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch(NumberFormatException e) {
						// ignore
						classLogger.warn("Project " + this.projectId + " has an invalid FORCE_PORT value");
					}
				}
			}
			
			//TODO: how do we account for chroot??
			String customClassPath = DIHelper.getInstance().getProperty("TCP_WORKER_CP");
			if(customClassPath == null) {
				classLogger.info("No custom class path set");
			}
			
			Path serverDirectoryPath = null;
			try {
				serverDirectoryPath = Files.createTempDirectory(Paths.get(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR)), "a");
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not create directory to launch project process");
			}
			
			classLogger.info("Starting TCP Server for Project = " + SmssUtilities.getUniqueName(this.projectName, this.projectId));
			// TODO: ignoring chroot for now...
			try {
				String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
				String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
				this.cpw.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectoryPath.toString(), customClassPath, debug);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Failed to start TCP Server for Project = " + SmssUtilities.getUniqueName(this.projectName, this.projectId));
			}
		}
	}

	@Override
	public String getCompileOutput() {
		String finalOutput = null;
		try {
			String compilerOutput = AssetUtility.getProjectAssetFolder(this.projectId) + "/classes/compileerror.out";
			File file = new File(compilerOutput);
			if(file.exists())
				finalOutput = FileUtils.readFileToString(new File(compilerOutput));
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return finalOutput;
	}
	
	//////////////////////////////////////////////////////////////////
	
	/*
	 * METHODS FROM IEngine that redirect to IProject methods
	 */

	@Override
	public void setEngineId(String engineId) {
		setProjectId(engineId);
	}

	@Override
	public String getEngineId() {
		return getProjectId();
	}

	@Override
	public void setEngineName(String engineName) {
		setProjectName(engineName);
	}

	@Override
	public String getEngineName() {
		return getProjectName();
	}

	@Override
	public Properties getOrigSmssProp() {
		return this.smssProp;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.PROJECT;
	}
	
	@Override
	public PROJECT_TYPE getProjectType() {
		return this.projectType;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.projectType.name();
	}

}
