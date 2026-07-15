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
package prerna.project.impl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.xml.sax.InputSource;

import com.google.gson.Gson;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IMCP;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.InternalMCP;
import prerna.engine.impl.RemoteMCP;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.project.impl.notebook.INotebookBuilder;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.project.impl.notebook.NotebookHelperFactory;
import prerna.project.impl.notebook.NotebookWriterFactory;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.IReactor;
import prerna.reactor.ProjectCustomReactorCompilator;
import prerna.reactor.frame.r.util.TCPRTranslator;
import prerna.reactor.legacy.playsheets.LegacyInsightDatabaseUtility;
import prerna.sablecc2.NotebookExecution;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.ParserException;
import prerna.tcp.client.CustomReactorWrapper;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.UploadUtilities;
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
	private String displayName = null;
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
	private String projectNotebookFolder = null;

	private boolean isAsset = false;
	private ProjectProperties projectProperties = null;

	private IRDBMSEngine insightRdbms;
	private String insightDatabaseLoc;

	private Boolean execReactorOnSocket = null;

	/**
	 * Hash for the specific engine reactors
	 */
	private Map<String, Class<IReactor>> projectSpecificHash = null;

	/**
	 * Custom class loader
	 */
	private ProjectReactorHelper reactorHelper = null;
	private SemossDate lastReactorCompilationDate = null;

	// publish portals
	private static final String PORTAL_INDEX_SCRIPT_ID = "semoss-env";
	private SemossDate lastPortalPublishDate = null;
	private boolean publishedPortal = false;
	private boolean republishPortal = false;

	// python server
	protected String prefix = null;
	protected String workingDirectory;
	protected String workingDirectoryBasePath = null;
	protected File cacheFolder;
	// project specific analytics thread
	private transient ClientProcessWrapper cpw = new ClientProcessWrapper();
	protected PyTranslator pyTranslator = null;

	protected IMCP projectMCP = null;

	protected volatile LoggerContext engineSpecificLoggerCtx;

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
		String smssDisplayName = this.smssProp.getProperty(Constants.PROJECT_DISPLAY_NAME);
		this.displayName = (smssDisplayName != null && !smssDisplayName.trim().isEmpty()) ? smssDisplayName
				: this.projectName;

		this.isAsset = Boolean.parseBoolean(this.smssProp.getProperty(Constants.IS_ASSET_APP));
		if (this.isAsset) {
			this.projectBaseFolder = AssetUtility.getUserAssetAppRootFolder(this.projectName, this.projectId);
			this.projectVersionFolder = AssetUtility.getUserAssetVersionFolder(this.projectName, this.projectId);
			this.projectAssetFolder = AssetUtility.getUserAssetFolder(this.projectName, this.projectId);
		} else {
			this.projectBaseFolder = AssetUtility.getProjectAppRootFolder(this.projectName, this.projectId);
			this.projectVersionFolder = AssetUtility.getProjectVersionFolder(this.projectName, this.projectId);
			this.projectAssetFolder = AssetUtility.getProjectAssetsFolder(this.projectName, this.projectId);
			this.projectPortalFolder = AssetUtility.getProjectPortalsFolder(this.projectName, this.projectId);
			this.projectNotebookFolder = AssetUtility.getProjectNotebookFolder(this.projectName, this.projectId);
		}

		if (this.smssProp.containsKey(Constants.PROJECT_GIT_PROVIDER)
				&& this.smssProp.containsKey(Constants.PROJECT_GIT_CLONE)) {
			this.projectGitProvider = this.smssProp.getProperty(Constants.PROJECT_GIT_PROVIDER);
			this.projectGitRepo = this.smssProp.getProperty(Constants.PROJECT_GIT_CLONE);
			this.gitProvider = AuthProvider.getProviderFromString(projectGitProvider);

			if (!AssetUtility.isGit(projectVersionFolder)) {
				User user = ThreadStore.getUser();
				String token = null;
				if (user != null && user.getAccessToken(this.gitProvider) != null) {
					token = user.getAccessToken(this.gitProvider).getAccess_token();
				}
				NounMetadata retNoun = GitPushUtils.clone(this.projectVersionFolder, this.projectGitRepo, token,
						this.gitProvider, false);
				if (retNoun.getNounType() == PixelDataType.ERROR) {
					throw new SemossPixelException(retNoun);
				}
			}
		}
		// initialize the default git
		else if (!AssetUtility.isGit(this.projectVersionFolder)) {
			GitRepoUtils.init(this.projectVersionFolder);
		}

		String projectTypeStr = this.smssProp.getProperty(Constants.PROJECT_ENUM_TYPE);
		if (projectTypeStr != null) {
			this.projectType = IProject.PROJECT_TYPE.valueOf(projectTypeStr);
		} else {
			this.projectType = IProject.PROJECT_TYPE.INSIGHTS;
		}

		if (!isAsset) {
			loadInsightsRdbms();
		}

		this.projectProperties = new ProjectProperties(this.projectAssetFolder, this.projectName, this.projectId);

		// load any assets that are already compiled
		this.reactorHelper = new ProjectReactorHelper(this);
		try {
			loadCompiledProjectReactors();
		} catch (Exception e) {
			classLogger.error("Unable to compile project reactors on project initialization for project '{}'",
					SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
		}
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
	 * 
	 * @throws Exception
	 */
	protected void loadInsightsRdbms() throws Exception {
		// load the rdbms insights db
		this.insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(this.smssProp).getAbsolutePath();

		// if it is not defined directly in the smss
		// we will not create an insights database
		if (insightDatabaseLoc != null) {
			this.insightRdbms = ProjectHelper.loadInsightsEngine(this.smssProp, classLogger);
		}
	}

	@Override
	public IRDBMSEngine getInsightDatabase() {
		return this.insightRdbms;
	}

	@Override
	public void setInsightDatabase(IRDBMSEngine insightDatabase) {
		this.insightRdbms = insightDatabase;
	}

	/**
	 * Sets the unique id for the project
	 * 
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
	public Vector<String> getPerspectives() {
		Vector<String> perspectives = Utility.getVectorOfReturn(GET_ALL_PERSPECTIVES_QUERY, insightRdbms, false);
		if (perspectives.contains("")) {
			int index = perspectives.indexOf("");
			perspectives.set(index, "Semoss-Base-Perspective");
		}
		return perspectives;
	}

	@Override
	public Vector<String> getInsights(String perspective) {
		String insightsInPerspective = null;
		if (perspective.equals("Semoss-Base-Perspective")) {
			perspective = null;
		}
		if (perspective != null && !perspective.isEmpty()) {
			insightsInPerspective = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE = '"
					+ perspective + "' ORDER BY QUESTION_ORDER";
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
		if (this.insightRdbms != null) {
			classLogger.debug("closing the insight engine ");
			try {
				this.insightRdbms.close();
			} catch (IOException e) {
				classLogger.error("Error closing insights database for project '{}'",
						SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
			}
		}

		// remove the symbolic link
		if (this.projectId != null && this.projectName != null) {
			String public_home = Utility.getDIHelperProperty(Constants.PUBLIC_HOME);
			if (public_home != null) {
				String fileName = public_home + java.nio.file.FileSystems.getDefault().getSeparator()
						+ SmssUtilities.getUniqueName(this.projectName, this.projectId);
				File file = new File(Utility.normalizePath(fileName));
				try {
					if (file.exists() && Files.isSymbolicLink(Paths.get(Utility.normalizePath(fileName)))) {
						FileUtils.forceDelete(file);
					}
				} catch (IOException e) {
					classLogger.error("Failed to delete project symbolic link at {}", file, e);
				}
			}
		}

		// TODO: do we want to close the py process everything time we close?
		// we close when we push insights (cause of the insights database) or update
		// smss

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
		classLogger.debug("Closing {}", folderName);
		this.close();

		if (this.insightDatabaseLoc != null) {
			File insightFile = new File(this.insightDatabaseLoc);
			if (insightFile.exists()) {
				classLogger.info("Deleting insight file {}", insightFile.getAbsolutePath());
				try {
					FileUtils.forceDelete(insightFile);
				} catch (IOException e) {
					classLogger.error("Failed to delete insight file {}", insightFile.getAbsolutePath(), e);
				}
			}
		}

		// this check is to ensure we are deleting the right folder
		String folderPath = Utility.getDIHelperProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR
				+ Constants.PROJECT_FOLDER + DIR_SEPARATOR + folderName;
		File folder = new File(folderPath);
		if (folder.exists() && folder.isDirectory()) {
			classLogger.debug("Folder getting deleted is {}", folder.getAbsolutePath());
			try {
				FileUtils.deleteDirectory(folder);
			} catch (IOException e) {
				classLogger.error("Failed to delete project folder {}", folder.getAbsolutePath(), e);
			}
		}

		classLogger.debug("Deleting smss {}", this.projectSmssFilePath);
		File smssFile = new File(this.projectSmssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch (IOException e) {
			classLogger.error("Failed to delete project smss file {}", this.projectSmssFilePath, e);
		}

		// remove from DIHelper
		UploadUtilities.removeEngineFromDIHelper(this.projectId);

		// remove from secret store
		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if (secretStore != null) {
			secretStore.deleteEngineSecrets(getCatalogType(), this.projectId, this.projectName);
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
		for (int i = 0; i < numIDs; i++) {
			String id = questionIDs[i];
			try {
				idString = idString + "'" + id + "'";
				if (i != numIDs - 1) {
					idString = idString + ", ";
				}
				counts.add(i);
			} catch (NumberFormatException e) {
				System.err.println(">>>>>>>> FAILED TO GET ANY INSIGHT FOR ARRAY :::::: " + questionIDs[i]);
			}
		}

		if (!idString.isEmpty()) {
			String query = GET_INSIGHT_INFO_QUERY.replace(QUESTION_PARAM_KEY, idString);
			classLogger.info("Running insights query {}", Utility.cleanLogString(query));

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
					if (cacheMinutes == null) {
						cacheMinutes = -1;
					}
					String cacheCron = (String) values[10];
					Boolean cacheEncrypt = (Boolean) values[11];
					if (cacheEncrypt == null) {
						cacheEncrypt = false;
					}
					Object[] pixel = null;
					// need to know if we have an array
					// or a clob
					if (insightRdbms.getQueryUtil().allowArrayDatatype()) {
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
						InputStreamReader reader = new InputStreamReader(
								new ByteArrayInputStream(pixelArray.getBytes()));
						pixel = gson.fromJson(reader, String[].class);
					}

					String perspective = values[3] + "";
					String order = values[5] + "";

					Insight in = null;
					if (pixel == null || pixel.length == 0) {
						in = new OldInsight(this, dataMakerName, layout);
						in.setRdbmsId(rdbmsId);
						in.setInsightName(insightName);
						((OldInsight) in).setOutput(layout);
						((OldInsight) in).setMakeup(insightMakeup);
//						in.setPerspective(perspective);
//						in.setOrder(order);
						((OldInsight) in).setDataTableAlign(dataTableAlign);
						// adding semoss parameters to insight
						((OldInsight) in).setInsightParameters(
								LegacyInsightDatabaseUtility.getParamsFromInsightId(this.insightRdbms, rdbmsId));
						in.setIsOldInsight(true);
					} else {
						in = new Insight(this.projectId, this.projectName, rdbmsId, cacheable, cacheMinutes, cacheCron,
								cacheEncrypt, pixel.length);
						in.setInsightName(insightName);
						List<String> pixelList = new Vector<String>(pixel.length);
						for (int i = 0; i < pixel.length; i++) {
							String pixelString = pixel[i].toString();
							List<String> breakdown;
							try {
								breakdown = PixelUtility.parsePixel(pixelString);
								pixelList.addAll(breakdown);
							} catch (ParserException | LexerException | IOException e) {
								classLogger.error("Error parsing pixel expression '{}' for insight id '{}'",
										pixelString, rdbmsId, e);
								throw new IllegalArgumentException("Error occurred parsing the pixel expression");
							}
						}
						in.setPixelRecipe(pixelList);
					}
					insightV.insertElementAt(in, counts.remove(0));
				}
			} catch (IllegalArgumentException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error("Failed to retrieve insights for query IDs {}", idString, e);
			} finally {
				if (wrap != null) {
					try {
						wrap.close();
					} catch (IOException e) {
						classLogger.error("Failed to close insight query wrapper for query IDs {}", idString, e);
					}
				}
			}
		}
		return insightV;
	}

	@Override
	public String getInsightDefinition() {
		StringBuilder stringBuilder = new StringBuilder();
		// call script command to get everything necessary to recreate rdbms engine on
		// the other side//
		ISelectWrapper wrap = null;
		try {
			wrap = WrapperManager.getInstance().getSWrapper(insightRdbms, "SCRIPT");
			String[] names = wrap.getVariables();
			while (wrap.hasNext()) {
				ISelectStatement ss = wrap.next();
				stringBuilder.append(ss.getVar(names[0]) + "").append("%!%");
			}
		} catch (Exception e) {
			classLogger.error("Failed to generate insight definition script for project '{}'",
					SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
		} finally {
			if (wrap != null) {
				try {
					wrap.close();
				} catch (IOException e) {
					classLogger.error("Failed to close SCRIPT wrapper while generating insight definition", e);
				}
			}
		}
		return stringBuilder.toString();
	}

	/**
	 * 
	 */
	@Override
	public void compileReactors() {
		File javaDirectory = new File(this.projectAssetFolder + "/java");

		// if there is no java.. dont even bother with this
		// no need to spend time on any of this
		if (!javaDirectory.exists()) {
			return;
		}

		String classesFolder = this.projectAssetFolder + "/classes";
		File classesDir = new File(classesFolder);
		// delete the existing classes folder if it exists
		// so we know the reactor files are fresh
		if (classesDir.exists() && classesDir.isDirectory()) {
			try {
				FileUtils.cleanDirectory(classesDir);
				classesDir.mkdir();
			} catch (Exception e) {
				classLogger.error("Failed to clean project classes directory {}", classesDir.getAbsolutePath(), e);
			}
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

		if (loadJars) {
			compileReactorFromJars(jars);
		} else if (hasPom) {
			compileReactorsFromPom(pomFile);
		} else {
			compileReactorsFromJavaFiles();
		}

		this.lastReactorCompilationDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
		classLogger.info("Project '{}' has new last compilation date {}", projectId, this.lastReactorCompilationDate);
	}

	/**
	 * 
	 */
	private void compileReactorsFromJavaFiles() {
		// have the classes been loaded already?
		if (ProjectCustomReactorCompilator.needsCompilation(this.projectId)) {
			int status = Utility.compileJava(this.projectAssetFolder, getCP());
			if (status == 0) {
				ProjectCustomReactorCompilator.setCompiled(this.projectId);
			} else {
				ProjectCustomReactorCompilator.setFailed(this.projectId);
			}

			this.projectSpecificHash = this.reactorHelper.loadReactors(this.projectAssetFolder);
		}
	}

	/**
	 * 
	 * @param pomFile
	 */
	private void compileReactorsFromPom(File pomFile) {
		if (evalMvnReload()) {
			this.reactorHelper.makeMvnClassloader(pomFile);
			if (!this.reactorHelper.isMvnDefined()) {
				// no point none of the stuff is set anyways
				return;
			}
			// try to load it directly from assets
			String targetFolder = getTargetFolder(pomFile);
			// target folder is relative to java folder for the main assets
			targetFolder = targetFolder + DIR_SEPARATOR + "classes";
			this.projectSpecificHash = this.reactorHelper.loadReactorsFromPom(pomFile.getParent(), targetFolder);
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
		try {
			if (projectSpecificHash != null && projectSpecificHash.containsKey(className.toUpperCase())) {
				Class<IReactor> thisReactorClass = projectSpecificHash.get(className.toUpperCase());
				retReac = thisReactorClass.getDeclaredConstructor().newInstance();
				return retReac;
			}
		} catch (Exception e) {
			classLogger.error("Failed to instantiate reactor '{}' from project pom for project '{}'", className,
					SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
		}
		return retReac;
	}

	/**
	 * 
	 * @param jars
	 */
	private void compileReactorFromJars(File[] jars) {
		// have the classes been loaded already?
		if (ProjectCustomReactorCompilator.needsCompilation(this.projectId)) {
			URL[] urls = new URL[jars.length];
			for (int i = 0; i < jars.length; i++) {
				try {
					urls[i] = jars[i].toURI().toURL();
				} catch (MalformedURLException e) {
					classLogger.error("Unable to resolve jar URL for project reactor jar '{}'", jars[i].getName(), e);
					throw new IllegalArgumentException("Unable to load jar file : " + jars[i].getName());
				}
			}
			projectSpecificHash = this.reactorHelper.loadReactorsFromJars(urls);
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
			if (projectSpecificHash.containsKey(className.toUpperCase())) {
				Class<IReactor> thisReactorClass = projectSpecificHash.get(className.toUpperCase());
				retReac = thisReactorClass.getDeclaredConstructor().newInstance();
			}
		} catch (Exception e) {
			classLogger.error("Failed to instantiate reactor '{}' from project jars for project '{}'", className,
					SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
		}

		return retReac;
	}

	@Override
	public void clearClassCache() {
		// clear the local hash
		if (projectSpecificHash != null) {
			this.projectSpecificHash.clear();
		}
		// recompile within reactor factory
		ProjectCustomReactorCompilator.reset(this.projectId);
		File mvnDepFile = new File(this.projectBaseFolder + DIR_SEPARATOR + "mvn_dep.output");
		// delete the maven dep file
		if (mvnDepFile.exists()) {
			mvnDepFile.delete();
		}

		this.reactorHelper.close();
	}

	/**
	 * 
	 * @return
	 */
	private boolean evalMvnReload() {
		// need to see if the mvn_dependency file is older than target
		// if so reload
		File classesDir = new File(this.projectBaseFolder + DIR_SEPARATOR + "target");
		File mvnDepFile = new File(this.projectBaseFolder + DIR_SEPARATOR + "mvn_dep.output");

		if (!mvnDepFile.exists()) {
			return true;
		}

		if (!classesDir.exists()) {
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
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			// Use this if the JAXP parser accepts it
			dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			// AND add the following to enforce limits on what the parser is allowed to do
			dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
			dbf.setXIncludeAware(false);
			dbf.setExpandEntityReferences(false);
			DocumentBuilder builder = dbf.newDocumentBuilder();

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
			classLogger.error("Failed to evaluate Maven target folder from pom file {}", pomFile.getAbsolutePath(), e);
		}
		return targetFolder;
	}

	/**
	 * load any existing reactor class files as is
	 */
	private void loadCompiledProjectReactors() {
		File javaDirectory = new File(this.projectAssetFolder + DIR_SEPARATOR + "java");

		File[] jars = javaDirectory.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		});
		File pomFile = new File(javaDirectory.getAbsolutePath() + DIR_SEPARATOR + "pom.xml");

		boolean loadJars = jars != null && jars.length > 0;
		boolean hasPom = pomFile.exists() && pomFile.isFile();

		if (hasPom) {
			// this is maven

			// TODO: need to figure out how we see if maven is already compiled and exists
			// TODO: need to figure out how we see if maven is already compiled and exists
			// TODO: need to figure out how we see if maven is already compiled and exists
			// TODO: need to figure out how we see if maven is already compiled and exists

		} else if (loadJars) {
			// not really a compile, but loading the reactors from the jars
			compileReactorFromJars(jars);
		}
		// load from existing classes folder - might be outdated but only doing this
		// when the project is first loaded
		else {
			String classesFolder = this.projectAssetFolder + "/classes";
			File classesDir = new File(classesFolder);
			if (classesDir.exists() && classesDir.isDirectory()) {
				this.projectSpecificHash = this.reactorHelper.loadReactors(this.projectAssetFolder);
				if (this.projectSpecificHash != null && !this.projectSpecificHash.isEmpty()) {
					ProjectCustomReactorCompilator.setCompiled(this.projectId);
					lastReactorCompilationDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
				}
			}
		}
	}

	@Override
	public IReactor getReactor(String className) {
		SemossDate lastCompiledDateInSecurity = SecurityProjectUtils.getReactorCompilationTimestamp(this.projectId);
		boolean outOfDate = false;
		if (lastCompiledDateInSecurity != null && this.lastReactorCompilationDate != null) {
			outOfDate = lastCompiledDateInSecurity.getLocalDateTime()
					.isAfter(this.lastReactorCompilationDate.getLocalDateTime());
		}
		// just pull to make sure we have the latest in case project was loaded
		// but not published
		if (outOfDate || this.lastReactorCompilationDate == null) {
			classLogger.info(
					"Pulling Java folder for project {}. Current reactors out of date = {}. Last compilation date = {}",
					this.projectId, outOfDate, this.lastReactorCompilationDate);
			ClusterUtil.pullProjectFolder(this, this.projectVersionFolder, Constants.ASSETS_FOLDER + "/" + "java");
			this.clearClassCache();
		}

		IReactor retReac = null;
		// if we are not out of date, we can see if this exists
		if (!outOfDate && this.lastReactorCompilationDate != null && projectSpecificHash != null) {
			try {
				if (projectSpecificHash != null && projectSpecificHash.containsKey(className.toUpperCase())) {
					Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
					retReac = (IReactor) thisReactorClass.getDeclaredConstructor().newInstance();
				}
			} catch (Exception e) {
				classLogger.error("Failed to instantiate cached reactor '{}' for project '{}'", className,
						SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
			}
		} else {
			// else we will see if we have java
			File javaDirectory = new File(this.projectAssetFolder + DIR_SEPARATOR + "java");

			// if there is no java.. dont even bother with this
			// no need to spend time on any of this
			if (!javaDirectory.exists()) {
				// dont need to keep setting this
				if (this.lastReactorCompilationDate == null) {
					this.lastReactorCompilationDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
					classLogger.info("Project '{}' does not have a Java folder. Setting last compilation date to {}",
							projectId, this.lastReactorCompilationDate);
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

			if (loadJars) {
				retReac = getReactorFromJars(className, jars);
			} else if (hasPom) {
				retReac = getReactorsFromPom(className, pomFile);
			} else {
				compileReactorsFromJavaFiles();
				try {
					if (projectSpecificHash != null && projectSpecificHash.containsKey(className.toUpperCase())) {
						Class thisReactorClass = projectSpecificHash.get(className.toUpperCase());
						retReac = (IReactor) thisReactorClass.getDeclaredConstructor().newInstance();
					}
				} catch (Exception e) {
					classLogger.error("Failed to instantiate reactor '{}' from project java source", className, e);
				}
			}

			this.lastReactorCompilationDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
			classLogger.info("Project '{}' has new last compilation date {}", projectId,
					this.lastReactorCompilationDate);
		}

		boolean useNettyPy = Utility.getDIHelperProperty(Constants.NETTY_PYTHON) != null
				&& Utility.getDIHelperProperty(Constants.NETTY_PYTHON).equalsIgnoreCase("true");
		if (!useNettyPy) {
			return retReac;
		}

		// secondary check to execute reactor here
		if (executeReactorOnSocket() && ((Utility.getDIHelperLocalProperty("core") == null
				|| Utility.getDIHelperLocalProperty("core").toString().equalsIgnoreCase("true")) && retReac != null)) {

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
		if (this.projectSpecificHash == null) {
			return new TreeSet<>();
		}

		return new TreeSet<>(this.projectSpecificHash.keySet());
	}

	private boolean executeReactorOnSocket() {
		if (this.execReactorOnSocket == null) {
			this.execReactorOnSocket = (Utility.getDIHelperProperty(Settings.CUSTOM_REACTOR_EXECUTION) != null)
					&& (Utility.getDIHelperProperty(Settings.CUSTOM_REACTOR_EXECUTION).toString()
							.equalsIgnoreCase("SOCKET"));
		}
		return execReactorOnSocket;
	}

	/**
	 * 
	 * @return
	 */
	private String getCP() {
		String envClassPath = null;
		StringBuilder retClassPath = new StringBuilder("");
		ClassLoader cl = getClass().getClassLoader();

		// Use LinkedHashSet to maintain order and avoid duplicates
		Set<String> classpathEntries = new LinkedHashSet<>();
		URL[] urls = ((URLClassLoader) cl).getURLs();
		String separator = ":";
		if (System.getProperty("os.name").toLowerCase().contains("win")) {
			separator = ";";
		}

		for (URL url : urls) {
			String thisURL = URLDecoder.decode(url.getFile(), StandardCharsets.UTF_8);
			File thisFile = new File(thisURL);

			if (thisFile.isFile()) {
				// If it's a JAR file, add it directly
				if (thisURL.endsWith(".jar")) {
					classpathEntries.add(thisURL);
				} else {
					// For other files, add the parent directory
					Path filePath = Paths.get(thisURL);
					Path parentPath = filePath.getParent();
					if (parentPath != null) {
						String parentDir = parentPath.toFile().getAbsolutePath();
						// Remove trailing slash/backslash
						if (parentDir.endsWith("/") || parentDir.endsWith("\\")) {
							parentDir = parentDir.substring(0, parentDir.length() - 1);
						}
						classpathEntries.add(parentDir);
					}
				}
			} else if (thisFile.isDirectory()) {
				// For directories, add the directory itself
				String dirPath = thisURL;
				// Remove trailing slash/backslash
				if (dirPath.endsWith("/") || dirPath.endsWith("\\")) {
					dirPath = dirPath.substring(0, dirPath.length() - 1);
				}
				classpathEntries.add(dirPath);
			}
		}

		// Build the final classpath string
		boolean appendSep = false;
		for (String entry : classpathEntries) {
			if (appendSep) {
				retClassPath.append(separator);
			}
			retClassPath.append(entry);
			appendSep = true;
		}

		envClassPath = "\"" + retClassPath.toString() + "\"";
		return envClassPath;
	}

	@Override
	public boolean requirePublish(boolean pullFromCloud) {
		// check in security DB when we last published
		SemossDate lastPublishedDateInSecurity = SecurityProjectUtils.getPortalPublishedTimestamp(this.projectId);
		boolean outOfDate = false;
		if (lastPublishedDateInSecurity != null && this.lastPortalPublishDate != null) {
			outOfDate = lastPublishedDateInSecurity.getZonedDateTime()
					.isAfter(this.lastPortalPublishDate.getZonedDateTime());
		}
		if (outOfDate || this.lastPortalPublishDate == null) {
			// just pull to make sure we have the latest in case project was loaded
			// but not published
			if (pullFromCloud) {
				classLogger.info(
						"Pulling Portals folder for project {}. Current portal out of date = {}. Last portal publish date = {}",
						this.projectId, outOfDate, this.lastPortalPublishDate);
				ClusterUtil.pullProjectFolder(this, this.projectPortalFolder);
			}
		}

		// if this are true we want to republish
		// we just add the additional logic above if we have to pull from cloud
		return this.republishPortal || outOfDate || !this.publishedPortal;
	}

	@Override
	/**
	 * Publish the portals folder to public_home
	 */
	// TODO: HAVE TO ADD SYNCHONIZED UNTIL DATES ARE RESOLVED
	public synchronized boolean publish(String publicHomeFilePath, boolean pullFromCloud) {
		if (publicHomeFilePath == null) {
			return false;
		}

		// find what is the final URL
		// this is the base url plus manipulations
		// find what the tomcat deploy directory is
		// no easy way to find other than may be find the classpath ? - will instrument
		// this through RDF Map
		boolean requirePublish = requirePublish(pullFromCloud);
		try {
			if (requirePublish) {
				Path sourcePortalsProjectPath = Paths.get(this.projectPortalFolder);
				Path targetPublicHomeProjectPortalsPath = Paths.get(
						publicHomeFilePath + DIR_SEPARATOR + this.projectId + DIR_SEPARATOR + Constants.PORTALS_FOLDER);

				File targetPublicHomeProjectPortalsDir = targetPublicHomeProjectPortalsPath.toFile();
				// if the target directory exists
				// we have to delete it before
				if (targetPublicHomeProjectPortalsDir.exists() && targetPublicHomeProjectPortalsDir.isDirectory()) {
					FileUtils.deleteDirectory(targetPublicHomeProjectPortalsDir);
				}

				rewritePortalIndexHtml(this.projectPortalFolder + DIR_SEPARATOR + "index.html");

				// do we physically copy of link?
				// first smss file
				// second rdf map
				boolean copy = true;
				if (smssProp != null && smssProp.getProperty(Settings.COPY_PROJECT) != null) {
					copy = Boolean.parseBoolean(smssProp.getProperty(Settings.COPY_PROJECT) + "");
				} else if (Utility.getDIHelperProperty(Settings.COPY_PROJECT) != null) {
					copy = Boolean.parseBoolean(Utility.getDIHelperProperty(Settings.COPY_PROJECT) + "");
				}

				// this is purely for testing purposes - this is because when eclipse publishes
				// it wipes the directory and removes the actual db
				if (copy) {
					if (!targetPublicHomeProjectPortalsDir.exists()) {
						targetPublicHomeProjectPortalsDir.mkdir();
					}
					FileUtils.copyDirectory(sourcePortalsProjectPath.toFile(), targetPublicHomeProjectPortalsDir);
				}
				// this is where we create symbolic link
				else if (!targetPublicHomeProjectPortalsDir.exists()
						&& !Files.isSymbolicLink(targetPublicHomeProjectPortalsPath)) {
					Files.createSymbolicLink(targetPublicHomeProjectPortalsPath, sourcePortalsProjectPath);
				}
				targetPublicHomeProjectPortalsDir.deleteOnExit();
				this.publishedPortal = true;
				this.republishPortal = false;
				this.lastPortalPublishDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
				classLogger.info("Project '{}' has new last portal published date {}",
						SmssUtilities.getUniqueName(this.projectName, this.projectId), this.lastPortalPublishDate);
			}
		} catch (Exception e) {
			classLogger.error("Failed to publish portals for project '{}'",
					SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
			this.publishedPortal = false;
			this.lastPortalPublishDate = null;
		}

		return this.publishedPortal;
	}

	@Override
	public INotebookHelper getNotebookHelper() {
		// if not blocks json
		// then ignore for now
		File blocksF = getBlocksF();
		if (!blocksF.exists() || !blocksF.isFile()) {
			return null;
		}

		try {
			return NotebookHelperFactory.getNotebookHelper(blocksF);
		} catch (IOException e) {
			classLogger.error("Failed to load notebook helper from {}", blocksF.getAbsolutePath(), e);
		}

		return null;
	}

	@Override
	public synchronized List<File> writeNotebooks() {
		File blocksF = getBlocksF();
		if (!blocksF.exists() || !blocksF.isFile()) {
			return null;
		}

		File projectNotebookF = new File(this.projectNotebookFolder);
		if (!projectNotebookF.exists() || !projectNotebookF.isDirectory()) {
			projectNotebookF.mkdirs();
		}

		try {
			INotebookBuilder builder = NotebookWriterFactory.getNotebookBuilder(blocksF);
			return builder.createNotebooks(projectNotebookF);
		} catch (IOException e) {
			classLogger.error("Failed to write notebooks from {} into {}", blocksF.getAbsolutePath(),
					projectNotebookF.getAbsolutePath(), e);
		} finally {
			ClusterUtil.pushProjectFolder(this, this.projectNotebookFolder);
		}

		return null;
	}

	@Override
	public NotebookExecution executeNotebooks(Insight insight, Map<String, String> inputReplacements) {
		// if not blocks json
		// then ignore for now
		File blocksF = getBlocksF();
		if (!blocksF.exists() || !blocksF.isFile()) {
			return null;
		}

		try {
			INotebookHelper helper = NotebookHelperFactory.getNotebookHelper(blocksF);
			return helper.executeNotebook(insight, inputReplacements);
		} catch (IOException e) {
			classLogger.error("Failed to execute notebooks from {}", blocksF.getAbsolutePath(), e);
		}

		return null;
	}

	@Override
	public Map<String, String> getEngineDependencies() {
		File blocksF = getBlocksF();
		if (!blocksF.exists() || !blocksF.isFile()) {
			return null;
		}

		try {
			INotebookHelper helper = NotebookHelperFactory.getNotebookHelper(blocksF);
			Map<String, String> engineMap = helper.getBlocksEngineDependencies();
			return engineMap;
		} catch (IOException e) {
			classLogger.error("Failed to read engine dependencies from notebook blocks file {}",
					blocksF.getAbsolutePath(), e);
		}

		return null;
	}

	public Map<String, String> getNotebookVariables() {
		File blocksF = getBlocksF();
		if (!blocksF.exists() || !blocksF.isFile()) {
			return null;
		}

		try {
			INotebookHelper helper = NotebookHelperFactory.getNotebookHelper(blocksF);
			Map<String, String> engineMap = helper.getNotebookVariables();
			return engineMap;
		} catch (IOException e) {
			classLogger.error("Failed to read notebook variables from {}", blocksF.getAbsolutePath(), e);
		}

		return null;
	}

	private File getBlocksF() {
		// Try blocks.ipynb first (notebook apps), then fall back to blocks.json
		String ipynbFilePath = this.projectPortalFolder + "/" + IProject.NOTEBOOK_IPYNB_FILE_NAME;
		File ipynbF = new File(ipynbFilePath);
		if (ipynbF.exists() && ipynbF.isFile()) {
			return ipynbF;
		}
		String blocksFilePath = this.projectPortalFolder + "/" + IProject.BLOCK_FILE_NAME;
		File blocksF = new File(blocksFilePath);
		return blocksF;
	}

	private void rewritePortalIndexHtml(String indexHtmlPath) {
		/*
		 * <script> window.SEMOSS = { "APP": "<project_id>", "MODULE":
		 * "/{route - optional}/{context - usually just Monolith}" } </script>
		 */
		// add the route if this is server deployment
		File indexHtmlF = new File(indexHtmlPath);
		if (!indexHtmlF.exists() || !indexHtmlF.isFile()) {
			return;
		}

		String module = Utility.getApplicationRouteAndContextPath();
		org.jsoup.nodes.Document document;
		try {
			document = Jsoup.parse(indexHtmlF, "UTF-8");
			String scriptContent = "{\"APP\": \"" + projectId + "\",\"MODULE\": \"" + module + "\"}";
			Element autoGenScript = document.getElementById(PORTAL_INDEX_SCRIPT_ID);
			if (autoGenScript == null) {
				document.selectFirst("head").child(0).before("<script id=\"" + PORTAL_INDEX_SCRIPT_ID
						+ "\" type=\"application/json\">" + scriptContent + "</script>");
			} else {
				autoGenScript.html(scriptContent);
			}

			String newHtml = document.html();
			try (FileWriter fw = new FileWriter(indexHtmlF, false)) {
				fw.write(newHtml);
				fw.flush();
			}
		} catch (Exception e) {
			classLogger.error("Failed to rewrite portal index html {}", indexHtmlF.getAbsolutePath(), e);
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
		if (!create) {
			return this.cpw.getSocketClient();
		}

		if (this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return this.cpw.getSocketClient();
		}

		createProjectTcpServer(port);
		return this.cpw.getSocketClient();
	}

	/**
	 * 
	 * @return
	 */
	@Override
	public TCPRTranslator getProjectRTranslator() {
		if (this.cpw.getSocketClient() == null) {
			createProjectTcpServer(-1);
		} else if (!this.cpw.getSocketClient().isConnected()) {
			this.cpw.shutdown(false);
			try {
				this.cpw.reconnect();
			} catch (Exception e) {
				classLogger.error("Failed to reconnect TCP client for project '{}'",
						SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
				throw new IllegalArgumentException("Failed to start TCP Server for Project = "
						+ SmssUtilities.getUniqueName(this.projectName, this.projectId));
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
	@Override
	public PyTranslator getProjectPyTranslator() {
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.createProjectTcpServer(-1);
		}
		this.pyTranslator.setSocketClient(this.cpw.getSocketClient());
		return this.pyTranslator;
	}

	/**
	 * 
	 */
	private synchronized void createProjectTcpServer(int port) {
		if (this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
		if (this.workingDirectoryBasePath == null) {
			this.createCacheFolder();
		}

		// check if we have already created a process wrapper
		ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
		if (this.cpw != null) {
			this.cpw.shutdown(false);
		}

		String timeout = "30";
		if (this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
			timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
		}
		if (cpwToInit.getSocketClient() == null) {
			boolean debug = false;

			// pull the relevant values from the smss
			String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
			String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
			String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "INFO");
			String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
			String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;

			if (port < 0) {
				// port has not been forced
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						// ignore
						classLogger.warn("Project {} has an invalid FORCE_PORT value '{}'", this.getEngineName(),
								forcePort);
					}
				}
			}

			String serverDirectory = this.cacheFolder.getAbsolutePath();
			boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from
											// python
			try {
				cpwToInit.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath,
						debug, timeout, loggerLevel);
			} catch (Exception e) {
				classLogger.error("Unable to create project TCP process/client for project '{}'",
						SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
				throw new IllegalArgumentException("Unable to connect to server for python model engine.");
			}
		} else if (!cpwToInit.getSocketClient().isConnected()) {
			cpwToInit.shutdown(false);
			try {
				cpwToInit.reconnect();
			} catch (Exception e) {
				classLogger.error("Unable to reconnect project TCP process/client for project '{}'",
						SmssUtilities.getUniqueName(this.projectName, this.projectId), e);
				throw new IllegalArgumentException(
						"Failed to start TCP Server for Python Model Engine = " + this.getEngineName());
			}
		}

		// create the py translator
		Insight processInsight = new Insight();
		processInsight.setContextProjectId(this.projectId);
		processInsight.setContextProjectName(this.projectName);
		InsightStore.getInstance().put(processInsight);
		this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);
		// finally set the cpw in the class
		this.cpw = cpwToInit;
	}

	/**
	 * 
	 */
	private void createCacheFolder() {
		String engineId = this.getEngineId();

		if (engineId == null || engineId.isEmpty()) {
			engineId = "";
		}
		// create a generic folder
		this.workingDirectory = "PROJECT_" + engineId + "_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + this.workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);

		// make the folder if one does not exist
		if (!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
	}

	@Override
	public String getCompileOutput() {
		String finalOutput = null;
		try {
			String compilerOutput = AssetUtility.getProjectAssetsFolder(this.projectId) + "/classes/compileerror.out";
			File file = new File(compilerOutput);
			if (file.exists()) {
				finalOutput = FileUtils.readFileToString(new File(compilerOutput), StandardCharsets.UTF_8);
			}
		} catch (IOException e) {
			classLogger.error("Failed to read compile output for project '{}'", this.projectId, e);
		}

		return finalOutput;
	}

	@Override
	public Logger getEngineLogger(String loggerName) {
		if (this.engineSpecificLoggerCtx != null) {
			return this.engineSpecificLoggerCtx.getLogger(loggerName);
		}

		File log4j2 = new File(this.projectAssetFolder + "log4j2.xml");
		if (!log4j2.exists() || !log4j2.isFile()) {
			return null;
		}

		if (engineSpecificLoggerCtx == null) {
			synchronized (this) {
				if (engineSpecificLoggerCtx == null) {
					ClassLoader isolatedLoader = new URLClassLoader(new URL[0], null);
					engineSpecificLoggerCtx = Configurator.initialize(this.projectId, isolatedLoader,
							"file:" + log4j2.getAbsolutePath());
				}
			}
		}

		return this.engineSpecificLoggerCtx.getLogger(loggerName);
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

	@Override
	public boolean isBasic() {
		return false;
	}

	@Override
	public void setBasic(boolean isBasic) {
		// always false
	}

	@Override
	public boolean keepInputOutput() {
		return true;
	}

	@Override
	public boolean isMCPEnabled() {
		return true;
	}

	private IMCP getProjectMCP() {
		if (this.projectMCP == null) {
			String endpoint = this.smssProp.getProperty(MCP_ENDPOINT);
			if (endpoint != null && !endpoint.isBlank()) {
				this.projectMCP = new RemoteMCP(endpoint);
			} else {
				this.projectMCP = new InternalMCP(this);
			}
		}
		return this.projectMCP;
	}

	@Override
	public JSONObject initMCP(String protocolVersion) {
		return getProjectMCP().initMCP(protocolVersion);
	}

	@Override
	public JSONObject getMCPResources() {
		return getProjectMCP().getMCPResources();
	}

	@Override
	public JSONObject getMCPResourcesTemplates() {
		return getProjectMCP().getMCPResourcesTemplates();
	}

	@Override
	public JSONObject getMCPPrompts() {
		return getProjectMCP().getMCPPrompts();
	}

	@Override
	public JSONObject getMCPTools() {
		return getProjectMCP().getMCPTools();
	}

	@Override
	public Object callTool(String toolName, Map<String, Object> params, Insight insight) {
		return getProjectMCP().callTool(toolName, params, insight);
	}

	@Override
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	@Override
	public String getDisplayName() {
		return (this.displayName != null && !this.displayName.trim().isEmpty()) ? this.displayName : this.projectName;
	}

}
