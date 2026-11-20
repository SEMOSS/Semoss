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
package prerna.project.api;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.date.SemossDate;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.EngineAccess;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.IgnoreEngineLogging;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.project.impl.ProjectProperties;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.reactor.IReactor;
import prerna.reactor.frame.r.util.TCPRTranslator;
import prerna.sablecc2.NotebookExecution;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.openshell.OpenShellSecrets;

/**
 * Contract for SEMOSS project engines and their project-scoped resources.
 *
 * <p>
 * A project owns its metadata, assets, insights, reactors, and optional
 * Python/R execution context. Specialized project types may add stricter
 * creation and lifecycle requirements.
 */
public interface IProject extends IEngine, IMCP {

	String MCP_ENDPOINT = "MCP_ENDPOINT";
	String MCP_AUTH_SCHEME = "MCP_AUTH_SCHEME";
	String MCP_AUTH_TOKEN = "MCP_AUTH_TOKEN";

	String DEPENDENCIES_FILE_SUFFIX = "_dependencies.json";
	String BLOCK_FILE_NAME = "blocks.json";
	String NOTEBOOK_FOLDER = ".notebooks";

	/**
	 * Project capabilities used to select the correct creation and lifecycle path.
	 */
	enum PROJECT_TYPE {
		BLOCKS, CODE, WORKSPACE, SKILL, INSIGHTS, NOTEBOOK, AUTOMATION;

		/**
		 * Whether a project of this type holds insights, and so has an insights
		 * database. Only an INSIGHTS project does.
		 *
		 * @return true for INSIGHTS
		 */
		public boolean holdsInsights() {
			return this == INSIGHTS;
		}

		/**
		 * Read a project's type from its smss PROJECT_ENUM_TYPE. A project whose smss
		 * does not set it is an INSIGHTS project.
		 *
		 * @param smssProp the project smss properties
		 * @return the project type
		 * @throws IllegalArgumentException when PROJECT_ENUM_TYPE is not a project type
		 */
		public static PROJECT_TYPE fromSmss(Properties smssProp) {
			String projectType = smssProp.getProperty(Constants.PROJECT_ENUM_TYPE);
			if (projectType == null || projectType.isBlank()) {
				return INSIGHTS;
			}
			return valueOf(projectType.trim());
		}
	};

	/**
	 * Sets the unique id for the project
	 * 
	 * @param projectId - id to set the project
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setProjectId(String projectId);

	/**
	 * Get the project id
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getProjectId();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	IProject.PROJECT_TYPE getProjectType();

	/**
	 * Sets the name for the project
	 * 
	 * @param projectName - name of the project
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setProjectName(String projectName);

	/**
	 * Get the project name
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getProjectName();

	// gets the perspectives for this engine
	// REFAC: Not sure we need this anymore
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	Vector<String> getPerspectives();

	// gets the questions for a given perspective
	// REFAC: Not sure we need this anymore
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	Vector<String> getInsights(String perspective);

	// get all the insights irrespective of perspective
	// REFAC: Not sure we need this anymore
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	Vector<String> getInsights();

	// get the insight for a given question description
	// REFAC: Not sure we need this anymore - we can do this where id is null
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	Vector<Insight> getInsight(String... id);

	/**
	 * Get the insights database. A project has one only when its type holds
	 * insights and it is not a user asset project.
	 *
	 * @return the insights database, or null when the project has none
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.EDIT)
	IRDBMSEngine getInsightDatabase();

	/**
	 * Set the insight database
	 * 
	 * @param insightDatabase
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.EDIT, projectCode = AccessPermissionEnum.OWNER)
	void setInsightDatabase(IRDBMSEngine insightDatabase);

	/**
	 * Get a string representation of the insights database
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getInsightDefinition();

	/**
	 * Compile the project specific reactors
	 */
	@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.EDIT)
	void compileReactors();

	/**
	 * Get project specific reactor
	 * 
	 * @param reactorName
	 * @param loader
	 * @return
	 */
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	IReactor getReactor(String reactorName);

	/**
	 * Get an ordered set of the reactor names
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	TreeSet<String> getAvailableReactors();

	// publish the engine assets to a specific location
	// once published the assets in this app are available as a public_home from the
	// browser
	// this is useful to access javascript etc.
	// to enable this - you need to put the property public_home_enable on the smss
	// file

	/**
	 * See if we need to republish. If requested pull from cloud
	 * 
	 * @param pullFromCloud
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean requirePublish(boolean pullFromCloud);

	/**
	 * 
	 * @param location
	 * @param pullFromCloud
	 * @return
	 */
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean publish(String location, boolean pullFromCloud);

	/**
	 * 
	 * @param republish
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.EDIT)
	void setRepublish(boolean republish);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean isPublished();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	SemossDate getLastPublishDate();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.EDIT)
	List<File> writeNotebooks();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	INotebookHelper getNotebookHelper();

	/**
	 * 
	 * @param insight
	 * @param inputReplacements
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	NotebookExecution executeNotebooks(Insight insight, Map<String, String> inputReplacements);

	/**
	 * Gets only engine deps listed in the project
	 * 
	 * @return Map of the variable name to the engine id
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	Map<String, String> getEngineDependencies();

	/**
	 * Return if an asset
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean isAsset();

	/**
	 * Get the project properties
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@Deprecated
	@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.EDIT)
	ProjectProperties getProjectProperties();

	/**
	 * Get the project git provider
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getProjectGitProvider();

	/**
	 * Get the project git repository URL
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getProjectGitRepo();

	/**
	 * Get the project git provider
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	AuthProvider getGitProvider();

	/**
	 * Clears the class cache
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.EDIT)
	void clearClassCache();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.EDIT)
	ClientProcessWrapper getClientProcessWrapper();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.EDIT)
	SocketClient getProjectTcpClient();

	/**
	 * 
	 * @param create
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.EDIT)
	SocketClient getProjectTcpClient(boolean create);

	/**
	 * 
	 * @param create
	 * @param port
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.EDIT)
	SocketClient getProjectTcpClient(boolean create, int port);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.EDIT)
	TCPRTranslator getProjectRTranslator();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.EDIT)
	PyTranslator getProjectPyTranslator();

	/**
	 * Clears the project modules cached in the project's dedicated python process
	 * by running {@code smss_clear_app_imports()} with the project asset paths, so
	 * the next import reads the current .py files. The process keeps running, and a
	 * process that is not running is not started.
	 *
	 * @return the result of {@code smss_clear_app_imports()}, a map of the cleared
	 *         module keys and MCP aliases, or null when the project python process
	 *         is not running
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.EDIT)
	Object clearPythonImports();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getCompileOutput();

	/**
	 * Drops any cached MCP handler so the next MCP call is rebuilt from the current
	 * smss properties. Call this after changing {@link #MCP_ENDPOINT} or its auth
	 * properties. No-op for implementations that do not cache a handler.
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	default void resetMCP() {
		// no-op
	}

	/**
	 * @return the url of the external MCP server this project delegates to, or null
	 *         when the project serves its own generated tools
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	default String getRemoteMCPEndpoint() {
		return null;
	}

	/**
	 * There is deliberately no getter for the matching auth token. The credential
	 * stays on the server, and callers that need to show it report
	 * {@link prerna.util.Constants#SENSITIVE_INFO_MASK} instead.
	 *
	 * @return the authentication scheme sent to the external MCP server, such as
	 *         Bearer or Basic, or null when none is configured
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	default String getRemoteMCPAuthScheme() {
		return null;
	}

	/**
	 * The project's sandbox secrets: its {@link OpenShellSecrets#SECRETS_PROP}
	 * declaration and each {@link OpenShellSecrets#VALUE_PROP_PREFIX} value, and no
	 * other setting. The platform hands them to the project's sandbox for everyone
	 * who can open the project; a project's custom code reads them only for its
	 * owners, as it does the rest of its settings.
	 *
	 * @return a copy holding only the sandbox secret settings, empty when there are
	 *         none
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.READ_ONLY, projectCode = AccessPermissionEnum.OWNER)
	default Properties getSandboxSecretSettings() {
		Properties sandboxSecrets = new Properties();
		Properties smssProp = getSmssProp();
		if (smssProp == null) {
			return sandboxSecrets;
		}
		for (String key : smssProp.stringPropertyNames()) {
			if (key.equals(OpenShellSecrets.SECRETS_PROP) || key.startsWith(OpenShellSecrets.VALUE_PROP_PREFIX)) {
				sandboxSecrets.setProperty(key, smssProp.getProperty(key));
			}
		}
		return sandboxSecrets;
	}

}
