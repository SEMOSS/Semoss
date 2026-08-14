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
import java.util.TreeSet;
import java.util.Vector;

import prerna.auth.AuthProvider;
import prerna.date.SemossDate;
import prerna.ds.py.PyTranslator;
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

public interface IProject extends IEngine, IMCP {

	String MCP_ENDPOINT = "MCP_ENDPOINT";
	String MCP_AUTH_SCHEME = "MCP_AUTH_SCHEME";
	String MCP_AUTH_TOKEN = "MCP_AUTH_TOKEN";

	String DEPENDENCIES_FILE_SUFFIX = "_dependencies.json";
	String BLOCK_FILE_NAME = "blocks.json";
	String NOTEBOOK_FOLDER = ".notebooks";

	enum PROJECT_TYPE {
		BLOCKS, CODE, WORKSPACE, SKILL, INSIGHTS, NOTEBOOK,
	};

	/**
	 * Sets the unique id for the project
	 * 
	 * @param projectId - id to set the project
	 */
	@IgnoreEngineLogging
	void setProjectId(String projectId);

	/**
	 * Get the project id
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getProjectId();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	IProject.PROJECT_TYPE getProjectType();

	/**
	 * Sets the name for the project
	 * 
	 * @param projectName - name of the project
	 */
	@IgnoreEngineLogging
	void setProjectName(String projectName);

	/**
	 * Get the project name
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getProjectName();

	// gets the perspectives for this engine
	// REFAC: Not sure we need this anymore
	@IgnoreEngineLogging
	Vector<String> getPerspectives();

	// gets the questions for a given perspective
	// REFAC: Not sure we need this anymore
	@IgnoreEngineLogging
	Vector<String> getInsights(String perspective);

	// get all the insights irrespective of perspective
	// REFAC: Not sure we need this anymore
	@IgnoreEngineLogging
	Vector<String> getInsights();

	// get the insight for a given question description
	// REFAC: Not sure we need this anymore - we can do this where id is null
	@IgnoreEngineLogging
	Vector<Insight> getInsight(String... id);

	/**
	 * Get the insight database
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	IRDBMSEngine getInsightDatabase();

	/**
	 * Set the insight database
	 * 
	 * @param insightDatabase
	 */
	@IgnoreEngineLogging
	void setInsightDatabase(IRDBMSEngine insightDatabase);

	/**
	 * Get a string representation of the insights database
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getInsightDefinition();

	/**
	 * Compile the project specific reactors
	 */
	void compileReactors();

	/**
	 * Get project specific reactor
	 * 
	 * @param reactorName
	 * @param loader
	 * @return
	 */
	IReactor getReactor(String reactorName);

	/**
	 * Get an ordered set of the reactor names
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
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
	boolean requirePublish(boolean pullFromCloud);

	/**
	 * 
	 * @param location
	 * @param pullFromCloud
	 * @return
	 */
	boolean publish(String location, boolean pullFromCloud);

	/**
	 * 
	 * @param republish
	 */
	@IgnoreEngineLogging
	void setRepublish(boolean republish);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean isPublished();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	SemossDate getLastPublishDate();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	List<File> writeNotebooks();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	INotebookHelper getNotebookHelper();

	/**
	 * 
	 * @param insight
	 * @param inputReplacements
	 * @return
	 */
	@IgnoreEngineLogging
	NotebookExecution executeNotebooks(Insight insight, Map<String, String> inputReplacements);

	/**
	 * Gets only engine deps listed in the project
	 * 
	 * @return Map of the variable name to the engine id
	 */
	@IgnoreEngineLogging
	Map<String, String> getEngineDependencies();

	/**
	 * Return if an asset
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean isAsset();

	/**
	 * Get the project properties
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	ProjectProperties getProjectProperties();

	/**
	 * Get the project git provider
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getProjectGitProvider();

	/**
	 * Get the project git repository URL
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getProjectGitRepo();

	/**
	 * Get the project git provider
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	AuthProvider getGitProvider();

	/**
	 * Clears the class cache
	 */
	@IgnoreEngineLogging
	void clearClassCache();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	ClientProcessWrapper getClientProcessWrapper();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	SocketClient getProjectTcpClient();

	/**
	 * 
	 * @param create
	 * @return
	 */
	@IgnoreEngineLogging
	SocketClient getProjectTcpClient(boolean create);

	/**
	 * 
	 * @param create
	 * @param port
	 * @return
	 */
	@IgnoreEngineLogging
	SocketClient getProjectTcpClient(boolean create, int port);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	TCPRTranslator getProjectRTranslator();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	PyTranslator getProjectPyTranslator();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getCompileOutput();

	/**
	 * Drops any cached MCP handler so the next MCP call is rebuilt from the current
	 * smss properties. Call this after changing {@link #MCP_ENDPOINT} or its auth
	 * properties. No-op for implementations that do not cache a handler.
	 */
	@IgnoreEngineLogging
	default void resetMCP() {
		// no-op
	}

	/**
	 * @return the url of the external MCP server this project delegates to, or null
	 *         when the project serves its own generated tools
	 */
	@IgnoreEngineLogging
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
	default String getRemoteMCPAuthScheme() {
		return null;
	}

}
