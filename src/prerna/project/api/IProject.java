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

import java.io.Closeable;
import java.util.TreeSet;
import java.util.Vector;

import prerna.auth.AuthProvider;
import prerna.date.SemossDate;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.project.impl.ProjectProperties;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.frame.r.util.TCPRTranslator;
import prerna.tcp.client.SocketClient;
import prerna.util.SemossClassloader;

public interface IProject extends IEngine, Closeable {
	
	String DEPENDENCIES_FILE_SUFFIX = "_dependencies.json";

	/**
	 * Sets the unique id for the project 
	 * @param projectId - id to set the project 
	 */
	void setProjectId(String projectId);
	
	/**
	 * Get the project id
	 * @return
	 */
	String getProjectId();

	/**
	 * Sets the name for the project 
	 * @param projectName - name of the project 
	 */
	void setProjectName(String projectName);
	
	/**
	 * Get the project name
	 * @return
	 */
	String getProjectName();
	
	// gets the perspectives for this engine
	// REFAC: Not sure we need this anymore
	Vector<String> getPerspectives();
	
	// gets the questions for a given perspective
	// REFAC: Not sure we need this anymore
	Vector<String> getInsights(String perspective);
	
	// get all the insights irrespective of perspective
	// REFAC: Not sure we need this anymore
	Vector<String> getInsights();

	// get the insight for a given question description
	// REFAC: Not sure we need this anymore - we can do this where id is null
	Vector<Insight> getInsight(String... id);
	
	/**
	 * Get the insight database
	 * @return
	 */
	RDBMSNativeEngine getInsightDatabase();

	/**
	 * Set the insight database
	 * @param insightDatabase
	 */
	void setInsightDatabase(RDBMSNativeEngine insightDatabase);
	
	/**
	 * Get a string representation of the insights database
	 * @return
	 */
	String getInsightDefinition();

	/**
	 * Compile the project specific reactors
	 */
	void compileReactors(SemossClassloader loader);
	
	/**
	 * Get project specific reactor
	 * @param reactorName
	 * @param loader
	 * @return
	 */
	IReactor getReactor(String reactorName, SemossClassloader loader);
	
	/**
	 * Get an ordered set of the reactor names
	 * @return
	 */
	TreeSet<String> getAvailableReactors();
	
	// publish the engine assets to a specific location
	// once published the assets in this app are available as a public_home from the browser
	// this is useful to access javascript etc. 
	// to enable this - you need to put the property public_home_enable on the smss file

	/**
	 * See if we need to republish. If requested pull from cloud
	 * 
	 * @param pullFromCloud
	 * @return
	 */
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
	void setRepublish(boolean republish);
	
	/**
	 * 
	 * @return
	 */
	boolean isPublished();
	
	/**
	 * 
	 * @return
	 */
	SemossDate getLastPublishDate();
	
	/**
	 * Return if an asset
	 * @return
	 */
	boolean isAsset();
	
	/**
	 * Get the project properties
	 * @return
	 */
	ProjectProperties getProjectProperties();
	
	/**
	 * Get the project git provider
	 * @return
	 */
	String getProjectGitProvider();
	
	/**
	 * Get the project git repository URL
	 * @return
	 */
	String getProjectGitRepo();

	/**
	 * Get the project git provider
	 * @return
	 */
	AuthProvider getGitProvider();
	
	/**
	 * Clears the class cache
	 */
	void clearClassCache();
	
	/**
	 * 
	 * @return
	 */
	SocketClient getProjectTcpClient();
	
	/**
	 * 
	 * @param create
	 * @return
	 */
	SocketClient getProjectTcpClient(boolean create);
	
	/**
	 * 
	 * @param create
	 * @param port
	 * @return
	 */
	SocketClient getProjectTcpClient(boolean create, int port);
	
	/**
	 * 
	 */
	void setProjectTcpClient(SocketClient tcpClient);
	
	/**
	 * 
	 * @return
	 */
	String getProjectTcpServerDirectory();
	
	/**
	 * 
	 * @return
	 */
	TCPRTranslator getProjectRTranslator();
	
	/**
	 * 
	 * @return
	 */
	TCPPyTranslator getProjectPyTranslator();
	
	/**
	 * 
	 * @return
	 */
	String getCompileOutput();
	
}

