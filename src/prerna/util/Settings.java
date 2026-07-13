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
package prerna.util;

public class Settings {

	public static final String CHECK_MEM = "CHECK_MEM";
	public static final String MEM_PROFILE_SETTINGS = "MEM_PROFILE_SETTINGS";
	public static final String CONSTANT_MEM = "CONSTANT";
	public static final String USER_MEM_LIMIT = "USER_MEM_LIMIT";
	public static final String SMART_SYNC = "SMART_SYNC";
	public static final String RESERVED_JAVA_MEM = "RESERVED_JAVA_MEM";
	public static final String PUBLIC_HOME = "PUBLIC_HOME";
	public static final String COPY_PROJECT = "COPY_PROJECT";
	public static final String JAVA_HOME = "JAVA_HOME";
	public static final String MVN_HOME = "MVN_HOME";
	public static final String REPO_HOME = "REPO_HOME";
	public static final String BLOCKING = "BLOCKING";
	public static final String TCP_CLIENT = "TCP_CLIENT";

	// TODO: come back to this ... might not be needed anymore
	// hugging face cache folder
	public static final String HF_CACHE_DIR = "HF_CACHE_DIR";

	// load the db again on the socket side or give
	// it as engine wrapper
	public static final String LOAD_DB_ON_SOCKET = "LOAD_DB_ON_SOCKET";

	public static final String CUSTOM_REACTOR_EXECUTION = "CUSTOM_REACTOR_EXECUTION";
	@Deprecated
	public static final String PY_HOME = "PY_HOME";
	public static final String PYTHONHOME = "PYTHONHOME";
	public static final String PYTHONHOME_SITE_PACKAGES = "PYTHONHOME_SITE_PACKAGES";
	public static final String NATIVE_PY_SERVER = "NATIVE_PY_SERVER";
	public static final String PY_SERVER_USER = "PY_SERVER_USER";

	public static final String PROMPT_STOPPER = "PROMPT_STOPPER";
	public static final String VAR_NAME = "VAR_NAME";
	public static final String TIMEOUT = "TIMEOUT";
	// input usually a list to functions
	public static final String INPUT = "INPUT";

	public static final String COUNT = "COUNT";

	// this is a JSON of all the requirements for a SMSS
	// engine like GPU requirement etc. etc.
	public static final String REQUIREMENTS = "REQUIREMENTS";

	// FOR REMOTE CLIENTS

	// The short name for a model.. Can't use the HuggingFace model repo id
	// here due to naming restrictions on kubernetes resources
	public static final String MODEL = "MODEL";
	// The HuggingFace model repo id
	public static final String MODEL_REPO_ID = "MODEL_REPO_ID";
	// The type of model
	public static final String MODEL_TYPE = "MODEL_TYPE";

	// DEBUGGING

	// the port to force the connection on
	public static final String FORCE_PORT = "FORCE_PORT";
	// the python logger level — DEBUG, INFO, WARNING, or CRITICAL
	public static final String LOGGER_LEVEL = "LOGGER_LEVEL";

}
