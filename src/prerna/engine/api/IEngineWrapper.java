/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.api;

import java.io.Closeable;

public interface IEngineWrapper extends Closeable {

	/** This method needs to be called to actually run the query */
	void execute() throws Exception;

	/**
	 * Set the query
	 *
	 * @param query
	 */
	void setQuery(String query);

	/**
	 * Get the set query
	 *
	 * @return
	 */
	String getQuery();

	/**
	 * Set the engine
	 *
	 * @param engine
	 */
	void setEngine(IDatabaseEngine engine);

	/**
	 * Get the engine
	 *
	 * @return
	 */
	public IDatabaseEngine getEngine();

	// /**
	// *
	// * @param val
	// */
	// void setTimeZone(TimeZone val);
	//
	// /**
	// *
	// * @return
	// */
	// TimeZone getTimeZone();

}
