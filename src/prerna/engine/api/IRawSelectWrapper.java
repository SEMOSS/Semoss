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

import java.util.Iterator;
import prerna.algorithm.api.SemossDataType;

public interface IRawSelectWrapper extends IEngineWrapper, Iterator<IHeadersDataRow> {

	/** Get the names of the returns */
	String[] getHeaders();

	/** Get the types for each return */
	// TODO: move to pixel data type
	SemossDataType[] getTypes();

	/**
	 * Get the number of rows
	 *
	 * @return
	 */
	long getNumRows() throws Exception;

	/** Get the size of the return */
	long getNumRecords() throws Exception;

	/** Reset the iterator */
	void reset() throws Exception;

	/**
	 * Can the full result set be flushed directly from the object
	 *
	 * @return
	 */
	boolean flushable();

	/**
	 * Return data flushed as a string
	 *
	 * @return
	 */
	String flush();
}
