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

public interface IApi {

	// the methods are not cohesive
	// but basically it has
	// SELECTORS
	// FILTERS
	// JOINS

	// gets all the hashtable with things needed by this connector
	public String[] getParams(); // this can be the same as listeners we are listening to

	// bunch of set data goes here
	public void set(String key, Object value); // I wonder if the value can be string [] - which is the actual name and
												// value

	// process and get the iterator
	// hopefully, the iterator is a map of selectors
	public Iterator<IHeadersDataRow> process();
}
