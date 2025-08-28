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

public interface IRemoteQueryable {

	/**
	 * Set the remote URL to get the next, etc. from
	 *
	 * @param engine
	 */
	void setRemoteAPI(String engine);

	/**
	 * Get the remote URL to get the next, etc. from
	 *
	 * @return
	 */
	String getRemoteAPI();

	/**
	 * Set the remote id
	 *
	 * @param id
	 */
	void setRemoteId(String id);

	/**
	 * Get the remote id
	 *
	 * @return
	 */
	String getRemoteId();

	/**
	 * Set if this is a remote request
	 *
	 * @param remote
	 * @return
	 */
	void setRemote(boolean remote);

	/**
	 * Get if this is a remote request
	 *
	 * @return
	 */
	boolean isRemote();
}
