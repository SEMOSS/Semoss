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
package prerna.util.gson;

import java.util.List;
import java.util.Vector;
import prerna.algorithm.api.ITableDataFrame;

public class FrameCacheHelper {

	/** Simple object to help cache frames in Insight Caching */
	private ITableDataFrame frame;

	private List<String> alias = new Vector<String>();

	FrameCacheHelper(ITableDataFrame frame) {
		this.frame = frame;
	}

	public void addAlias(String alias) {
		this.alias.add(alias);
	}

	public boolean sameFrame(ITableDataFrame frame) {
		return this.frame == frame;
	}

	public ITableDataFrame getFrame() {
		return this.frame;
	}

	public List<String> getAlias() {
		return this.alias;
	}
}
