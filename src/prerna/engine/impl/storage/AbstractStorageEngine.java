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
package prerna.engine.impl.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import prerna.engine.api.IEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.util.Utility;

public abstract class AbstractStorageEngine extends AbstractEngine implements IStorageEngine {

	/**
	 * Init the general storage values
	 *
	 * @param builder
	 * @throws Exception
	 */
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
	}

	// Converts comma-separated local file/folder paths to List<Path>
	protected List<Path> parseLocalPaths(String commaSeparatedPaths) throws Exception {
		List<Path> result = new ArrayList<>();
		String[] parts = commaSeparatedPaths.split(",");

		for (String part : parts) {
			String trimmed = part.trim();
			if (!trimmed.isEmpty()) {
				result.add(Paths.get(trimmed));
			}
		}

		return result;
	}

	// Converts comma-separated cloud storage object paths to normalized String list
	protected List<String> parseStorageObjectPaths(String commaSeparatedPaths) {
		List<String> result = new ArrayList<>();
		String[] parts = commaSeparatedPaths.split(",");

		for (String part : parts) {
			String trimmed = part.trim();
			if (!trimmed.isEmpty()) {
				// Normalize the path using the utility method
				String normalized = Utility.normalizePath(trimmed);
				// Remove the leading slash if present
				if (normalized.startsWith("/")) {
					normalized = normalized.substring(1);
				}
				result.add(normalized);
			}
		}

		return result;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.STORAGE;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getStorageType().toString();
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}
}
