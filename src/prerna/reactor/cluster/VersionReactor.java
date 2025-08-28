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
package prerna.reactor.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class VersionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(VersionReactor.class);

	private static Map<String, String> versionMap;
	public static String POM_VERSION_KEY = "version";
	public static String POM_BUILD_DATE_KEY = "build.date";

	public static String VERSION_KEY = POM_VERSION_KEY;
	public static String DATETIME_KEY = "datetime";

	public VersionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELOAD.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String reloadStr = this.keyValue.get(this.keysToGet[0]);
		boolean reload = reloadStr != null && Boolean.parseBoolean(reloadStr);
		return new NounMetadata(getVersionMap(reload), PixelDataType.MAP, PixelOperationType.VERSION);
	}

	public static Map<String, String> getVersionMap(boolean reload) {
		if (reload || VersionReactor.versionMap == null) {
			Map<String, String> tempMap = new HashMap<>();

			InputStream versionStream = null;
			try {
				Properties props = new Properties();
				Enumeration<URL> resources = VersionReactor.class.getClassLoader()
						.getResources("META-INF/maven/org.semoss/semoss/pom.properties");
				while (resources.hasMoreElements()) {
					URL url = resources.nextElement();
					versionStream = url.openStream();
					props.load(versionStream);
					tempMap.put(VERSION_KEY, props.getProperty(POM_VERSION_KEY));
					tempMap.put(DATETIME_KEY, props.getProperty(POM_BUILD_DATE_KEY));
					VersionReactor.versionMap = new HashedMap<>(tempMap);
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				try {
					if (versionStream != null) {
						versionStream.close();
					}
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return VersionReactor.versionMap;
	}
}
