package prerna.reactor.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
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
		this.keysToGet = new String[] { ReactorKeysEnum.RELOAD.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String reloadStr = this.keyValue.get(this.keysToGet[0]);
		boolean reload = reloadStr != null && Boolean.parseBoolean(reloadStr);
		return new NounMetadata(getVersionMap(reload), PixelDataType.MAP, PixelOperationType.VERSION);
	}

	/**
	 * 
	 * @param reload
	 * @return
	 */
	public static Map<String, String> getVersionMap(boolean reload) {
		if (reload || VersionReactor.versionMap == null) {
			Map<String, String> tempMap = new HashMap<>();

			try {
				Properties props = new Properties();
				Enumeration<URL> resources = VersionReactor.class.getClassLoader()
						.getResources("META-INF/maven/org.semoss/semoss/pom.properties");
				boolean found = false;
				while (resources.hasMoreElements()) {
					URL url = resources.nextElement();
					try (InputStream in = url.openStream()) {
						props.load(in);
						tempMap.put(VERSION_KEY, props.getProperty(POM_VERSION_KEY));
						tempMap.put(DATETIME_KEY, props.getProperty(POM_BUILD_DATE_KEY));
						VersionReactor.versionMap = Collections.unmodifiableMap(new HashedMap<>(tempMap));
						found = true;
					}
				}

				if (!found) {
					classLogger.info("Maven path not found ... likely local development, trying Monolith path ...");
					resources = VersionReactor.class.getClassLoader()
							.getResources("META-INF/maven/org.semoss/monolith/pom.properties");
					while (resources.hasMoreElements()) {
						URL url = resources.nextElement();
						try (InputStream in = url.openStream()) {
							props.load(in);
							tempMap.put(VERSION_KEY, props.getProperty(POM_VERSION_KEY));
							tempMap.put(DATETIME_KEY, props.getProperty(POM_BUILD_DATE_KEY));
							VersionReactor.versionMap = Collections.unmodifiableMap(new HashedMap<>(tempMap));
							found = true;
						}
					}
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		return VersionReactor.versionMap;
	}

}
