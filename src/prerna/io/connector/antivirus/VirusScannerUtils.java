package prerna.io.connector.antivirus;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

public class VirusScannerUtils {
	
	private static Logger logger = LogManager.getLogger(VirusScannerUtils.class);
	
	public static Map<String, Collection<String>> getViruses(String name, InputStream is) {
		if (Utility.isVirusScanningEnabled()) {
			long start = System.currentTimeMillis();
			IVirusScanner vs = VirusScannerFactory.getVirusScannerConnector();
			if (vs == null) {
				throw new IllegalArgumentException("Could not find virus scanner.");
			}
			
			Map<String, Collection<String>> viruses = vs.getViruses(name, is);
			long end = System.currentTimeMillis();
			logger.info("TIME TOOK: {} ms", (end - start));
			
			return viruses;
		} else {
			logger.warn("Virus scanner is disabled.");
			return new HashMap<String, Collection<String>>();
		}
	}

}
