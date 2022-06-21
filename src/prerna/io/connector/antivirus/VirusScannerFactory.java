package prerna.io.connector.antivirus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.antivirus.clamav.ClamAVScannerUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class VirusScannerFactory {
	
	private static final Logger logger = LogManager.getLogger(VirusScannerFactory.class);
	
	private VirusScannerFactory() {
		
	}
	
	public static IVirusScanner getVirusScannerConnector() {
		if (Utility.isVirusScanningDisabled()) {
			return null;
		}

		String scanType = DIHelper.getInstance().getProperty(Constants.VIRUS_SCANNING_METHOD);
		if (scanType.equalsIgnoreCase(IVirusScanner.CLAM_AV)) {
			return ClamAVScannerUtils.getInstance();
		} else {
			logger.warn("Virus Scanning is enabled but could not find type for input = '" + scanType + "'");
			return null;
		}
	}
}
