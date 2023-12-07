package prerna.io.connector.antivirus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.antivirus.clamav.ClamAVScannerUtils;
import prerna.io.connector.antivirus.tika.ApacheTikaScannerUtils;
import prerna.io.connector.antivirus.virustotal.VirusTotalScannerUtils;
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
		String scanType = DIHelper.getInstance().getProperty(Constants.VIRUS_SCANNING_METHOD).toUpperCase();
		
		if(scanType.equals(IVirusScanner.VIRUS_SCANNER_TYPE.CLAM_AV.toString())) {
			return ClamAVScannerUtils.getInstance();
		} 
		else if(scanType.equals(IVirusScanner.VIRUS_SCANNER_TYPE.VIRUS_TOTAL.toString())) {
			return VirusTotalScannerUtils.getInstance();
		} 
		else if(scanType.equals(IVirusScanner.VIRUS_SCANNER_TYPE.APACHE_TIKA.toString())) {
			return new ApacheTikaScannerUtils();
		} 
		
		else if (scanType.equalsIgnoreCase(IVirusScanner.CLAM_AV)) {
			logger.warn("Using deprecated value - please update parameter value for " + Constants.VIRUS_SCANNING_METHOD + "to CLAM_AV");
			logger.warn("Using deprecated value - please update parameter value for " + Constants.VIRUS_SCANNING_METHOD + "to CLAM_AV");
			logger.warn("Using deprecated value - please update parameter value for " + Constants.VIRUS_SCANNING_METHOD + "to CLAM_AV");
			logger.warn("Using deprecated value - please update parameter value for " + Constants.VIRUS_SCANNING_METHOD + "to CLAM_AV");
			logger.warn("Using deprecated value - please update parameter value for " + Constants.VIRUS_SCANNING_METHOD + "to CLAM_AV");
			return ClamAVScannerUtils.getInstance();
		} else {
			logger.warn("Virus Scanning is enabled but could not find type for input = '" + scanType + "'");
			return null;
		}
	}
}
