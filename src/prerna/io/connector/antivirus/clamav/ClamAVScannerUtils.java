package prerna.io.connector.antivirus.clamav;

import java.io.InputStream;
import java.nio.file.FileSystems;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.antivirus.IVirusScanner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import xyz.capybara.clamav.ClamavClient;
import xyz.capybara.clamav.Platform;
import xyz.capybara.clamav.commands.scan.result.ScanResult;

public class ClamAVScannerUtils implements IVirusScanner {
	
	private static final Logger classLogger = LogManager.getLogger(ClamAVScannerUtils.class);
	
	public static final String CLAMAV_SCANNING_PORT = "CLAMAV_SCANNING_PORT";
	public static final String CLAMAV_SCANNING_ADDRESS = "CLAMAV_SCANNING_ADDRESS";
	public static final String CLAMAV_SCANNING_FS = "CLAMAV_SCANNING_FS";
	
	private static ClamAVScannerUtils instance;
	private ClamavClient client;
	
	private ClamAVScannerUtils() throws Exception {
		int port = Optional.ofNullable(getVirusScanningPort())
				.orElseThrow(() -> new Exception("Port cannot be null"));
		
		String address = Optional.ofNullable(getVirusScanningAddress())
				.orElseThrow(() -> new Exception("Address cannot be null"));

		Platform platform = Optional.ofNullable(getVirusScanningFileSystem())
				.orElseThrow(() -> new Exception("Platform cannot be null"));
		
		classLogger.info("address: " + address + " port: " + port + " platform: " + platform.toString());
		
		this.client = new ClamavClient(address, port, platform);
	}

	public static IVirusScanner getInstance() {
		if(instance != null) {
			return instance;
		}

		if(instance == null) {
			synchronized(ClamAVScannerUtils.class) {
				if(instance == null) {
					try {
						instance = new ClamAVScannerUtils();
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		return instance;
	}
	
	@Override
	public Map<String, Collection<String>> getViruses(String name, InputStream is) {
		ScanResult sr = this.client.scan(is);
		
		if (sr instanceof ScanResult.OK) {
			return new HashMap<String, Collection<String>>();
		} else {
			return ((ScanResult.VirusFound) sr).getFoundViruses();
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private static String getVirusScanningAddress() {
		return DIHelper.getInstance().getProperty(CLAMAV_SCANNING_ADDRESS);
	}
	
	/**
	 * 
	 * @return
	 */
	private static Integer getVirusScanningPort() {
		String virusScanning = DIHelper.getInstance().getProperty(CLAMAV_SCANNING_PORT);
		if(virusScanning == null) {
			// default configuration is false
			return null;
		}
		
		return Integer.valueOf(virusScanning);
	}
	
	/**
	 * 
	 * @return
	 */
	public static Platform getVirusScanningFileSystem() {
		String platform = DIHelper.getInstance().getProperty(CLAMAV_SCANNING_FS);

		if ("WINDOWS".equalsIgnoreCase(platform)) {
			return Platform.WINDOWS;	
		} else if ("UNIX".equalsIgnoreCase(platform)) {
			return Platform.UNIX;
		} else {
			String sep = FileSystems.getDefault().getSeparator();
			if (sep.equals("/")) {	
				return Platform.UNIX;
			} else if (sep.equals("\\")) {
				return Platform.WINDOWS;
			} else {
				return null;
			}
		}
	}
}
