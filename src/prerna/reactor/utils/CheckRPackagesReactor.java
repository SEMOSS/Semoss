package prerna.reactor.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class CheckRPackagesReactor extends AbstractReactor {

	private static final String CLASS_NAME = CheckRPackagesReactor.class.getName();
	
	// keep the list of packages static so 
	// we do not need to call this every time
	public static String[] pkgs = null;
	public static boolean rInstalled = false;
	
	public CheckRPackagesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELOAD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String reloadStr = this.keyValue.get(this.keysToGet[0]);
		boolean reload = reloadStr != null && Boolean.parseBoolean(reloadStr);
		setPackages(reload);
		Map<String, Object> returnMap = new HashMap<String,Object>();
		returnMap.put("RInstalled", CheckRPackagesReactor.rInstalled);
		returnMap.put("R", CheckRPackagesReactor.pkgs);
		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CHECK_R_PACKAGES);
	}
	
	private void setPackages(boolean reload) {
		if(reload || pkgs == null) {
			Logger logger = getLogger(CLASS_NAME);
			try{
				AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
				rJavaTranslator.startR();

				// R call to retrieve all of user's install packages
				CheckRPackagesReactor.pkgs = rJavaTranslator.getStringArray("as.character(unique(data.frame(installed.packages())$Package))");
				CheckRPackagesReactor.rInstalled = true;
			} catch(Exception e){
				logger.error(Constants.STACKTRACE, e);
				logger.info(e.getMessage());
				CheckRPackagesReactor.rInstalled = false;
				CheckRPackagesReactor.pkgs = new String[0];
			}
		}
	}
}
