package prerna.sablecc2.reactor.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;

public class CheckRPackagesReactor extends AbstractReactor {

	private static final String CLASS_NAME = CheckRPackagesReactor.class.getName();

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		Map<String, Object> returnMap = new HashMap<String,Object>();
		try{
			AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
			rJavaTranslator.startR();

			// R call to retrieve all of user's install packages
			String[] pkgs = rJavaTranslator.getStringArray("as.character(unique(data.frame(installed.packages())$Package))");
			
			returnMap.put("RInstalled", true);
			if (pkgs.length > 0) {
				returnMap.put("R", pkgs);
			} else {
				returnMap.put("R", new String[0]);
			}
		} catch(Exception e){
			logger.info(e.getMessage());
			returnMap.put("RInstalled", false);
			returnMap.put("R", new String[0]);
		}
		
		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CHECK_R_PACKAGES);
	}
}
