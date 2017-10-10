package prerna.sablecc2.reactor.frame.r.util;

import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RJavaTranslatorFactory {

	// this is so we only grab from DIHelper once
	private static boolean INIT = false;
	// this will be the specific class we want
	// either JRI version or RServe version
	private static Class translatorClass = null;
	
	private RJavaTranslatorFactory() {
		
	}
	
	private static void init() {
		String useJriStr = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI);
		boolean useJri = false;
		if(useJriStr != null) {
			useJri = Boolean.valueOf(useJriStr);
		}
		final String basePackage = "prerna.sablecc2.reactor.frame.r.util.";
		String className = null;
		if(useJri) {
			className = basePackage + "RJavaJriTranslator";
		} else {
			className = basePackage + "RJavaRserveTranslator";
		}
		
		try {
			translatorClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		INIT = true;
	}
	
	/**
	 * Get a new RJavaTranslator based on if we are using rserve or jri
	 * @param insight
	 * @param logger
	 * @return
	 */
	public static IRJavaTranslator getRJavaTranslator(Insight insight, Logger logger) {
		IRJavaTranslator newInstance  = null;
		if(!INIT) {
			init();
		}
		try {
			newInstance = (IRJavaTranslator) translatorClass.newInstance();
			newInstance.setInsight(insight);
			newInstance.setLogger(logger);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return newInstance;
	}
	
}
