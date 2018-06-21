package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RJavaTranslatorFactory {

	// get the OS type
	private static String OS = System.getProperty("os.name").toLowerCase();
	
	private static boolean isWin = false;
	static {
		isWin = (OS.indexOf("win") >= 0);
	}
	
	// this is so we only grab from DIHelper once
	private static boolean INIT = false;
	// this will be the specific class we want
	// either JRI version or RServe version
	private static Class translatorClass = null;

	// since JRI shuts down java
	// need to determine if we should risk it
	private static Boolean attemptConnection = null;
	// boolean for using jri or not
	private static boolean useJri = false;
	
	// value for r mem size
	public static String rMemory = "4096"; 
	
	private RJavaTranslatorFactory() {
		
	}
	
	/**
	 * This will determine the translator class to use (Rserve or JRI)
	 */
	private static void init() {
		String rMemory = DIHelper.getInstance().getProperty(Constants.R_MEM_LIMIT);
		if(rMemory != null) {
			RJavaTranslatorFactory.rMemory = rMemory; 
		}
		
		String useJriStr = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI);
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
	public static AbstractRJavaTranslator getRJavaTranslator(Insight insight, Logger logger) {
		AbstractRJavaTranslator newInstance  = null;
		if(!INIT) {
			init();
		}
		
		if(!getAttemptConnection()) {
			throw new IllegalArgumentException("Cannot find valid R paths to connect to R");
		}
		
		try {
			newInstance = (AbstractRJavaTranslator) translatorClass.newInstance();
			newInstance.setLogger(logger);
			
			// TODO: until we get everythign using this
			// let us pass the r connection info
			// if we have an r data table
			if(insight != null) {
				newInstance.setInsight(insight);
				ITableDataFrame dm = (ITableDataFrame) insight.getDataMaker();
				if(dm != null && dm instanceof RDataTable) {
					newInstance.setConnection(((RDataTable) dm).getConnection());
					newInstance.setPort(((RDataTable) dm).getPort());
				}
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return newInstance;
	}

	private static boolean getAttemptConnection() {
		/*
		 * Since the FE calls this all the time
		 * And if it hangs up and breaks an issue arises
		 * We will be more clever for when we try to call the startR method
		 */
		
		if(attemptConnection == null) {
			if(isWin && useJri) {
				boolean hasRHome = true;
				// first, check if R is in the path
				String r_home = System.getenv("R_HOME");
				if( (r_home == null || r_home.isEmpty())) {
					hasRHome = false;
				}
				
				boolean hasRLibs = true;
				// check for r_libs
				String r_libs = System.getenv("R_LIBS");
				if( (r_libs == null || r_libs.isEmpty())) {
					hasRLibs = false;
				}
				
				String regexFileSep = "(\\\\|/)";
				
				String path = System.getenv("Path");
				Stream<String> sPath = Stream.of(path.split(";"));
				if(hasRHome && hasRLibs) {
					String cleanedRHome = r_home.replace("\\", "\\\\");
					String cleanedRLibs = r_libs.replace("\\", "\\\\");
					// we need R_HOME
					// we need R_HOME\bin\x64 or R_HOME\bin\x86
					// we need R_LIBS
					// we need R_LIBS\rJava\jri\x64 or R_LIBS\rJava\jri\i386
					boolean hasAllRequiredPaths = sPath.anyMatch(p -> p.matches(cleanedRHome) && (new File(p).isDirectory()))
							&& sPath.anyMatch(p -> p.matches(cleanedRHome + regexFileSep + "bin" + regexFileSep) && (new File(p).isDirectory()))
							&& sPath.anyMatch(p -> p.matches(cleanedRLibs) && (new File(p).isDirectory()))
							&& sPath.anyMatch(p -> p.matches(cleanedRLibs + regexFileSep + "rJava" + regexFileSep + "jri" + regexFileSep) && (new File(p).isDirectory()));
					
					if(hasAllRequiredPaths) {
						attemptConnection = true;
					} else {
						attemptConnection = false;
					}
				} else {
					List<String> potentialEntries = sPath.filter(p -> p.matches(regexFileSep + "R" + regexFileSep)).collect(Collectors.toList());
					if(potentialEntries.size() < 4) {
						attemptConnection = false;
					}

					boolean containsJri = potentialEntries.stream().anyMatch(p -> p.matches("rJava" + regexFileSep + "jri") && (new File(p).isDirectory()) );
					if(!containsJri) {
						attemptConnection = false;
					}

					// if we get to this point
					// we are good
					attemptConnection = true;
				}
			} else {
				attemptConnection = true;
			}
		}
		
		return attemptConnection;
	}
}
