package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cluster.util.ClusterUtil;
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

	// determine if we should even try to do R
	private static boolean USE_R = true;
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
		String useRStr =  DIHelper.getInstance().getProperty(Constants.USE_R);
		if(useRStr != null) {
			RJavaTranslatorFactory.USE_R = Boolean.parseBoolean(useRStr);
			if(!RJavaTranslatorFactory.USE_R) {
				INIT = true;
				return;
			}
		}
		
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
			
		} else if (ClusterUtil.IS_USER_RSERVE) {
			className = basePackage + "RJavaUserRserveTranslator";
			
		} else if (Boolean.parseBoolean(System.getenv("REMOTE_RSERVE"))) {
			
			className = basePackage + "RJavaRemoteRserveTranslator";
			
		} else{
 
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
	public static void initRConnection() {
		if(!INIT) {
			init();
		}
		
		if(!USE_R) {
			throw new IllegalArgumentException("R is set to false for this instance");
		}

		if(!getAttemptConnection()) {
			throw new IllegalArgumentException("Cannot find valid R paths to connect to R");
		}

		try {
			AbstractRJavaTranslator newInstance = (AbstractRJavaTranslator) translatorClass.newInstance();
			Insight dummyIn = new Insight();
			Logger dummyLogger = LogManager.getLogger(RJavaTranslatorFactory.class.getName());
			newInstance.setInsight(dummyIn);
			newInstance.setLogger(dummyLogger);
			newInstance.startR();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Stop the R connection if running
	 */
	public static void stopRConnection() {
		if(INIT) {
			try {
				AbstractRJavaTranslator newInstance = (AbstractRJavaTranslator) translatorClass.newInstance();
				Insight dummyIn = new Insight();
				Logger dummyLogger = LogManager.getLogger(RJavaTranslatorFactory.class.getName());
				newInstance.setInsight(dummyIn);
				newInstance.setLogger(dummyLogger);
				newInstance.endR();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
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

		if(!USE_R) {
			throw new IllegalArgumentException("R is set to false for this instance");
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
			determineAttemptConnection();
		}

		return attemptConnection;
	}

	public static synchronized void determineAttemptConnection() {
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

			String path = System.getenv("Path");
			List<String> pathSplit = Stream.of(path.split(";")).map(p -> p.replace("\\", "/")).distinct().collect(Collectors.toList());
			if(hasRHome && hasRLibs) {
				// make sure R_HOME and R_LIBS both exist
				if(!(new File(r_home).isDirectory()) || !(new File(r_libs)).isDirectory() ) {
					attemptConnection = false;
				} else {
					String cleanedRHome = r_home.replace("\\", "/");
					String cleanedRLibs = r_libs.replace("\\", "/");
					// we need R_HOME\bin\x64 or R_HOME\bin\x86
					// we need R_LIBS\rJava\jri\x64 or R_LIBS\rJava\jri\i386
					long rHomeInPath = pathSplit.stream().filter(p -> ( p.matches(Pattern.quote(cleanedRHome) + "/bin/.*") && (new File(p).isDirectory()) ) ).count();
					long rLibInPath = pathSplit.stream().filter(p -> ( p.matches(Pattern.quote(cleanedRLibs) + "/rJava/jri/.*") && (new File(p).isDirectory()) ) ).count();

					if(rHomeInPath >= 1 && rLibInPath >= 1) {
						attemptConnection = true;
					} else {
						attemptConnection = false;
					}
				}
			} else {
				List<String> rOrPortables = pathSplit.stream().filter(p -> 
					( p.matches(".*/R/.*") && (new File(p).isDirectory()) ) 
						|| ( p.matches(".*/R-Portables/.*") && (new File(p).isDirectory()) ) 
					).collect(Collectors.toList());
				
				long rLibInPath = rOrPortables.stream().filter(p -> ( p.matches(".*/rJava/jri/.*") && (new File(p).isDirectory()) ) ).count();
				
				if(rLibInPath >= 1) {
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


	public static void main(String[] args) {
		String r_home = "C:\\Users\\SEMOSS\\Desktop\\R-Portable\\App\\R-Portable";
		String r_libs = "C:\\Users\\SEMOSS\\Desktop\\R-Portable\\App\\R-Portable\\library";

		String path = "C:\\Users\\SEMOSS\\Desktop\\R-Portable\\App\\R-Portable;"
				+ "C:\\Users\\SEMOSS\\Desktop\\R-Portable\\App\\R-Portable\\library;"
				+ "C:\\Users\\SEMOSS\\Desktop\\R-Portable\\App\\R-Portable\\library\\rJava\\jri;"
				+ "C:\\Users\\SEMOSS\\Desktop\\R-Portable\\App\\R-Portable\\bin";

		String cleanedRHome = r_home.replace("\\", "/");
		String cleanedRLibs = r_libs.replace("\\", "/");
		// we need R_HOME
		// we need R_HOME\bin\x64 or R_HOME\bin\x86
		// we need R_LIBS
		// we need R_LIBS\rJava\jri\x64 or R_LIBS\rJava\jri\i386
		boolean hasAllRequiredPaths = Stream.of(path.split(";")).map(p -> p.replace("\\", "/"))
				.anyMatch(p -> 
				p.matches(Pattern.quote(cleanedRHome))
				|| p.matches(Pattern.quote(cleanedRHome + "/bin/"))
				|| p.matches(Pattern.quote(cleanedRLibs))
				|| p.matches(Pattern.quote(cleanedRLibs + "/rJava/jri/"))
				);

		System.out.println(hasAllRequiredPaths);
	}
}
