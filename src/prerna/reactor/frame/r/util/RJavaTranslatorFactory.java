/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.frame.r.util;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.impl.r.RserveUtil;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

public class RJavaTranslatorFactory {

	private static final Logger classLogger = LogManager.getLogger(RJavaTranslatorFactory.class);

	// get the OS type
	private static String OS = System.getProperty("os.name").toLowerCase();
	private static int envNum = 0;
	private static String prefix = "env";

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
	private static Class<?> translatorClass = null;

	// since JRI shuts down java
	// need to determine if we should risk it
	private static Boolean attemptConnection = null;
	// boolean for using jri or not
	private static boolean useJri = false;
	// boolean for using netty or not
	private static boolean useNetty = false;

	// value for r mem size
	public static String rMemory = "4096";

	private RJavaTranslatorFactory() {

	}

	/**
	 * This will determine the translator class to use (Rserve or JRI)
	 */
	private static void init() {
		String useRStr = Utility.getDIHelperProperty(Constants.USE_R);
		if (useRStr != null) {
			RJavaTranslatorFactory.USE_R = Boolean.parseBoolean(useRStr);
			if (!RJavaTranslatorFactory.USE_R) {
				INIT = true;
				return;
			}
		}

		String rMemory = Utility.getDIHelperProperty(Constants.R_MEM_LIMIT);
		if (rMemory != null) {
			RJavaTranslatorFactory.rMemory = rMemory;
		}

		String nettyStr = Utility.getDIHelperProperty(Constants.NETTY_R);
		if (nettyStr != null) {
			useNetty = Boolean.parseBoolean(nettyStr);
		}

		String useJriStr = Utility.getDIHelperProperty(Constants.R_CONNECTION_JRI);
		if (useJriStr != null) {
			useJri = Boolean.valueOf(useJriStr);
		}

		String className = null;
		// making netty take precedence so we dont need to set multiple variabled
		if (useNetty) {
			className = TCPRTranslator.class.getName();
		} else if (useJri) {
			className = RJavaJriTranslator.class.getName();
		} else if (RserveUtil.IS_USER_RSERVE) {
			className = RJavaUserRserveTranslator.class.getName();
		} else if (Boolean.parseBoolean(System.getenv("REMOTE_RSERVE"))) {
			className = RJavaRemoteRserveTranslator.class.getName();
		} else {
			className = RJavaRserveTranslator.class.getName();
		}

		try {
			translatorClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			classLogger.error("Failed to load the R translator class {}", className, e);
		}

		INIT = true;
	}

	/**
	 * Starts the R connection this instance is configured for, using a translator
	 * that is not tied to a user's insight.
	 *
	 * @throws IllegalArgumentException if R is turned off for this instance or the
	 *                                  R paths needed to connect are missing
	 */
	public static void initRConnection() {
		if (!INIT) {
			init();
		}

		if (!USE_R) {
			throw new IllegalArgumentException("R is set to false for this instance");
		}

		if (!getAttemptConnection()) {
			throw new IllegalArgumentException("Cannot find valid R paths to connect to R");
		}

		AbstractRJavaTranslator translator = newStandaloneTranslator();
		if (translator != null) {
			translator.startR();
		}
	}

	/**
	 * Stops the R connection when the factory has loaded a translator class.
	 */
	public static void stopRConnection() {
		if (INIT && translatorClass != null) {
			AbstractRJavaTranslator translator = newStandaloneTranslator();
			if (translator != null) {
				translator.endR();
			}
		}
	}

	/**
	 * Creates a translator of the configured class with an empty insight, for
	 * starting or stopping the R connection outside of a user's insight.
	 *
	 * @return the translator, or null when the class cannot be instantiated
	 */
	private static AbstractRJavaTranslator newStandaloneTranslator() {
		try {
			AbstractRJavaTranslator translator = (AbstractRJavaTranslator) translatorClass.getConstructor()
					.newInstance();
			translator.setInsight(new Insight());
			translator.setLogger(classLogger);
			return translator;
		} catch (ReflectiveOperationException e) {
			classLogger.error("Failed to create the R translator {}", translatorClass.getName(), e);
			return null;
		}
	}

	/**
	 * Get a new RJavaTranslator based on if we are using rserve or jri
	 * 
	 * @param insight the insight the translator runs for, or null for none
	 * @param logger  the logger the translator writes to
	 * @return the translator, or null when the translator class cannot be
	 *         instantiated
	 */
	public static AbstractRJavaTranslator getRJavaTranslator(Insight insight, Logger logger) {
		AbstractRJavaTranslator newInstance = null;
		if (!INIT) {
			init();
		}

		if (!USE_R) {
			throw new IllegalArgumentException("R is set to false for this instance");
		}

		if (!getAttemptConnection()) {
			throw new IllegalArgumentException("Cannot find valid R paths to connect to R");
		}

		try {
			newInstance = (AbstractRJavaTranslator) translatorClass.getConstructor().newInstance();
			newInstance.setLogger(logger);

			// make the environment here
			newInstance.env = prefix + envNum;
			envNum++;

			// TODO: until we get everythign using this
			// let us pass the r connection info
			// if we have an r data table
			if (insight != null) {
				newInstance.setInsight(insight);
				ITableDataFrame dm = (ITableDataFrame) insight.getDataMaker();
				if (dm != null && dm instanceof RDataTable) {
					newInstance.setConnection(((RDataTable) dm).getConnection());
					newInstance.setPort(((RDataTable) dm).getPort());

					// set the environment
					newInstance.initREnv(newInstance.env);

				}
			}
		} catch (ReflectiveOperationException | IllegalArgumentException | SecurityException e) {
			classLogger.error("Failed to create the R translator {}", translatorClass.getName(), e);
		}
		return newInstance;
	}

	private static boolean getAttemptConnection() {
		/*
		 * Since the FE calls this all the time And if it hangs up and breaks an issue
		 * arises We will be more clever for when we try to call the startR method
		 */

		if (attemptConnection == null) {
			determineAttemptConnection();
		}

		return attemptConnection;
	}

	/**
	 * Decides whether this instance may try to connect to R. JRI shuts down the JVM
	 * when it cannot find R, so on Windows with JRI the connection is only
	 * attempted when R and rJava/jri are on the path; every other setup always
	 * attempts it.
	 */
	public static synchronized void determineAttemptConnection() {
		if (isWin && useJri) {
			// first, check if R is in the path
			String rHome = System.getenv("R_HOME");
			if (rHome != null) {
				rHome = rHome.trim();
			}
			classLogger.info("Using R_HOME = \"{}\"", rHome);

			// check for r_libs
			String rLibs = System.getenv("R_LIBS");
			if (rLibs != null) {
				rLibs = rLibs.trim();
			}
			classLogger.info("Using R_LIBS = \"{}\"", rLibs);

			String path = System.getenv("Path");
			List<String> pathSplit = Stream.of(path == null ? new String[0] : path.split(";"))
					.map(p -> p.replace("\\", "/")).distinct().collect(Collectors.toList());
			if (rHome != null && !rHome.isEmpty() && rLibs != null && !rLibs.isEmpty()) {
				// make sure R_HOME and R_LIBS both exist
				if (!(new File(rHome).isDirectory()) || !(new File(rLibs)).isDirectory()) {
					attemptConnection = false;
				} else {
					String cleanedRHome = rHome.replace("\\", "/");
					String cleanedRLibs = rLibs.replace("\\", "/");
					// we need R_HOME\bin\x64 or R_HOME\bin\x86
					// we need R_LIBS\rJava\jri\x64 or R_LIBS\rJava\jri\i386

					long rHomeInPath = pathSplit.stream()
							.filter(p -> (p.matches(Pattern.quote(cleanedRHome) + "/bin/.*")
									&& (new File(Utility.normalizePath(p)).isDirectory())))
							.count();

					long rLibInPath = pathSplit.stream()
							.filter(p -> (p.matches(Pattern.quote(cleanedRLibs) + "/rJava/jri/.*")
									&& (new File(Utility.normalizePath(p)).isDirectory())))
							.count();

					attemptConnection = rHomeInPath >= 1 && rLibInPath >= 1;
				}
			} else {
				List<String> rOrPortables = pathSplit.stream()
						.filter(p -> (p.matches(".*/R/.*") && (new File(Utility.normalizePath(p)).isDirectory()))
								|| (p.matches(".*/R-Portables/.*")
										&& (new File(Utility.normalizePath(p)).isDirectory())))
						.collect(Collectors.toList());

				long rLibInPath = rOrPortables.stream().filter(
						p -> (p.matches(".*/rJava/jri/.*") && (new File(Utility.normalizePath(p)).isDirectory())))
						.count();

				attemptConnection = rLibInPath >= 1;
			}
		} else {
			attemptConnection = true;
		}
	}
}
