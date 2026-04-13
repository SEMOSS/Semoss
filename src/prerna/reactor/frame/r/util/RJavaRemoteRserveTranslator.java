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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RFileInputStream;
import org.rosuda.REngine.Rserve.RFileOutputStream;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.engine.impl.r.RRemoteRserve;
import prerna.reactor.runtime.AbstractBaseRClass;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RJavaRemoteRserveTranslator extends RJavaRserveTranslator {

	private static final Logger classLogger = LogManager.getLogger(RJavaRemoteRserveTranslator.class);

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private RJavaRemoteRserveTranslator() {

	}

	@Override
	public void startR() {
		if (this.insight != null) {
			NounMetadata noun = this.insight.getVarStore().get(R_CONN);
			if (noun != null) {
				retCon = (RConnection) this.insight.getVarStore().get(R_CONN).getValue();
			}
			NounMetadata nounPort = this.insight.getVarStore().get(R_PORT);
			if (nounPort != null) {
				port = (String) nounPort.getValue();
			}
		}

		if (this.insight.getUser() != null) {
			if (this.insight.getUser().getRcon() != null) {
				retCon = this.insight.getUser().getRconRemote().getConnection();
			}
		}

		if (this.retCon == null) {
			classLogger.info("R Connection has not been defined yet...");
		} else {
			classLogger.info("Retrieving existing R Connection...");
		}

		if (this.retCon == null) {
			try {
				classLogger.info("Starting R Connection... ");
				if (this.insight != null) {
					if (this.insight.getUser() != null) {
						if (this.insight.getUser().getRcon() == null) {
							RRemoteRserve rTemp = new RRemoteRserve();
							this.insight.getUser().setRconRemote(rTemp);
						}
						this.retCon = this.insight.getUser().getRconRemote().getConnection();
					}
				} else {
					RRemoteRserve rTemp = new RRemoteRserve();
					this.retCon = rTemp.getConnection();
				}
				classLogger.info("Successfully created R Connection... ");

				// port = Utility.findOpenPort();
				// logger.info("Starting it on port.. " + port);
				// // need to find a way to get a common name
				// masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
				// retCon = new RConnection("127.0.0.1", Integer.parseInt(port));

				if (retCon == null) {
					throw new NullPointerException(
							"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
									+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
				}

				// load all the libraries
				retCon.eval("library(splitstackshape);");
				classLogger.info("Loaded packages splitstackshape");
				// data table
				retCon.eval("library(data.table);");
				classLogger.info("Loaded packages data.table");
				// reshape2
				retCon.eval("library(reshape2);");
				classLogger.info("Loaded packages reshape2");
				// stringr
				retCon.eval("library(stringr)");
				classLogger.info("Loaded packages stringr");
				// lubridate
				retCon.eval("library(lubridate);");
				classLogger.info("Loaded packages lubridate");
				// dplyr
				retCon.eval("library(dplyr);");
				classLogger.info("Loaded packages dplyr");

				if (this.insight != null) {
					this.insight.getVarStore().put(AbstractBaseRClass.R_CONN,
							new NounMetadata(retCon, PixelDataType.R_CONNECTION));
					this.insight.getVarStore().put(AbstractBaseRClass.R_PORT,
							new NounMetadata(port, PixelDataType.CONST_STRING));
				}

			} catch (Exception e) {
				classLogger.error(
						"Failed to start remote R connection and load required packages (splitstackshape, data.table, reshape2, stringr, lubridate, dplyr).",
						e);
				throw new IllegalArgumentException(
						"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
								+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
			}
		}
		// initialize the r environment
		initREnv();
	}

	private void transferToServer(String clientFile, String serverFile) {
		RConnection r = getRcon();
		byte[] b = new byte[8192];
		BufferedInputStream clientStream = null;
		RFileOutputStream serverStream = null;
		try {
			/* the file on the client machine we read from */
			clientStream = new BufferedInputStream(new FileInputStream(new File(clientFile)));

			/* the file on the server we write to */
			serverStream = r.createFile(serverFile);

			/* typical java IO stuff */
			int c = clientStream.read(b);
			while (c >= 0) {
				serverStream.write(b, 0, c);
				c = clientStream.read(b);
			}

		} catch (IOException e) {
			classLogger.error("Failed to transfer file from client path '{}' to R server path '{}'.", clientFile,
					serverFile, e);
		} finally {
			if (serverStream != null) {
				try {
					serverStream.close();
				} catch (IOException e) {
					classLogger.error("Failed to close R server output stream for server path '{}'.", serverFile, e);
				}
			}
			if (clientStream != null) {
				try {
					clientStream.close();
				} catch (IOException e) {
					classLogger.error("Failed to close client input stream for client path '{}'.", clientFile, e);
				}
			}
		}
	}

	private void transferToClient(String clientFile, String serverFile) {
		RConnection r = getRcon();
		byte[] b = new byte[8192];
		BufferedOutputStream clientStream = null;
		RFileInputStream serverStream = null;
		try {
			/* the file on the client machine we write to */
			clientStream = new BufferedOutputStream(new FileOutputStream(new File(clientFile)));

			/* the file on the server machine we read from */
			serverStream = r.openFile(serverFile);

			/* typical java io stuff */
			int c = serverStream.read(b);
			while (c >= 0) {
				clientStream.write(b, 0, c);
				c = serverStream.read(b);
			}

			clientStream.close();
			serverStream.close();
		} catch (IOException e) {
			classLogger.error("Failed to transfer file from R server path '{}' to client path '{}'.", serverFile,
					clientFile, e);
		} finally {
			if (serverStream != null) {
				try {
					serverStream.close();
				} catch (IOException e) {
					classLogger.error("Failed to close R server input stream for server path '{}'.", serverFile, e);
				}
			}
			if (clientStream != null) {
				try {
					clientStream.close();
				} catch (IOException e) {
					classLogger.error("Failed to close client output stream for client path '{}'.", clientFile, e);
				}
			}
		}
	}

	@Override
	public void runR(String script) {
		String insightCacheLoc = Utility.getInsightCacheDir();
		String csvInsightCacheFolder = Utility.getCsvInsightCacheDir();
		String baseDir = insightCacheLoc + "\\" + csvInsightCacheFolder + "\\";
		String tempFileLocation = baseDir + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		// write file out on local FS
		File f = new File(tempFileLocation);
		try {
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			classLogger.error("Failed to write temporary R script for execution at path '{}'.", tempFileLocation, e1);
		}

		// Copy file over to server
		String fileExtension = FilenameUtils.getExtension(tempFileLocation);
		String newServerFileLoc = "/tmp/" + Utility.getRandomString(15) + "." + fileExtension;
		transferToServer(tempFileLocation, newServerFileLoc);

		// Execute the file with respect to the server file location
		try {
			this.executeEmptyR("source(\"" + newServerFileLoc + "\", local=TRUE)");
		} finally {
			// delete local and server file
			f.delete();
		}
	}

	@Override
	public String runRAndReturnOutput(String script) {
		RConnection r = null;
		Boolean remoteR = false;
		if (Boolean.parseBoolean(System.getenv("REMOTE_RSERVE"))) {
			r = getRcon();
			remoteR = true;
		}
		String insightCacheLoc = Utility.getInsightCacheDir();
		String csvInsightCacheFolder = Utility.getCsvInsightCacheDir();
		String baseDir = insightCacheLoc + DIR_SEPARATOR + csvInsightCacheFolder + DIR_SEPARATOR;
		String tempFileLocation = baseDir + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		String outputLoc = baseDir + Utility.getRandomString(15) + ".txt";
		File outputF = null;
		if (remoteR) {
			outputLoc = "/tmp/" + Utility.getRandomString(15) + ".txt";
		}
		outputLoc = outputLoc.replace("\\", "/");

		if (remoteR) {
			try {
				if (r != null) {
					r.createFile(outputLoc);
				}
			} catch (IOException e) {
				classLogger.error("Failed to create remote output file '{}' for R script execution.", outputLoc, e);
			}
		} else {
			outputF = new File(outputLoc);
			try {
				outputF.createNewFile();
			} catch (IOException e) {
				classLogger.error("Failed to create local output file '{}' for R script execution.", outputLoc, e);
			}
		}

		String randomVariable = "con" + Utility.getRandomString(6);
		File f = new File(tempFileLocation);
		try {
			script = script.trim();
			if (!script.endsWith(";")) {
				script = script + ";";
			}
			script = randomVariable + "<- file(\"" + outputLoc + "\"); sink(" + randomVariable
					+ ", append=TRUE, type=\"output\"); " + "sink(" + randomVariable
					+ ", append=TRUE, type=\"message\"); " + script + " sink();";
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			classLogger.error("Failed to write temporary R script with output redirection at path '{}'.",
					tempFileLocation, e1);
		}
		String scriptOutput = null;
		if (remoteR) {
			String fileExtension = FilenameUtils.getExtension(tempFileLocation);
			String newServerFileLoc = "/tmp/" + Utility.getRandomString(15) + "." + fileExtension;
			transferToServer(tempFileLocation, newServerFileLoc);
			// overwrite the previous location to be server side location
			tempFileLocation = newServerFileLoc;

			try {
				String finalScript = "print(source(\"" + tempFileLocation + "\", print.eval=TRUE, local=TRUE)); ";
				this.executeR(finalScript);
				try {
					String outputLocLocal = baseDir + Utility.getRandomString(15) + ".txt";

					transferToClient(outputLocLocal, outputLoc);
					outputF = new File(outputLocLocal);
					scriptOutput = FileUtils.readFileToString(outputF);
				} catch (IOException e) {
					classLogger.error(
							"Failed to copy/read remote R script output from server path '{}' to local output file.",
							outputLoc, e);
				}
			} finally {
				f.delete();
				if (outputF != null) {
					outputF.delete();
				}
				// executeEmptyR("file.remove(" + tempFileLocation + ");");
				// executeEmptyR("file.remove(" + outputLoc + ");");

			}
		}

		else {
			try {
				String finalScript = "print(source(\"" + tempFileLocation + "\", print.eval=TRUE, local=TRUE)); ";
				this.executeR(finalScript);
				try {
					scriptOutput = FileUtils.readFileToString(outputF);
				} catch (IOException e) {
					classLogger.error("Failed to read local R script output file '{}'.", outputLoc, e);
				}
			} finally {
				f.delete();
				if (outputF != null) {
					outputF.delete();
				}
			}
		}

		// drop the random con variable
		this.executeEmptyR("rm(" + randomVariable + ")");
		this.executeEmptyR("gc()");

		if (scriptOutput == null) {
			// throw new NullPointerException("Neccesity to trim, scriptOutput cannot be
			// null here.");
			return "";
		}

		// return the final output
		return scriptOutput.trim();
	}

	@Override
	public Object executeR(String rScript) {
		try {
			classLogger.info("executeR: " + rScript);
			return retCon.eval(rScript);
		} catch (Exception e) {
			classLogger.error("Failed to execute R script via eval. Script='{}'.", Utility.cleanLogString(rScript), e);
		}
		return null;
	}

	@Override
	public void executeEmptyR(String rScript) {
		try {
			classLogger.info("executeR: " + Utility.cleanLogString(rScript));
			retCon.voidEval(rScript);
		} catch (RserveException e) {
			classLogger.error("Failed to execute R script via voidEval. Script='{}'.", Utility.cleanLogString(rScript),
					e);
		}
	}

	public RConnection getRcon() {
		RConnection rConTemp = null;
		// see if there is a user
		if (this.insight.getUser() != null) {
			// is a r connection already there, return it
			if (this.insight.getUser().getRcon() != null) {
				classLogger.info("Retrieving existing R Connection...");
				rConTemp = this.insight.getUser().getRconRemote().getConnection();
			}
			// else set it
			else {
				classLogger.info("R Connection has not been defined yet...");
				classLogger.info("Starting R Connection... ");
				RRemoteRserve rTemp = new RRemoteRserve();
				this.insight.getUser().setRconRemote(rTemp);
			}
		}
		// maybe there is something in the insight
		else if (this.insight != null) {
			classLogger.info("Retrieving existing R Connection...");
			NounMetadata noun = this.insight.getVarStore().get(R_CONN);
			if (noun != null) {
				rConTemp = (RConnection) this.insight.getVarStore().get(R_CONN).getValue();
			}
		}
		// if there is no user or insight associated, just send back a new RCon that
		// will be a fresh rserve space
		else {
			classLogger.info("R Connection has not been defined yet...");
			classLogger.info("Starting R Connection... ");
			RRemoteRserve rTemp = new RRemoteRserve();
			rConTemp = rTemp.getConnection();
		}
		return rConTemp;
	}

}
