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
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RJavaRemoteRserveTranslator extends RJavaRserveTranslator {

	private static final Logger logger = LogManager.getLogger(RJavaRemoteRserveTranslator.class);

	private static final String STACKTRACE = "StackTrace: ";

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	RJavaRemoteRserveTranslator() {

	}

	@Override
	public void startR() {
		if(this.insight != null) {
			NounMetadata noun = this.insight.getVarStore().get(R_CONN);
			if (noun != null) {
				retCon = (RConnection) this.insight.getVarStore().get(R_CONN).getValue();
			}
			NounMetadata nounPort = this.insight.getVarStore().get(R_PORT);
			if (nounPort != null) {
				port = (String) nounPort.getValue();
			}
		}

		if(this.insight.getUser() != null){
			if(this.insight.getUser().getRcon() !=null){
				retCon = this.insight.getUser().getRconRemote().getConnection();
			}
		}

		if(this.retCon == null) {
			logger.info("R Connection has not been defined yet...");
		} else {
			logger.info("Retrieving existing R Connection...");
		}

		if (this.retCon == null) {
			try {
				logger.info("Starting R Connection... ");
				if(this.insight!= null){
					if(this.insight.getUser() != null){
						if(this.insight.getUser().getRcon() == null){
							RRemoteRserve rTemp = new RRemoteRserve();
							this.insight.getUser().setRconRemote(rTemp);
						}
						this.retCon = this.insight.getUser().getRconRemote().getConnection();
					}
				}else{
					RRemoteRserve rTemp = new RRemoteRserve();
					this.retCon = rTemp.getConnection();
				}
				logger.info("Successfully created R Connection... ");

				//				port = Utility.findOpenPort();
				//				logger.info("Starting it on port.. " + port);
				//				// need to find a way to get a common name
				//				masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
				//				retCon = new RConnection("127.0.0.1", Integer.parseInt(port));

				if(retCon == null) {
					throw new NullPointerException(
							"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
									+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
				}
				
				// load all the libraries
				retCon.eval("library(splitstackshape);");
				logger.info("Loaded packages splitstackshape");
				// data table
				retCon.eval("library(data.table);");
				logger.info("Loaded packages data.table");
				// reshape2
				retCon.eval("library(reshape2);");
				logger.info("Loaded packages reshape2");
				// stringr
				retCon.eval("library(stringr)");
				logger.info("Loaded packages stringr");
				// lubridate
				retCon.eval("library(lubridate);");
				logger.info("Loaded packages lubridate");
				// dplyr
				retCon.eval("library(dplyr);");
				logger.info("Loaded packages dplyr");

				if(this.insight != null) {
					this.insight.getVarStore().put(AbstractBaseRClass.R_CONN, new NounMetadata(retCon, PixelDataType.R_CONNECTION));
					this.insight.getVarStore().put(AbstractBaseRClass.R_PORT, new NounMetadata(port, PixelDataType.CONST_STRING));
				}

			} catch (Exception e) {
				logger.error(
						"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
								+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
				logger.error(STACKTRACE, e);
				throw new IllegalArgumentException(
						"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
								+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
			}
		}
		// initialize the r environment
		initREnv();
	}

	private void transferToServer( String clientFile, String serverFile ){
		RConnection r = getRcon();
		byte [] b = new byte[8192];
		BufferedInputStream clientStream = null;
		RFileOutputStream serverStream = null;
		try{
			/* the file on the client machine we read from */
			 clientStream = new BufferedInputStream( 
					new FileInputStream( new File( clientFile ) ) ); 

			/* the file on the server we write to */
			 serverStream = r.createFile( serverFile );

			/* typical java IO stuff */
			int c = clientStream.read(b) ; 
			while( c >= 0 ){
				serverStream.write( b, 0, c ) ;
				c = clientStream.read(b) ;
			}

		} catch( IOException e){
			logger.error(STACKTRACE, e);
		} finally {
			if(serverStream != null) {
		          try {
		        	  serverStream.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
			if(clientStream != null) {
		          try {
		        	  clientStream.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
		}
	}

	private void transferToClient( String clientFile, String serverFile ){
		RConnection r = getRcon();
		byte[] b = new byte[8192];
		BufferedOutputStream clientStream = null;
		RFileInputStream serverStream = null;
		try{
			/* the file on the client machine we write to */
			 clientStream = new BufferedOutputStream(
					new FileOutputStream( new File( clientFile ) ) );

			/* the file on the server machine we read from */
			 serverStream = r.openFile( serverFile );

			/* typical java io stuff */
			int c = serverStream.read(b) ; 
			while( c >= 0 ){
				clientStream.write( b, 0, c ) ;
				c = serverStream.read(b) ;
			}

			clientStream.close();
			serverStream.close(); 
		} catch( IOException e){
			logger.error(STACKTRACE, e);
		}finally {
			if(serverStream != null) {
		          try {
		        	  serverStream.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
			if(clientStream != null) {
		          try {
		        	  clientStream.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
		}
	}

	@Override
	public void runR(String script) {
		String insightCacheLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		String csvInsightCacheFolder = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		String baseDir = insightCacheLoc + "\\" + csvInsightCacheFolder + "\\";
		String tempFileLocation = baseDir + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		//write file out on local FS
		File f = new File(tempFileLocation);
		try {
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			logger.error("Error in writing R script for execution!");
			logger.error(STACKTRACE, e1);
		}

		//Copy file over to server
		String fileExtension = FilenameUtils.getExtension(tempFileLocation);
		String newServerFileLoc = "/tmp/" + Utility.getRandomString(15) + "." + fileExtension;
		transferToServer(tempFileLocation, newServerFileLoc);

		//Execute the file with respect to the server file location
		try {
			this.executeEmptyR("source(\"" + newServerFileLoc + "\", local=TRUE)");
		} finally {
			//delete local and server file
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
		String insightCacheLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		String csvInsightCacheFolder = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		String baseDir = insightCacheLoc + DIR_SEPARATOR + csvInsightCacheFolder + DIR_SEPARATOR;
		String tempFileLocation = baseDir + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		String outputLoc = baseDir + Utility.getRandomString(15) + ".txt";
		File outputF = null;
		if (remoteR){
			outputLoc = "/tmp/" + Utility.getRandomString(15) + ".txt";
		}
		outputLoc = outputLoc.replace("\\", "/");

		if (remoteR){
			try {
				if (r != null) {
					r.createFile( outputLoc );
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		} else {
			outputF = new File(outputLoc);
			try {
				outputF.createNewFile();
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}

		String randomVariable = "con" + Utility.getRandomString(6);
		File f = new File(tempFileLocation);
		try {
			script = script.trim();
			if(!script.endsWith(";")) {
				script = script +";";
			}
			script = randomVariable + "<- file(\"" + outputLoc + "\"); sink(" + randomVariable + ", append=TRUE, type=\"output\"); "
					+ "sink(" + randomVariable + ", append=TRUE, type=\"message\"); " + script + " sink();";
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			logger.error("Error in writing R script for execution!");
			logger.error(STACKTRACE, e1);
		}
		String scriptOutput = null;
		if (remoteR){
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

					transferToClient(outputLocLocal,outputLoc);
					outputF = new File(outputLocLocal);
					scriptOutput = FileUtils.readFileToString(outputF);
				} catch (IOException e) {
					logger.error(STACKTRACE, e);
				}
			} finally {
				f.delete();
				if (outputF != null) {
					outputF.delete();
				}
				//					executeEmptyR("file.remove(" + tempFileLocation + ");");
				//					executeEmptyR("file.remove(" + outputLoc + ");");

			}
		} 

		else{
			try {
				String finalScript = "print(source(\"" + tempFileLocation + "\", print.eval=TRUE, local=TRUE)); ";
				this.executeR(finalScript);
				try {
					scriptOutput = FileUtils.readFileToString(outputF);
				} catch (IOException e) {
					logger.error(STACKTRACE, e);
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
			// throw new NullPointerException("Neccesity to trim, scriptOutput cannot be null here.");
			return "";
		}

		// return the final output
		return scriptOutput.trim();
	}

	@Override
	public Object executeR(String rScript) {
		try {
			logger.info("executeR: " + rScript);
			return retCon.eval(rScript);
		} catch (Exception e) {
			logger.error(STACKTRACE, e);
		}
		return null;
	}

	@Override
	public void executeEmptyR(String rScript) {
		try {
			logger.info("executeR: " + Utility.cleanLogString(rScript));
			retCon.voidEval(rScript);
		} catch (RserveException e) {
			logger.error(STACKTRACE, e);
		}
	}

	public RConnection getRcon() {
		RConnection rConTemp = null;
		//see if there is a user
		if(this.insight.getUser() != null){
			//is a r connection already there, return it
			if(this.insight.getUser().getRcon() != null){
				logger.info("Retrieving existing R Connection...");
				rConTemp = this.insight.getUser().getRconRemote().getConnection();
			}
			//else set it
			else{
				logger.info("R Connection has not been defined yet...");
				logger.info("Starting R Connection... ");
				RRemoteRserve rTemp = new RRemoteRserve();
				this.insight.getUser().setRconRemote(rTemp);
			}
		}
		//maybe there is something in the insight
		else if(this.insight != null) {
			logger.info("Retrieving existing R Connection...");
			NounMetadata noun = this.insight.getVarStore().get(R_CONN);
			if (noun != null) {
				rConTemp = (RConnection) this.insight.getVarStore().get(R_CONN).getValue();
			}
		}
		//if there is no user or insight associated, just send back a new RCon that will be a fresh rserve space
		else{
			logger.info("R Connection has not been defined yet...");
			logger.info("Starting R Connection... ");
			RRemoteRserve rTemp = new RRemoteRserve();
			rConTemp = rTemp.getConnection();
		}
		return rConTemp;
	}

}
