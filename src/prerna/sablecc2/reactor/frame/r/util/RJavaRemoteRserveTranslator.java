package prerna.sablecc2.reactor.frame.r.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RFileInputStream;
import org.rosuda.REngine.Rserve.RFileOutputStream;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.engine.impl.r.RRemoteRserve;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.runtime.AbstractBaseRClass;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RJavaRemoteRserveTranslator  extends AbstractRJavaTranslator{
	RConnection retCon;
	String port;

	RJavaRemoteRserveTranslator() {

	}


	@Override
	public void startR() {
		if(this.insight != null) {
			NounMetadata noun = (NounMetadata) this.insight.getVarStore().get(R_CONN);
			if (noun != null) {
				retCon = (RConnection) this.insight.getVarStore().get(R_CONN).getValue();
			}
			NounMetadata nounPort = this.insight.getVarStore().get(R_PORT);
			if (nounPort != null) {
				port = (String) nounPort.getValue();
			}
		}

		if(this.insight.getUser() != null){
			if(this.insight.getUser().getRConn() !=null){
				retCon = this.insight.getUser().getRConn();
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
						if(this.insight.getUser().getRConn() == null){
							RRemoteRserve rTemp = new RRemoteRserve();
							this.insight.getUser().setRConn(rTemp.getConnection());
						}
						this.retCon = this.insight.getUser().getRConn();
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

				// initialize the r environment
				initREnv();
			} catch (Exception e) {
				System.out.println(
						"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
								+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
				e.printStackTrace();
				throw new IllegalArgumentException(
						"ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
								+ "1)splitstackshape\n 2)data.table\n 3)reshape2\n 4)stringr\n 5)lubridate\n 6)dplyr");
			}
		}
	}

	public void transferToServer( String client_file, String server_file ){
		RConnection r = getRcon();
		//RConnection r = getConnection();
		byte [] b = new byte[8192];
		try{
			/* the file on the client machine we read from */
			BufferedInputStream client_stream = new BufferedInputStream( 
					new FileInputStream( new File( client_file ) ) ); 

			/* the file on the server we write to */
			RFileOutputStream server_stream = r.createFile( server_file );

			/* typical java IO stuff */
			int c = client_stream.read(b) ; 
			while( c >= 0 ){
				server_stream.write( b, 0, c ) ;
				c = client_stream.read(b) ;
			}
			server_stream.close();
			client_stream.close(); 

		} catch( IOException e){
			e.printStackTrace(); 
		}
	}

	public void transfer_toclient( String client_file, String server_file ){
		RConnection r = getRcon();

		byte [] b = new byte[8192];
		try{

			/* the file on the client machine we write to */
			BufferedOutputStream client_stream = new BufferedOutputStream( 
					new FileOutputStream( new File( client_file ) ) );

			/* the file on the server machine we read from */
			RFileInputStream server_stream = r.openFile( server_file );

			/* typical java io stuff */
			int c = server_stream.read(b) ; 
			while( c >= 0 ){
				client_stream.write( b, 0, c ) ;
				c = server_stream.read(b) ;
			}
			client_stream.close();
			server_stream.close(); 

		} catch( IOException e){
			e.printStackTrace(); 
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
			System.out.println("Error in writing R script for execution!");
			e1.printStackTrace();
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
			//executeEmptyR("file.remove(" + newServerFileLoc + ");");
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
		String baseDir = insightCacheLoc + "\\" + csvInsightCacheFolder + "\\";
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
				r.createFile( outputLoc );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}else {

			outputF = new File(outputLoc);
			try {
				outputF.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
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
			System.out.println("Error in writing R script for execution!");
			e1.printStackTrace();
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

					transfer_toclient(outputLocLocal,outputLoc);
					outputF = new File(outputLocLocal);
					scriptOutput = FileUtils.readFileToString(outputF);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} finally {
				f.delete();
				outputF.delete();
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
					e.printStackTrace();
				}
			} finally {
				f.delete();
				outputF.delete();
			}
		}

		// drop the random con variable
		this.executeEmptyR("rm(" + randomVariable + ")");
		this.executeEmptyR("gc()");

		// return the final output
		return scriptOutput.trim();
	}

	@Override
	public Object executeR(String rScript) {
		try {
			System.out.println("executeR: " + rScript);
			return retCon.eval(rScript);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void executeEmptyR(String rScript) {
		try {
			System.out.println("executeR: " + rScript);
			retCon.voidEval(rScript);
		} catch (RserveException e) {
			e.printStackTrace();
		}
	}

	/*
	@Override
	public String runRAndReturnOutput(String script) {
		RConnection r = getRcon();

		//local file location
		String insightCacheLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		String csvInsightCacheFolder = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		String baseDir = insightCacheLoc + "\\" + csvInsightCacheFolder + "\\";
		String tempFileLocation = baseDir + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		//Local output file location
		File outputLocalF = null;

		String	outputSever = "/tmp/" + Utility.getRandomString(15) + ".txt";

		outputSever = outputSever.replace("\\", "/");

		//Create the file on the server
		try {
			r.createFile( outputSever );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//Local file creation
		String randomVariable = "con" + Utility.getRandomString(6);
		File f = new File(tempFileLocation);
		try {
			script = script.trim();
			if(!script.endsWith(";")) {
				script = script +";";
			}
			script = randomVariable + "<- file(\"" + outputSever + "\"); sink(" + randomVariable + ", append=TRUE, type=\"output\"); "
					+ "sink(" + randomVariable + ", append=TRUE, type=\"message\"); " + script + " sink();";
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			System.out.println("Error in writing R script for execution!");
			e1.printStackTrace();
		}


		String scriptOutput = null;

			//Transfer the local file script to the server to execute
			String fileExtension = FilenameUtils.getExtension(tempFileLocation);
			String newServerFileLoc = "/tmp/" + Utility.getRandomString(15) + "." + fileExtension;
			transferToServer(tempFileLocation, newServerFileLoc);


			try {
				String finalScript = "print(source(\"" + newServerFileLoc + "\", print.eval=TRUE, local=TRUE)); ";
				this.executeR(finalScript);
				try {

					//Grab the server output file and bring it to the local file output
					String outputLocal = baseDir + Utility.getRandomString(15) + ".txt";
					transfer_toclient(outputLocal,outputSever);
					outputLocalF = new File(outputLocal);
					scriptOutput = FileUtils.readFileToString(outputLocalF);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} finally {
				f.delete();
				outputLocalF.delete();
				//					executeEmptyR("file.remove(" + newServerFileLoc + ");");
				//					executeEmptyR("file.remove(" + outputSever + ");");

			}


		// drop the random con variable
		this.executeEmptyR("rm(" + randomVariable + ")");
		this.executeEmptyR("gc()");

		// return the final output
		return scriptOutput.trim();
	}
	 */


	public RConnection getRcon(){

		RConnection rConTemp = null;

		//see if there is a user
		if(this.insight.getUser() != null){
			//is a r connection already there, return it
			if(this.insight.getUser().getRConn() != null){
				logger.info("Retrieving existing R Connection...");
				rConTemp = this.insight.getUser().getRConn();
			}
			//else set it
			else{
				logger.info("R Connection has not been defined yet...");
				logger.info("Starting R Connection... ");
				RRemoteRserve rTemp = new RRemoteRserve();
				this.insight.getUser().setRConn(rTemp.getConnection());
			}
		}
		//maybe there is something in the insight
		else if(this.insight != null) {
			logger.info("Retrieving existing R Connection...");
			NounMetadata noun = (NounMetadata) this.insight.getVarStore().get(R_CONN);
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


	@Override
	public String getString(String script) {
		try {
			return retCon.eval(script).asString();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		try {
			return retCon.eval(script).asStrings();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int getInt(String script) {
		int number = 0;
		try {
			number = retCon.eval(script).asInteger();
			return number;
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return number;
	}

	@Override
	public int[] getIntArray(String script) {
		try {
			return retCon.eval(script).asIntegers();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public double getDouble(String script) {
		double number = 0;
		try {
			number = retCon.eval(script).asDouble();
			return number;
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return number;
	}

	@Override
	public double[] getDoubleArray(String script) {
		try {
			return retCon.eval(script).asDoubles();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public double[][] getDoubleMatrix(String script) {
		try {
			return retCon.eval(script).asDoubleMatrix();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Object getFactor(String script) {
		try {
			return retCon.eval(script).asFactor();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean getBoolean(String script) {
		// 1 = TRUE, 0 = FALSE
		try {
			REXP val = retCon.eval(script);
			if(val != null) {
				return (val.asInteger() == 1);
			}
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		try {
			double[] breaks;
			Map<String, Object> histJ = (Map<String, Object>) (retCon.eval(script).asNativeJavaObject());
			if (histJ.get("breaks") instanceof int[]){
				int[] breaksInt = (int[]) histJ.get("breaks");
				breaks = Arrays.stream(breaksInt).asDoubleStream().toArray();
			} else { 
			breaks = (double[]) histJ.get("breaks");
			}
			int[] counts = (int[]) histJ.get("counts");

			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("breaks", breaks);
			retMap.put("counts", counts);
			return retMap;
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<Object[]>();

		int numCols = colNames.length;
		for (int i = 0; i < numCols; i++) {
			String script = framename + "$" + colNames[i];
			REXP val = (REXP) executeR(script);

			if (val.isNumeric()) {
				// for a double array
				try {
					double[] rows = val.asDoubles();
					int numRows = rows.length;
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, numRows, numCols);
					}
					for (int j = 0; j < numRows; j++) {
						dataMatrix.get(j)[i] = rows[j];
					}
					continue;
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
				//in case values cannot be doubles
				try {
					int[] rows = val.asIntegers();
					int numRows = rows.length;
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, numRows, numCols);
					}
					for (int j = 0; j < numRows; j++) {
						dataMatrix.get(j)[i] = rows[j];
					}
					continue;
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
				//in case values cannot be put into an array
				//for an integer
				try {
					int row = val.asInteger();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}

			} else {
				// for a string array
				try {
					String[] rows = val.asStrings();
					int numRows = rows.length;
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, numRows, numCols);
					}
					for (int j = 0; j < numRows; j++) {
						dataMatrix.get(j)[i] = rows[j];
					}
					continue;
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
				//for a string
				try {
					String row = val.asString();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException rme) {
					rme.printStackTrace();
				}
			}
		}

		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);

		return retMap;
	}

	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		REXP rs = (REXP) executeR(rScript);
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		if(result instanceof Map) {
			return processMapReturn((Map<String, Object>) result, headerOrdering);
		} else if(result instanceof List) {
			String[] returnNames = null;
			try {
				Object namesAttr = rs.getAttribute("names").asNativeJavaObject();
				if(namesAttr instanceof String[]) {
					returnNames = (String[]) namesAttr;
				} else {
					// assume it is single string
					returnNames = new String[]{namesAttr.toString()};
				}
			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
			return processListReturn((List) result, headerOrdering, returnNames);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}
	}

	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		REXP rs = (REXP) executeR(rScript);
		Object[] retArray = null;
		Object result = null;
		try {
			result = rs.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		if(result instanceof Map) {
			retArray =  processMapReturn((Map<String, Object>) result, headerOrdering).get(0);
		} else if(result instanceof List) {
			String[] returnNames = null;
			try {
				Object namesAttr = rs.getAttribute("names").asNativeJavaObject();
				if(namesAttr instanceof String[]) {
					returnNames = (String[]) namesAttr;
				} else {
					// assume it is single string
					returnNames = new String[]{namesAttr.toString()};
				}
			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
			retArray = (Object[]) processListReturn((List) result, headerOrdering, returnNames).get(0);
		} else {
			throw new IllegalArgumentException("Unknown data type returned from R");
		}

		return retArray;
	}

	private List<Object[]> processMapReturn(Map<String, Object> result,  String[] headerOrdering) {
		List<Object[]> retArr = new Vector<Object[]>(500);
		int numColumns = headerOrdering.length;
		for(int idx = 0; idx < numColumns; idx++) {
			Object val = result.get(headerOrdering[idx]);

			if(val instanceof Object[]) {
				Object[] data = (Object[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if(val instanceof double[]) {
				double[] data = (double[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if(val instanceof int[]) {
				int[] data = (int[]) val;
				if(retArr.size() == 0) {
					for(int i = 0; i < data.length; i++) {
						Object[] values = new Object[numColumns];
						values[idx] = data[i];
						retArr.add(values);
					}
				} else {
					for(int i = 0; i < data.length; i++) {
						Object[] values = retArr.get(i);
						values[idx] = data[i];
					}
				}
			} else if (val instanceof String) {
				String data = (String) val;
				if (retArr.size() == 0) {
					Object[] values = new Object[numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object[] values = retArr.get(0);
					values[idx] = data;
				}
			} else if (val instanceof Double) {
				Double data = (Double) val;
				if (retArr.size() == 0) {
					Object [] values = new Object[numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object[] values = retArr.get(0);
					values [idx] = data;
				}	
			} else if (val instanceof Integer){
				Integer data = (Integer) val;
				if (retArr.size() == 0) {
					Object [] values = new Object [numColumns];
					values[idx] = data;
					retArr.add(values);
				} else {
					Object [] values = retArr.get(0);
					values [idx] = data;
				}
			} else {
				logger.info("ERROR ::: Could not identify the return type for this iterator!!!");
			}
		}
		return retArr;
	}

	private List<Object[]> processListReturn(List<Object[]> result, String[] headerOrdering, String[] returnNames) {
		List<Object[]> retArr = new Vector<Object[]>(500);

		// match the returns based on index
		int numHeaders = headerOrdering.length;
		int[] headerIndex = new int[numHeaders];
		for(int i = 0; i < numHeaders; i++) {
			headerIndex[i] = ArrayUtilityMethods.arrayContainsValueAtIndex(returnNames, headerOrdering[i]);
		}

		for(int i = 0; i < numHeaders; i++) {
			// grab the right column index
			int columnIndex = headerIndex[i];
			// each column comes back as an array
			// need to first initize my return matrix
			Object col = result.get(columnIndex);
			if(col instanceof Object[]) {
				Object[] columnResults = (Object[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			} else if(col instanceof double[]) {
				double[] columnResults = (double[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			} else if(col instanceof int[]) {
				int[] columnResults = (int[]) col;
				int numResults = columnResults.length;
				if(retArr.size() == 0) {
					for(int j = 0; j < numResults; j++) {
						Object[] values = new Object[numHeaders];
						values[i] = columnResults[j];
						retArr.add(values);
					}
				} else {
					for(int j = 0; j < numResults; j++) {
						Object[] values = retArr.get(j);
						values[i] = columnResults[j];
					}
				}
			}
		}

		return retArr;
	}

	@Override
	public void setConnection(RConnection connection) {
		if (connection != null) {
			this.retCon = connection;
		}
	}

	public RConnection getConnection() {
		return this.retCon;
	}

	@Override
	public void setPort(String port) {
		if (this.port != null) {
			this.port = port;
		}
	}

	public String getPort() {
		return this.port;
	}

	@Override
	public void endR() {
		// only have 1 connection
		// do not do this..
		//	try {
		//		if(retCon != null) {
		//			retCon.shutdown();
		//		}
		//		// clean up other things
		//		System.out.println("R Shutdown!!");
		//	} catch (Exception e) {
		//		e.printStackTrace();
		//	}
	}

	@Override
	public void initREnv() {
		try {
			if(this.retCon != null) {
				this.retCon.eval("if(!exists(\"" + this.env + "\")) {" + this.env  + "<- new.env();}");
			}
		} catch (RserveException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stopRProcess() {
		// TODO Auto-generated method stub

	}


}
