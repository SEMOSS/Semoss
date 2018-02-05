package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RJavaJriTranslator extends AbstractRJavaTranslator {

	protected Rengine engine;

	/**
	 * This will start R, only if it has not already been started
	 * In this case we are starting an engine for JRI
	 */
	@Override
	public void startR() {
		Rengine retEngine = Rengine.getMainEngine();
		if(retEngine == null) {
			if(this.insight.getVarStore().containsKey(R_ENGINE)) {
				retEngine = (Rengine) this.insight.getVarStore().get(R_ENGINE).getValue();
			}
		}
		if(retEngine == null) {
			logger.info("R Connection has not been defined yet");
		} else {
			logger.info("Connection right now is set to: " + retEngine);
		}
		
		String OS = java.lang.System.getProperty("os.name").toLowerCase();
		if(retEngine == null) {
			try {
				// start the R Engine
				if(OS.contains("mac")) {
					retEngine = new Rengine(new String[]{"--vanilla"}, true, null);
				} else {
					retEngine = new Rengine(null, true, null);
				}
				logger.info("Successfully created engine.. ");

				// load all the libraries
				Object ret = retEngine.eval("library(splitstackshape);");
				if(ret == null) {
					throw new ClassNotFoundException("Package splitstackshape could not be found!");
				} else {
					logger.info("Successfully loaded packages splitstackshape");
				}
				// data table
				ret = retEngine.eval("library(data.table);");
				if(ret == null) {
					throw new ClassNotFoundException("Package data.table could not be found!");
				} else {
					logger.info("Successfully loaded packages data.table");
				}
				// reshape2
				ret = retEngine.eval("library(reshape2);");
				if(ret == null) {
					throw new ClassNotFoundException("Package reshape2 could not be found!");
				} else {
					logger.info("Successfully loaded packages reshape2");
				}

				// Don't load RJDBC if OS is Mac because we'll write to CSV and load into data.table to avoid rJava setup
				if(!OS.contains("mac")) {
					// rjdbc
					ret = retEngine.eval("library(RJDBC);");
					if(ret == null) {
						throw new ClassNotFoundException("Package RJDBC could not be found!");
					} else {
						logger.info("Successfully loaded packages RJDBC");
					}
				}
				// stringr
				ret = retEngine.eval("library(stringr);");
				if(ret == null) {
					throw new ClassNotFoundException("Package stringr could not be found!");
				} else {
					logger.info("Successfully loaded packages stringr");
				}
				
				// set the rengine
				this.insight.getVarStore().put(IRJavaTranslator.R_ENGINE, new NounMetadata(retEngine, PixelDataType.R_ENGINE));
			} catch(NullPointerException e) {
				e.printStackTrace();
				System.out.println("Could not connect to R JRI.  Please make sure paths are accurate");
				throw new IllegalArgumentException("Could not connect to R JRI.  Please make sure paths are accurate");
			} catch(ClassNotFoundException e) {
				System.out.println("ERROR ::: " + e.getMessage() + "\nMake sure you have all the following libraries installed:\n"
						+ "1)splitstackshape\n"
						+ "2)data.table\n"
						+ "3)reshape2\n"
						+ "4)RJDBC*\n"
						+ "5)stringr\n\n"
						+ "*Please note RJDBC might require JAVA_HOME environment path to be defined on your system.");
				e.printStackTrace();
				throw new IllegalArgumentException("ERROR ::: " + e.getMessage() + "\nMake sure you have all the following libraries installed:\n"
						+ "1)splitstackshape\n"
						+ "2)data.table\n"
						+ "3)reshape2\n"
						+ "4)RJDBC*\n"
						+ "5)stringr\n\n"
						+ "*Please note RJDBC might require JAVA_HOME environment path to be defined on your system.");
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		engine = retEngine;
	}

	@Override
	public Object executeR(String rScript) {
		try {
			REXP rexp = engine.eval(rScript);
			if(rexp == null) {
				logger.info("Hmmm... REXP returned null for script = " + rScript);
			}
			return rexp;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getString(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asString();
		}
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asStringArray();
		}
		return null;
	}

	@Override 
	public int getInt(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asInt();
		}
		return 0;
	}

	@Override
	public int[] getIntArray(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asIntArray();
		}
		return null;
	}

	@Override
	public double getDouble(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asDouble();
		}
		return 0;
	}

	@Override
	public double[] getDoubleArray(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asDoubleArray();
		}
		return null;
	}
	
	@Override
	public double[][] getDoubleMatrix(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asDoubleMatrix();
		}
		return null;
	}

	@Override
	public Object getFactor(String script) {
		REXP val = engine.eval(script);
		if(val != null) {
			return val.asFactor();
		}
		return null;
	}
	
	@Override
	public double[] getHistogramBreaks(String script) {
		REXP histR = (REXP)engine.eval(script);
		if (histR != null) {
			RVector vectorR = histR.asVector();
			double[] breaks = vectorR.at("breaks").asDoubleArray();
			return breaks;
		}
		return null;
	}
	
	@Override
	public int[] getHistogramCounts(String script) {
		REXP histR = (REXP)engine.eval(script);
		if (histR != null) {
			RVector vectorR = histR.asVector();
			int[] counts = vectorR.at("counts").asIntArray();
			return counts;
		}
		return null;
	}
	
	@Override
	public Map<String, Object> flushObjectAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<Object[]>();
		
		int numCols = colNames.length;
		for (int i = 0; i < numCols; i++) {
			String script = framename + "$" + colNames[i];
			REXP val = (REXP) executeR(script);
			int typeInt = val.getType();
			
			if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				double[] rows = val.asDoubleArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				int[] rows = val.asIntArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if (typeInt == REXP.XT_ARRAY_STR) {
				String[] rows = val.asStringArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if(typeInt == REXP.XT_INT) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asInt();
			} else if(typeInt == REXP.XT_DOUBLE) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asDouble();
			} else if(typeInt == REXP.XT_STR) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asString();
			}
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);
		
		return retMap; 
	}

	@Override
	public Object parseAndEvalScript(String script) {
		return engine.eval(script);
	}

	@Override
	public void setConnection(RConnection connection) {
		logger.info("JRI does not use RConnection object");
	}

	@Override
	public void setPort(String port) {
		logger.info("JRI does not require a port");
	}
	
	@Override
	public void endR() {
		// java.lang.System.setSecurityManager(curManager);
		if (engine != null) {
			engine.end();
		}
		// clean up other things
		System.out.println("R Shutdown!!");
		// java.lang.System.setSecurityManager(reactorManager);
	}
	
	@Override
	public void runR(String script)
	{
		runR(script, true);
	}

	public void runR(String script, boolean result) {
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		File f = new File(tempFileLocation);
		try {
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			System.out.println("Error in writing R script for execution!");
			e1.printStackTrace();
		}

		try {
			REXP output = (REXP) this.executeR("paste( capture.output(print( source(\"" + tempFileLocation + "\")$value ) ), collapse='\n')");
			if (output.getType() == REXP.XT_NULL) {
				throw new IllegalArgumentException("Unable to wrap method in paste/capture");
			}
			if (result) {
				System.out.println(output.asString());
			}
		} catch (Exception e) {
			try {
				Object output = this.executeR(script);
				if (result) {
					java.lang.System.out.println("RCon data.. " + output);
					StringBuilder builder = new StringBuilder();
					getResultAsString(output, builder);
					System.out.println("Output : " + builder.toString());
				}
			} catch (Exception e2) {
				e2.printStackTrace();
				String errorMessage = null;
				if (e2.getMessage() != null && !e2.getMessage().isEmpty()) {
					errorMessage = e2.getMessage();
				} else {
					errorMessage = "Unexpected error in execution of R routine ::: " + script;
				}
				throw new IllegalArgumentException(errorMessage);
			}
		} finally {
			f.delete();
		}
	}
	
	
	private void getResultAsString(Object output, StringBuilder builder)
	{
		// Generic vector..
		if(output instanceof REXPGenericVector) 
		{			
			org.rosuda.REngine.RList list = ((REXPGenericVector)output).asList();
			
			String[] attributeNames = getAttributeArr(((REXPGenericVector)output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if(attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if(!matchesRows) {
					if(attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if(attributeNames.length > 1){
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for(int listIndex = 0; listIndex < size; listIndex++) {
				if(matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}
		
		// List..
		else if(output instanceof REXPList) {
			org.rosuda.REngine.RList list = ((REXPList)output).asList();
			
			String[] attributeNames = getAttributeArr(((REXPList)output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if(attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if(!matchesRows) {
					if(attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if(attributeNames.length > 1){
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for(int listIndex = 0; listIndex < size; listIndex++) {
				if(matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}
		
		// Integers..
		else if(output instanceof REXPInteger)
		{
			int [] ints =  ((REXPInteger)output).asIntegers();
			if(ints.length > 1)
			{
				for(int intIndex = 0;intIndex < ints.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(ints[intIndex]);
					} else {
						builder.append(" ").append(ints[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(ints[0]);
			}
		}
		
		// Doubles.. 
		else if(output instanceof REXPDouble)
		{
			double [] doubles =  ((REXPDouble)output).asDoubles();
			if(doubles.length > 1)
			{
				for(int intIndex = 0;intIndex < doubles.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(doubles[intIndex]);
					} else {
						builder.append(" ").append(doubles[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(doubles[0]);
			}
		}
		
		// Strings..
		else if(output instanceof REXPString)
		{
			String [] strings =  ((REXPString)output).asStrings();
			if(strings.length > 1)
			{				
				for(int intIndex = 0;intIndex < strings.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(strings[intIndex]);
					} else {
						builder.append(" ").append(strings[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(strings[0]);
			}
		}
		
		// JRI
		else if(output instanceof org.rosuda.JRI.REXP) {
			int typeInt = ((org.rosuda.JRI.REXP) output).getType();
			if(typeInt == REXP.XT_DOUBLE) {
				builder.append( ((org.rosuda.JRI.REXP) output).asDouble()  );
				
			} else if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asDoubleArray() ));

			} else if(typeInt == REXP.XT_ARRAY_DOUBLE + 1) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asDoubleMatrix() ));

			} else if(typeInt == REXP.XT_INT) {
				builder.append( ((org.rosuda.JRI.REXP) output).asInt()  );
				
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asIntArray() ));
				
			} else if(typeInt == REXP.XT_STR) {
				builder.append( ((org.rosuda.JRI.REXP) output).asString()  );
				
			} else if(typeInt == REXP.XT_ARRAY_STR) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asStringArray() ));
				
			} else if(typeInt == REXP.XT_BOOL) {
				builder.append( ((org.rosuda.JRI.REXP) output).asBool()  );
				
			} else if(typeInt == REXP.XT_ARRAY_BOOL) {
				
			} else if(typeInt == REXP.XT_LIST) {
				builder.append( ((org.rosuda.JRI.REXP) output).asString()  );
				
			} else if(typeInt == REXP.XT_VECTOR) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asStringArray() ));
				
			} 
			
		}
		
		builder.append("\n");
	}
}
