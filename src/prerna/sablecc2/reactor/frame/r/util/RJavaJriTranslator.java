package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
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
		logger.info("Connection right now is set to: " + retEngine);

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
}
