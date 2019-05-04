package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.RFactor;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RserveUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RJavaUserRserveTranslator extends AbstractRJavaTranslator {

	private IRUserConnection rcon;
	
	
	////////////////////////////////////////
	// Starting R
	////////////////////////////////////////
	@Override
	public void startR() {
		
		// Try to start R
		try {
			
			// First define the rcon 
			if (userRconIsDefined()) {
				rcon = this.insight.getUser().getRcon();
			} else {
				if (userIsDefined()) {
					this.insight.getUser().setRcon(IRUserConnection.getRUserConnection(getRDataFile()));
					rcon = this.insight.getUser().getRcon();
				} else {
					rcon = IRUserConnection.getRUserConnection();
				}
				
				// Then initialize
				rcon.initializeConnection();
				rcon.loadDefaultPackages();
				initREnv(); // TODO>>>timb: R - why do we need these? They are not called in recover methods of the RUserConnection framework (later)
				setMemoryLimit();
			}
		} catch (Exception e) {
			
			// If r fails to start, don't preserve the improperly loaded object
			if (rcon != null) {
				try {
					rcon.stopR();
				} catch (Exception ignore) {
					// Nothing to do here
				} finally {
					this.insight.getUser().setRcon(null);
				}
			}
			throw new IllegalArgumentException("Failed to start R: " +  e.getMessage(), e);
		}
	}

	private boolean userRconIsDefined() {
		return userIsDefined() && this.insight.getUser().getRcon() != null;
	}
	
	private boolean userIsDefined() {
		return this.insight != null && this.insight.getUser() != null;
	}
	
	private String getUserInfo() {
		User user = this.insight.getUser();
		AuthProvider primaryProvider = user.getPrimaryLogin();
		AccessToken token = user.getAccessToken(primaryProvider);
		String userInfo = primaryProvider.name() + "___" + token.getName().replaceAll(" ", "_");
		return userInfo; 
	}
	
	private String getRDataFile() {
		if (userIsDefined()) {
			if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.USER_WORKSPACE))) {
				String assetDirectory = WorkspaceAssetUtils.getUserAssetRootDirectory(this.insight.getUser(), this.insight.getUser().getPrimaryLogin());
				if (assetDirectory != null && new File(assetDirectory).isDirectory()) {
					String rDataDirectory = assetDirectory + "/" + "RData";
					new File(rDataDirectory).mkdir();
					return RserveUtil.getRDataFile(rDataDirectory, getUserInfo());
				}
			}
			return RserveUtil.getRDataFile(getUserInfo());
		} else {
			return RserveUtil.getRDataFile("anonymous");
		}
	}
	
	@Override
	public void initREnv() {
		try {
			rcon.eval("if(!exists(\"" + this.env + "\")) {" + this.env  + "<- new.env();}");
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to initialize R environment.", e);
		}
	}
	
	
	////////////////////////////////////////
	// R evaluations
	////////////////////////////////////////
	@Override
	public Object executeR(String rScript) {
		return rcon.eval(rScript);
	}
	
	@Override
	public void executeEmptyR(String rScript) {
		rcon.voidEval(rScript);
	}
	
	
	////////////////////////////////////////
	// Cancellation
	////////////////////////////////////////
	@Override
	public boolean cancelExecution() {
		return false;
		// TODO >>>timb: R - need to complete cancellation here (later)
	}
	
	
	////////////////////////////////////////
	// Raw R connection
	////////////////////////////////////////
	// TODO >>>timb: R - should get rid of this (later)
	@Deprecated
	public RConnection getConnection() {
		return rcon.getRConnection();
	}
	
	
	////////////////////////////////////////
	// Data types
	////////////////////////////////////////
	@Override
	public String getString(String rScript) {
		try {
			return rcon.eval(rScript).asString();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a string.");
		}
	}

	@Override
	public String[] getStringArray(String rScript) {
		try {
			return rcon.eval(rScript).asStrings();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a string array.");
		}
	}
	
	/**
	 * This method is used to get the column types of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		String script = "sapply(" + frameName + ", class);";
		REXP val = rcon.eval(script);
		try {
			 return val.asStrings();
		} catch (REXPMismatchException e) {
			// ignore
		}
		
		try {
			RList list = val.asList();
			int size = list.size();
			String[] arr = new String[size];
			for(int i = 0; i < size; i++) {
				Object v = list.get(i);
				if(v instanceof REXP) {
					try {
						REXP vRexp = (REXP) v;
						arr[i] = vRexp.asString();
					}  catch (REXPMismatchException e) {
						// could be an array
						// actually, seems like RServe 
						// will give you the first element
						// of an array if you use asString
						// even if asStrings() works
						REXP vRexp = (REXP) v;
						arr[i] = vRexp.asStrings()[0];
					}
				} else {
					arr[i] = v.toString();
				}
			}
			
			return arr;
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int getInt(String rScript) {
		try {
			return rcon.eval(rScript).asInteger();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to an integer.");
		}
	}

	@Override
	public int[] getIntArray(String rScript) {
		try {
			return rcon.eval(rScript).asIntegers();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to an integer array.");
		}
	}

	@Override
	public double getDouble(String rScript) {
		try {
			return rcon.eval(rScript).asDouble();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double.");
		}
	}

	@Override
	public double[] getDoubleArray(String rScript) {
		try {
			return rcon.eval(rScript).asDoubles();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double array.");
		}
	}

	@Override
	public double[][] getDoubleMatrix(String rScript) {
		try {
			return rcon.eval(rScript).asDoubleMatrix();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a double matrix.");
		}
	}

	@Override
	public boolean getBoolean(String rScript) {
		try {
			return rcon.eval(rScript).asInteger() == 1;
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a inter.");
		}
	}

	@Override
	public Object getFactor(String rScript) {
		try {
			return rcon.eval(rScript).asFactor();
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to a factor.");
		}
	}

	@Override
	public void setConnection(RConnection connection) {
		// This is not necessary as connection is allocated per user
	}

	@Override
	public void setPort(String port) {
		// This is not necessary as port is allocated per user
	}

	@Override
	/**
	 * Stops all r processes.
	 */
	public void endR() {
		try {
			IRUserConnection.endR();
		} catch (Exception e) {
			logger.warn("Unable to end R.", e);
		}
	}

	@Override
	public void stopRProcess() {
		try {
			rcon.detach();
		} catch (Exception e) {
			logger.warn("Unable to stop R process.", e);
		}
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		try {
			double[] breaks;
			@SuppressWarnings("unchecked")
			Map<String, Object> histJ = (Map<String, Object>) (rcon.eval(script).asNativeJavaObject());
			if (histJ.get("breaks") instanceof int[]) {
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
		} catch (REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
		}
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<Object[]>();

		int numCols = colNames.length;
		for (int i = 0; i < numCols; i++) {
			String script = framename + "$" + colNames[i];
			REXP val = rcon.eval(script);

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
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
				// in case values cannot be doubles
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
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
				// in case values cannot be put into an array
				// for an integer
				try {
					int row = val.asInteger();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException e) {
					logger.debug(e);
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
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
				// for a string
				try {
					String row = val.asString();
					if (dataMatrix.isEmpty()) {
						initEmptyMatrix(dataMatrix, 1, numCols);
					}
					dataMatrix.get(0)[i] = row;
					continue;
				} catch (REXPMismatchException e) {
					logger.debug(e);
				}
			}
		}

		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);

		return retMap;
	}

	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		return getBulkDataRow(rScript, headerOrdering).get(0);
	}

	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		List<Object[]> retArr = new Vector<Object[]>(500);

		REXP rs = rcon.eval(rScript);
		try {
			RList result = rs.asList();
			
			// match the returns based on index
			int numColumns = headerOrdering.length;
			int[] headerIndex = new int[numColumns];
			for(int i = 0; i < numColumns; i++) {
				headerIndex[i] = result.names.indexOf(headerOrdering[i]);
			}
			
			for(int colNum = 0; colNum < numColumns; colNum++) {
				// grab the right column index
				int columnIndex = headerIndex[colNum];
				// each column comes back as an array
				// need to first initialize my return matrix
				REXP val = (REXP) result.get(columnIndex);
				if(val.isFactor()) {
					RFactor data = val.asFactor();
					if(retArr.size() == 0) {
						for(int i = 0; i < data.size(); i++) {
							Object[] values = new Object[numColumns];
							values[colNum] = data.at(i);
							retArr.add(values);
						}
					} else {
						for(int i = 0; i < data.size(); i++) {
							Object[] values = retArr.get(i);
							values[colNum] = data.at(i);
						}
					}
				} else if(val.isInteger()) {
					// see if the integer is pointing to a factor
					// NOTE ::: CAN NEVER GET .asFactor() to return true when it is a factor...
					RList attributeList = null;
					boolean isFactor = false;
					if(val._attr() != null) {
						attributeList = val._attr().asList();
						if(attributeList != null) {
							isFactor = attributeList.names.contains("levels");
						}
					}
					int[] data = val.asIntegers();
					boolean[] na = val.isNA();
					if(isFactor) {
						String[] levels = ((REXP) attributeList.get("levels")).asStrings();
						if(retArr.size() == 0) {
							for(int i = 0; i < data.length; i++) {
								Object[] values = new Object[numColumns];
								if(na[i]) {
									// keep it as null
									retArr.add(values);
									continue;
								}
								values[colNum] = levels[data[i]-1];
								retArr.add(values);
							}
						} else {
							for(int i = 0; i < data.length; i++) {
								Object[] values = retArr.get(i);
								if(na[i]) {
									// keep it as null
									continue;
								}
								values[colNum] = levels[data[i]-1];
							}
						}
					} else {
						if(retArr.size() == 0) {
							for(int i = 0; i < data.length; i++) {
								Object[] values = new Object[numColumns];
								if(na[i]) {
									// keep it as null
									retArr.add(values);
									continue;
								}
								values[colNum] = data[i];
								retArr.add(values);
							}
						} else {
							for(int i = 0; i < data.length; i++) {
								Object[] values = retArr.get(i);
								if(na[i]) {
									// keep it as null
									continue;
								}
								values[colNum] = data[i];
							}
						}
					}
				} else if(val.isNumeric()) {
					double[] data = val.asDoubles();
					boolean[] na = val.isNA();
					if(retArr.size() == 0) {
						for(int i = 0; i < data.length; i++) {
							Object[] values = new Object[numColumns];
							if(na[i]) {
								// keep it as null
								retArr.add(values);
								continue;
							}
							values[colNum] = data[i];
							retArr.add(values);
						}
					} else {
						for(int i = 0; i < data.length; i++) {
							Object[] values = retArr.get(i);
							if(na[i]) {
								// keep it as null
								continue;
							}
							values[colNum] = data[i];
						}
					}
				} else if(val.isString()) {
					String[] data = val.asStrings();
					if(retArr.size() == 0) {
						for(int i = 0; i < data.length; i++) {
							Object[] values = new Object[numColumns];
							values[colNum] = data[i];
							retArr.add(values);
						}
					} else {
						for(int i = 0; i < data.length; i++) {
							Object[] values = retArr.get(i);
							values[colNum] = data[i];
						}
					}
				} else if(val.isFactor()) {
					RFactor data = val.asFactor();
					if(retArr.size() == 0) {
						for(int i = 0; i < data.size(); i++) {
							Object[] values = new Object[numColumns];
							values[colNum] = data.at(i);
							retArr.add(values);
						}
					} else {
						for(int i = 0; i < data.size(); i++) {
							Object[] values = retArr.get(i);
							values[colNum] = data.at(i);
						}
					}
				}
			}
		} catch(REXPMismatchException e) {
			throw new IllegalArgumentException("R did not evaluate to the proper data type.", e);
		}
		
		return retArr;
	}
}
