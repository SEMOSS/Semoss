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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.Utility;

public class TCPRTranslator extends AbstractRJavaTranslator {

	private static final Logger classLogger = LogManager.getLogger(TCPRTranslator.class);

	private SocketClient nc = null;
	private boolean started = false;
	private boolean insightSet = false;

	/**
	 * 
	 * @param nc
	 */
	public void setClient(SocketClient nc) {
		this.nc = nc;
	}

	@Override
	public void initREnv(String env) {
		this.env = env;
		if (nc != null && !started) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, env);
			ps.payloadClasses = new Class[] { String.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			}
		}
	}

	@Override
	public void startR() {
		if (nc != null && !started) {
			// initialize the environment
			initREnv(this.env);
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName);
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			} else if (ps != null) {
				started = true;
			}
		}
	}

	@Override
	public Object executeR(String rScript) {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = constructPayload(methodName, rScript);
		ps.payloadClasses = new Class[] { String.class };
		if (nc != null) {
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return ps.payload[0];
			} else if (ps != null) {
				logger.info(ps.ex);
			}
		}

		return null;
	}

	@Override
	public void executeEmptyR(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			logger.info(" >>> Running Script " + rScript);

			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);

			if (ps != null && ps.ex != null) {
				logger.info(Utility.cleanLogString(ps.ex));
			}
		}
	}

	@Override
	public boolean cancelExecution() {
		return false;
	}

	@Override
	public void runR(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps.longRunning = true;
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(Utility.cleanLogString(ps.ex));
			}
		}
	}

	@Override
	public String runRAndReturnOutput(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.longRunning = true;
			ps.payloadClasses = new Class[] { String.class };
			PayloadStruct retPS = (PayloadStruct) nc.executeCommand(ps);
			if (retPS.processed) {
				return retPS.payload[0] + "";
			} else {
				return " Script " + ps.payload[0] + " Failed with " + retPS.ex;
			}
		}
		return null;
	}

	@Override
	public String getString(String script) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return ps.payload[0] + "";
			} else if (ps != null) {
				return ps.ex + "";
			}
		}
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return (String[]) ps.payload[0];
			}
			if (ps != null) {
				logger.info(Utility.cleanLogString(ps.ex));
			}
		}
		return null;
	}

	@Override
	public int getInt(String script) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return (Integer) ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return 0;
	}

	@Override
	public int[] getIntArray(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return (int[]) ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return null;
	}

	@Override
	public double getDouble(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return (Double) ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return 0;
	}

	@Override
	public double[] getDoubleArray(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return (double[]) ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return null;
	}

	@Override
	public double[][] getDoubleMatrix(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();

			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				return (double[][]) ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return null;
	}

	@Override
	public boolean getBoolean(String rScript) {

		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };

			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				logger.info("Set the insight");
				return (Boolean) ps.payload[0];
			} else if (ps != null) {
				// need a way to throw exception
			}
		}
		return false;
	}

	@Override
	public Object getFactor(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { Insight.class };

			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				logger.info("Set the insight");
				return ps.payload[0];
			} else if (ps != null) {
				// need a way to throw exception
			}
		}
		return null;
	}

	@Override
	public void setInsight(Insight insight) {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		if (nc != null && !insightSet) {
			PayloadStruct ps = constructPayload(methodName, insight);
			ps.payloadClasses = new Class[] { Insight.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex == null) {
				logger.info("Set the insight");
				insightSet = true;
			}
		}
		this.insight = insight;
	}

	@Override
	public void setConnection(RConnection connection) {
		// no use
	}

	@Override
	public void setPort(String port) {
		// no use
	}

	@Override
	public void endR() {
		// dont know what I need to do here but..
	}

	@Override
	public void stopRProcess() {

	}

	@Override
	public void executeEmptyRDirect(String rScript) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info("Exception " + Utility.cleanLogString(ps.ex));
			}
		}
	}

	@Override
	Object executeRDirect(String rScript) {
		if (nc != null) {

			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info("Exception " + ps.ex);
			} else if (ps != null) {
				return ps.payload[0];
			}
		}
		return null;
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info("Exception " + ps.ex);
			} else if (ps != null) {
				return (Map<String, Object>) ps.payload[0];
			}
		}
		return null;
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, framename, colNames);
			ps.payloadClasses = new Class[] { String.class, String[].class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info("Exception " + ps.ex);
			} else if (ps != null) {
				return (Map<String, Object>) ps.payload[0];
			}
		}
		return null;
	}

	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript, headerOrdering);
			ps.payloadClasses = new Class[] { String.class, String[].class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info("Exception " + Utility.cleanLogString(ps.ex));
			} else if (ps != null) {
				return (Object[]) ps.payload[0];
			}
		}
		return null;
	}

	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript, headerOrdering);
			ps.payloadClasses = new Class[] { String.class, String[].class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info("Exception " + Utility.cleanLogString(ps.ex));
			} else if (ps != null) {
				return (List<Object[]>) ps.payload[0];
			}
		}
		return null;
	}

	@Override
	public String[] getColumnTypes(String frameName) {
		if (nc != null) {

			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);

			if (ps != null && ps.ex == null) {
				String[] retString = (String[]) ps.payload[0];
				if (retString == null) {
					classLogger.warn("Ret string is null for frame {}", frameName);
				}
				return retString;
			} else if (ps != null) {
				logger.info(ps.ex);
			}
		}
		return null;
	}

	@Override
	public void initREnv() {
		if (nc != null && !started) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, env);
			ps.payloadClasses = new Class[] { String.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			}
		}
	}

	@Override
	public boolean isEmpty(String frameName) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			} else if (ps != null) {
				return (Boolean) ps.payload[0];
			}
		}

		return false;
	}

	@Override
	public boolean varExists(String varname) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, varname);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			} else if (ps != null) {
				return (Boolean) ps.payload[0];
			}
		}
		return false;
	}

	@Override
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, columnName, typeToConvert);
			ps.payloadClasses = new Class[] { String.class, String.class, SemossDataType.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			}
		}

	}

	@Override
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert,
			SemossDataType currentType) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, columnName, typeToConvert, currentType);
			ps.payloadClasses = new Class[] { String.class, String.class, SemossDataType.class, SemossDataType.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			}
		}

	}

	@Override
	public String getColumnType(String frameName, String column) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, column);
			ps.payloadClasses = new Class[] { String.class, String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			} else if (ps != null) {
				String output = (String) ps.payload[0];
				if (output == null) {
					classLogger.warn("Ret string is null for frame {} column {}", frameName, column);
				}
				return output;
			}
		}
		return null;
	}

	@Override
	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType,
			String dateFormat) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, colName, newType, dateFormat);
			ps.payloadClasses = new Class[] { String.class, String.class, String.class, String.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			}
		}
	}

	@Override
	public int getNumRows(String frameName) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName);
			ps.payloadClasses = new Class[] { String.class };
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			} else if (ps != null) {
				return (Integer) ps.payload[0];
			}
		}
		return 0;
	}

	@Override
	public void initEmptyMatrix(List<Object[]> matrix, int numRows, int numCols) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, matrix, numRows, numCols);
			ps.payloadClasses = new Class[] { matrix.getClass(), Integer.class, Integer.class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			}
		}

	}

	@Override
	public void checkPackages(String[] packages) {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, new Object[] { packages });
			ps.payloadClasses = new Class[] { String[].class };
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(Utility.cleanLogString(ps.ex));
			}
		}

	}

	@Override
	public boolean checkPackages(String[] packages, Logger logger) {
		// we cannot do this I dont think
		return false;
	}

	@Override
	protected void setMemoryLimit() {
		if (nc != null) {
			String methodName = new Object() {
			}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName);
			ps.payloadClasses = new Class[] {};
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			if (ps != null && ps.ex != null) {
				logger.info(ps.ex);
			}
		}
	}

	private PayloadStruct constructPayload(String methodName, Object... objects) {
		// go through the objects and if they are set to null then make them as string
		// null
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.R;
		ps.methodName = methodName;
		ps.payload = objects;
		ps.env = this.env;
		return ps;
	}

}
