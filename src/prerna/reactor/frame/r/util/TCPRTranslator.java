package prerna.reactor.frame.r.util;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.Utility;

public class TCPRTranslator extends AbstractRJavaTranslator {

	private SocketClient nc = null;
	Logger logger = null;
	String port = null;
	Insight insight = null;
	boolean started = false;
	boolean insightSet = false;
	
	/**
	 * 
	 * @param nc
	 */
	public void setClient(SocketClient nc) {
		this.nc = nc;
	}
	
	@Override
	public void initREnv(String env) {
		// TODO Auto-generated method stub
		// need to create the netty client here ?
		this.env = env;
		if(nc != null && !started)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, env);
			ps.payloadClasses = new Class[] {String.class};
			ps.hasReturn = false;
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
		//return output;
	}

	@Override
	public void startR() {
		if(nc != null && !started) {
			// initialize the environment
			initREnv(this.env);
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName);
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null) {
				logger.info(ps.ex);
			} else if(ps != null) {
				started = true;
			}
			
//			// set the memory limit
//			if(started) {
//				setMemoryLimit();
//			}
		}
	}

	@Override
	public Object executeR(String rScript) {
		// TODO Auto-generated method stub
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		PayloadStruct ps = constructPayload(methodName, rScript);
		ps.payloadClasses = new Class[] {String.class};
		if(nc != null)
		{
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				return ps.payload[0];
			}
			else if(ps != null)
				logger.info(ps.ex);
		}
		
		return null;
	}

	@Override
	public void executeEmptyR(String rScript) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			
			logger.info(" >>> Running Script " + rScript);
	
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			ps.hasReturn = false;
			ps = (PayloadStruct) nc.executeCommand(ps);
			
			
			if(ps != null  &&  ps.ex!= null)
				logger.info(Utility.cleanLogString(ps.ex));
		}		
	}

	@Override
	public boolean cancelExecution() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void runR(String rScript) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			ps.longRunning = true;
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
				logger.info(Utility.cleanLogString(ps.ex));
		}		
	}

	@Override
	public String runRAndReturnOutput(String rScript) {
		if(nc != null)
		{
			// TODO Auto-generated method stub
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.longRunning = true;
			ps.payloadClasses = new Class[] {String.class};
			PayloadStruct retPS = (PayloadStruct)nc.executeCommand(ps);
			//System.err.println("Output is " + output);
			if(retPS.processed)
				return retPS.payload[0] + "";
			else
				return " Script " + ps.payload[0] + " Failed with " + retPS.ex;
		}
		return null;
	}

	@Override
	public String runRAndReturnOutput(String rScript, Map appMap) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript, appMap);
			ps.payloadClasses = new Class[] {String.class, Map.class};
			ps.longRunning = true;
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.processed)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return ps.payload[0] + "";
			}
			else if(ps != null && !ps.processed) return ps.payload[0] + "  Failed to execute. Please check syntax ::: " + ps.ex;
		}
		return null;
	}

	@Override
	public String getString(String script) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] {String.class};
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return ps.payload[0] + "";
			}
			else if(ps != null) return ps.ex+ "";
		}
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] {String.class};
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return (String [])ps.payload[0];
			}
			if(ps != null)
				logger.info(Utility.cleanLogString(ps.ex));
		}
		return null;
	}

	@Override
	public int getInt(String script) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] {String.class};
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return (Integer)ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return 0;
	}

	@Override
	public int[] getIntArray(String rScript) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return (int [])ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return null;
	}

	@Override
	public double getDouble(String rScript) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return (Double)ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return 0;
	}

	@Override
	public double[] getDoubleArray(String rScript) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return (double[])ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return null;
	}

	@Override
	public double[][] getDoubleMatrix(String rScript) 
	{
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
	
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			//ps.payload = new Object[] {"2+2"};
			//if(rScript.equalsIgnoreCase("yo"))
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				//System.err.println("Output is " + ps.payload[0]);
				return (double[][])ps.payload[0];
			}
			logger.info(ps.ex);
		}
		return null;
	}

	@Override
	public boolean getBoolean(String rScript) {

		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
	
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				logger.info("Set the insight");
				return (Boolean)ps.payload[0];
			}
			else if(ps != null)
			{
				// need a way to throw exception
			}
		}
		return false;
	}

	@Override
	public Object getFactor(String rScript) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {Insight.class};
	
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				logger.info("Set the insight");
				return ps.payload[0];
			}
			else if(ps != null)
			{
				// need a way to throw exception
			}
		}
		return null;
	}

	@Override
	public void setInsight(Insight insight) {
		// TODO Auto-generated method stub
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
		if(nc != null && !insightSet)
		{
			PayloadStruct ps = constructPayload(methodName, insight);
			ps.payloadClasses = new Class[] {Insight.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex== null)
			{
				logger.info("Set the insight");
				insightSet = true;
			}
		}
		this.insight = insight;

	}
	
	@Override
	public Insight getInsight() {
		return this.insight;

	}

	@Override
	public void setLogger(Logger logger) {
		// TODO Auto-generated method stub
		this.logger = logger;

	}

	@Override
	public void setConnection(RConnection connection) {
		// TODO Auto-generated method stub
		// no use

	}

	@Override
	public void setPort(String port) {
		// TODO Auto-generated method stub
		this.port = port;
		
	}

	@Override
	public void endR() {
		// TODO Auto-generated method stub
		// dont know what I need to do here but.. 
		
	}

	@Override
	public void stopRProcess() {
		// TODO Auto-generated method stub

	}	

	@Override
	public void executeEmptyRDirect(String rScript) {
		if(nc != null)
		{
			// TODO Auto-generated method stub
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info("Exception " + Utility.cleanLogString(ps.ex));
			}
		}
	}

	@Override
	Object executeRDirect(String rScript) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
	
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript);
			ps.payloadClasses = new Class[] {String.class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info("Exception " + ps.ex);
			}
			else if(ps != null)
				return ps.payload[0];
		}		
		return null;
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, script);
			ps.payloadClasses = new Class[] {String.class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info("Exception " + ps.ex);
			}
			else if(ps != null)
				return (Map<String, Object>)ps.payload[0];
		}			
		return null;
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		if(nc != null)
		{

			// TODO Auto-generated method stub
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, framename, colNames);
			ps.payloadClasses = new Class[] {String.class, String[].class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info("Exception " + ps.ex);
			}
			else if(ps != null)
				return (Map<String, Object>)ps.payload[0];
		}		
		return null;
	}

	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript, headerOrdering);
			ps.payloadClasses = new Class[] {String.class, String[].class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info("Exception " + Utility.cleanLogString(ps.ex));
			}
			else if(ps != null)
				return (Object[])ps.payload[0];
		}			
		return null;
	}

	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, rScript, headerOrdering);
			ps.payloadClasses = new Class[] {String.class, String[].class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info("Exception " + Utility.cleanLogString(ps.ex));
			}
			else if(ps != null)
				return (List<Object[]>)ps.payload[0];
		}		
		return null;
	}

	public String[] getColumnTypes(String frameName) 
	{
		if(nc != null)
		{
			
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName);
			ps.payloadClasses = new Class[] {String.class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			
			if(ps != null  &&  ps.ex== null)
			{
				String [] retString = (String [])ps.payload[0];
				if(retString == null)
				{
					System.out.println("Ret string is null for frame ..  " + frameName);
				}
				return retString;
			}
			else if(ps != null)
			{
				logger.info(ps.ex);
			}
		}
		return null;
	}

	@Override
	public void initREnv() {
		// TODO Auto-generated method stub
		if(nc != null && !started)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, env);
			ps.payloadClasses = new Class[] {String.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
	}

	@Override
	public boolean isEmpty(String frameName) {
		// TODO Auto-generated method stub
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName);
			ps.payloadClasses = new Class[] {String.class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
			else if(ps != null) 
				return (Boolean)ps.payload[0];
		}

		return false;
	}

	@Override
	public boolean varExists(String varname) {
		
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, varname);
			ps.payloadClasses = new Class[] {String.class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
			else if(ps != null)
				return (Boolean)ps.payload[0];
		}
		return false;
	}

	@Override
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, columnName, typeToConvert);
			ps.payloadClasses = new Class[] {String.class, String.class, SemossDataType.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
		
	}

	@Override
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert,
			SemossDataType currentType) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, columnName, typeToConvert, currentType);
			ps.payloadClasses = new Class[] {String.class, String.class, SemossDataType.class, SemossDataType.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
		
	}

	@Override
	public String getColumnType(String frameName, String column) {
		if(nc != null)
		{
			
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, column);
			ps.payloadClasses = new Class[] {String.class, String.class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
			else if(ps != null)
			{
				String output = (String)ps.payload[0];
				if(output == null)
				{
					System.out.println("Ret string is null for frame ..  " + frameName + "<><>" + column);
				}
				return output;
			}
		}
		return null;
	}

	@Override
	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType,
			String dateFormat) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName, colName, newType, dateFormat);
			ps.payloadClasses = new Class[] {String.class, String.class, String.class, String.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
		
	}

	@Override
	public int getNumRows(String frameName) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, frameName);
			ps.payloadClasses = new Class[] {String.class};
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
			else if(ps != null)
				return (Integer)ps.payload[0];
		}
		return 0;
	}

	@Override
	public void initEmptyMatrix(List<Object[]> matrix, int numRows, int numCols) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, matrix, numRows, numCols);
			ps.payloadClasses = new Class[] {matrix.getClass(), Integer.class, Integer.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
		
	}

	@Override
	public void checkPackages(String[] packages) {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, new Object[] {packages});
			ps.payloadClasses = new Class[] {String[].class};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(Utility.cleanLogString(ps.ex));
			}
		}
		
	}

	@Override
	public boolean checkPackages(String[] packages, Logger logger) {
		// we cannot do this I dont think
		return false;
	}
	
	protected void setMemoryLimit() {
		if(nc != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName);
			ps.payloadClasses = new Class[] {};
			ps.hasReturn = false;
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
	}

	
	private PayloadStruct constructPayload(String methodName, Object...objects )
	{
		// go through the objects and if they are set to null then make them as string null
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.R;
		ps.methodName = methodName;
		ps.payload = objects;
		ps.env = this.env;
		
		return ps;
	}
	

	


	public static void main(String [] args)
	{
		TCPRTranslator tr = new TCPRTranslator();
		tr.getBoolean("abcd");
	}



}
