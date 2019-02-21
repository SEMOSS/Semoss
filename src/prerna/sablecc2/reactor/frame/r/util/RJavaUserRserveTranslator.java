package prerna.sablecc2.reactor.frame.r.util;

import java.util.List;
import java.util.Map;

import org.rosuda.REngine.Rserve.RConnection;

public class RJavaUserRserveTranslator extends AbstractRJavaTranslator{

	@Override
	public void initREnv() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startR() {
		// TODO Auto-generated method stub
	}

	@Override
	public Object executeR(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void executeEmptyR(String rScript) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getString(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getStringArray(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getInt(String script) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int[] getIntArray(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getDouble(String rScript) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double[] getDoubleArray(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double[][] getDoubleMatrix(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean(String rScript) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object getFactor(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConnection(RConnection connection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPort(String port) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endR() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopRProcess() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getHistogramBreaksAndCounts(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> flushFrameAsTable(String framename, String[] colNames) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		// TODO Auto-generated method stub
		return null;
	}
	

}
