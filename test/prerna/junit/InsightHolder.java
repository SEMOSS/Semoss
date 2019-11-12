package prerna.junit;

import prerna.cluster.util.AZClient;
import prerna.ds.py.PyExecutorThread;
import prerna.om.Insight;

public class InsightHolder {
	
	private static InsightHolder insightHolder = null;
	private static Insight insight = null;
	private static PyExecutorThread jep = null;

	
	
	private InsightHolder() {
		
	}
	
	public static  InsightHolder getInstance()
	{
		if(insightHolder == null)
		{
			insightHolder = new InsightHolder();
		}
		return insightHolder;
	}
	
	
	public static void setInsight(Insight in) {
		insight = in;
	}
	
	public static void setPy(PyExecutorThread py) {
		jep=py;
	}
	
	public Insight getInsight() {
		return insight;
	}
	
	public PyExecutorThread getPy() {
		return jep;
	}
	
}
