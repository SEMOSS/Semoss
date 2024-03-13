package prerna.junit.pixel;

import prerna.om.Insight;

public class InsightHolder {
	
	private static InsightHolder insightHolder = null;
	private static Insight insight = null;
	//private static PyExecutorThread jepThread = null;

	
	
	private InsightHolder() {
		
	}
	
	public synchronized static  InsightHolder getInstance()
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
	
//	public static void setPy(PyExecutorThread py) {
//		//jepThread=py;
//		insight.setPy(py);
//	}
	
	public Insight getInsight() {
		return insight;
	}
	
//	public PyExecutorThread getPy() {
//		return insight.getPy();
//	}
	
}
