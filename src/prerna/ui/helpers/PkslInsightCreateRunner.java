package prerna.ui.helpers;

import java.util.List;
import java.util.Map;

import prerna.om.Insight;

public class PkslInsightCreateRunner implements Runnable{

	private Insight in;
	private Map<String, Object> returnData;
	
	public PkslInsightCreateRunner(Insight in) {
		this.in = in;
	}
	
	@Override
	public void run() {
		List<String> pkslList = in.getPkslRecipe();
		returnData = in.runPkql(pkslList);
	}

	public Object getReturnData() {
		return this.returnData;
	}
}
