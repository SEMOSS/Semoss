package prerna.sablecc2.reactor.expression;

import java.util.Hashtable;
import java.util.Map;

import prerna.sablecc2.om.GenRowStruct;

public class OpFactory {

	private static Map<String, Class> opMap = new Hashtable<String, Class>();
	
	static {
		fillOpMap();
	}
	
	private static void fillOpMap() {
		opMap.put("max", OpMax.class);
		opMap.put("min", OpMin.class);
		opMap.put("sum", OpSum.class);
		opMap.put("mean", OpMean.class);
		opMap.put("average", OpMean.class);
		opMap.put("median", OpMedian.class);
	}
	
	public static OpReactor getOp(String operationName, GenRowStruct curRow) {
		operationName = operationName.toLowerCase();
		
		OpReactor reactor = null;
		try {
			reactor = (OpReactor) opMap.get(operationName).newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		reactor.In();
		reactor.mergeCurRow(curRow);
		return reactor;
	}



}
