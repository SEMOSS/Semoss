package prerna.reactor.task.lambda.map.function;

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.task.lambda.map.IMapLambda;
import prerna.reactor.task.lambda.map.function.math.PercentLambda;
import prerna.reactor.task.lambda.map.function.math.RoundLambda;
import prerna.reactor.task.lambda.map.function.string.ConcatLambda;
import prerna.reactor.task.lambda.map.function.string.LeftLambda;
import prerna.reactor.task.lambda.map.function.string.RightLambda;
import prerna.reactor.task.lambda.map.function.string.StrLengthLambda;
import prerna.reactor.task.lambda.map.function.string.SubstringLambda;
import prerna.reactor.task.lambda.map.function.string.TrimLambda;

public class MapLambdaFactory {

	public static Map<String, Class> mapLambdas = new HashMap<String, Class>();
	
	static {
		init();
	}
	
	private MapLambdaFactory() {
		
	}
	
	private static void init() {
		// really weird ones... 
		mapLambdas.put("TONUMERIC", ToNumericTypeLambda.class);
		mapLambdas.put("TOURL", ToUrlTypeLambda.class);
		mapLambdas.put("GOOGLELATLONG", GoogleLatLongLambda.class);
		
		// more normal ones
		
		// string manipulation
		mapLambdas.put("CONCAT", ConcatLambda.class);
		mapLambdas.put("LEN", StrLengthLambda.class);
		mapLambdas.put("TRIM", TrimLambda.class);
		mapLambdas.put("SUBSTR", SubstringLambda.class);
		mapLambdas.put("SUBSTRING", SubstringLambda.class);
		mapLambdas.put("MID", SubstringLambda.class);
		mapLambdas.put("LEFT", LeftLambda.class);
		mapLambdas.put("RIGHT", RightLambda.class);
//		mapLambdas.put("SPLIT", GoogleLatLongLambda.class);
//		mapLambdas.put("REGEX_REPLACE", GoogleLatLongLambda.class);
//
//		// math
//		mapLambdas.put("SUM", GoogleLatLongLambda.class);
//		mapLambdas.put("AVERAGE", GoogleLatLongLambda.class);
//		mapLambdas.put("MAX", GoogleLatLongLambda.class);
//		mapLambdas.put("MIN", GoogleLatLongLambda.class);
//		mapLambdas.put("MEDIAN", GoogleLatLongLambda.class);
		mapLambdas.put("PERCENT", PercentLambda.class);
		mapLambdas.put("ROUND", RoundLambda.class);
//		mapLambdas.put("POWER", PercentLambda.class);
	}

	public static IMapLambda getLambda(String transType) {
		IMapLambda newClass = null;
		
		transType = transType.toUpperCase();
		if(mapLambdas.containsKey(transType)) {
			try {
				newClass = (IMapLambda) mapLambdas.get(transType).newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		return newClass;
	}
	
}
