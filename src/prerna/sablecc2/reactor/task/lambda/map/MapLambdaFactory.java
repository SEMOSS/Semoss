package prerna.sablecc2.reactor.task.lambda.map;

import java.util.HashMap;
import java.util.Map;

public class MapLambdaFactory {

	public static Map<String, Class> mapLambdas = new HashMap<String, Class>();
	
	static {
		init();
	}
	
	private MapLambdaFactory() {
		
	}
	
	private static void init() {
		mapLambdas.put("TONUMERIC", ToNumericTypeLambda.class);
		mapLambdas.put("TOURL", ToUrlTypeLambda.class);
		mapLambdas.put("GOOGLELATLONG", GoogleLatLongLambda.class);
	}

	public static IMapLambda getTransformation(String transType) {
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
