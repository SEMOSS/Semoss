package prerna.sablecc2.reactor.task.transformation.map;

import java.util.HashMap;
import java.util.Map;

public class MapTransformations {

	public static Map<String, Class> transformations = new HashMap<String, Class>();
	
	static {
		init();
	}
	
	private MapTransformations() {
		
	}
	
	private static void init() {
		transformations.put("TONUMERIC", ToNumericTypeTransformation.class);
		transformations.put("TOURL", ToUrlTypeTransformation.class);
	}

	public static IMapTransformation getTransformation(String transType) {
		IMapTransformation newClass = null;
		
		transType = transType.toUpperCase();
		if(transformations.containsKey(transType)) {
			try {
				newClass = (IMapTransformation) transformations.get(transType).newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		return newClass;
	}
	
}
