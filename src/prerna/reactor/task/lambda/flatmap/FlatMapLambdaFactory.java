package prerna.reactor.task.lambda.flatmap;

import java.util.HashMap;
import java.util.Map;

public class FlatMapLambdaFactory {

	public static Map<String, Class> flatMapLambdas = new HashMap<String, Class>();
	
	static {
		init();
	}
	
	private FlatMapLambdaFactory() {
		
	}
	
	private static void init() {
		flatMapLambdas.put("TWITTERSEARCH", TwitterSearchLambda.class);
		flatMapLambdas.put("GOOGLESENTIMENT", GoogleSentimentAnalyzerLambda.class);
		flatMapLambdas.put("GOOGLEENTITY", GoogleEntityAnalyzerLambda.class);
	}

	public static IFlatMapLambda getLambda(String transType) {
		IFlatMapLambda newClass = null;
		
		transType = transType.toUpperCase();
		if(flatMapLambdas.containsKey(transType)) {
			try {
				newClass = (IFlatMapLambda) flatMapLambdas.get(transType).newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		return newClass;
	}
	
}
