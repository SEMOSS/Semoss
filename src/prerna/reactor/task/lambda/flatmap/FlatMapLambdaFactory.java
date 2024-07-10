package prerna.reactor.task.lambda.flatmap;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

public class FlatMapLambdaFactory {
	
	private static final Logger classLogger = LogManager.getLogger(FlatMapLambdaFactory.class);

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
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		return newClass;
	}
	
}
