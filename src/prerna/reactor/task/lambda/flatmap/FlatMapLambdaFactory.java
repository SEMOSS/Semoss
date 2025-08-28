/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
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
		if (flatMapLambdas.containsKey(transType)) {
			try {
				newClass = (IFlatMapLambda) flatMapLambdas.get(transType).newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		return newClass;
	}
}
