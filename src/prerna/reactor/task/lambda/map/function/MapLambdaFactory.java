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
package prerna.reactor.task.lambda.map.function;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.task.lambda.map.IMapLambda;
import prerna.reactor.task.lambda.map.function.math.PercentLambda;
import prerna.reactor.task.lambda.map.function.math.RoundLambda;
import prerna.reactor.task.lambda.map.function.string.ConcatLambda;
import prerna.reactor.task.lambda.map.function.string.LeftLambda;
import prerna.reactor.task.lambda.map.function.string.RightLambda;
import prerna.reactor.task.lambda.map.function.string.StrLengthLambda;
import prerna.reactor.task.lambda.map.function.string.SubstringLambda;
import prerna.reactor.task.lambda.map.function.string.TrimLambda;
import prerna.util.Constants;

public class MapLambdaFactory {

  private static final Logger classLogger = LogManager.getLogger(MapLambdaFactory.class);
  public static Map<String, Class> mapLambdas = new HashMap<String, Class>();

  static {
    init();
  }

  private MapLambdaFactory() {}

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
    if (mapLambdas.containsKey(transType)) {
      try {
        newClass = (IMapLambda) mapLambdas.get(transType).newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }

    return newClass;
  }
}
