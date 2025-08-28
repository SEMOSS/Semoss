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
package prerna.engine.impl.model;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.om.ThreadStore;

public class EmbeddedModelEngine extends AbstractPythonModelEngine {

  @Override
  public ModelTypeEnum getModelType() {
    return ModelTypeEnum.EMBEDDED;
  }

  protected List<String> keywordExtractionCall(
      Object input, Insight insight, Map<String, Object> parameters) {
    checkSocketStatus();

    StringBuilder callMaker = new StringBuilder(varName);
    String inputAsString = PyUtils.determineStringType(input);
    callMaker.append(".keyword_extraction(input = ").append(inputAsString);
    if (parameters != null && !parameters.isEmpty()) {
      callMaker.append(", **").append(PyUtils.determineStringType(parameters));
    }
    callMaker.append(")");

    List<String> output = (List<String>) pyTranslator.runDirectPy(callMaker.toString());
    return output;
  }

  public List<String> keywordExtraction(
      Object input, Insight insight, Map<String, Object> parameters) {
    ZonedDateTime inputTime = ZonedDateTime.now();
    List<String> keywordExtractionResponse = keywordExtractionCall(input, insight, parameters);
    ZonedDateTime outputTime = ZonedDateTime.now();

    if (inferenceLogsEnbaled) {
      String messageId = UUID.randomUUID().toString();
      Thread inferenceRecorder =
          new Thread(
              new ModelEngineInferenceLogsWorker(
                  /*messageId*/ messageId,
                  /*messageMethod*/ "textKeywords",
                  /*engine*/ this,
                  /*insightId*/ insight.getInsightId(),
                  /*projectContextId*/ insight.getContextProjectId(),
                  /*projectId*/ insight.getProjectId(),
                  /*user*/ insight.getUser(),
                  /*sessionId*/ ThreadStore.getSessionId(),
                  /*roomId*/ ThreadStore.getInsightId(),
                  /*context*/ null,
                  /*prompt*/ input + "",
                  /*fullPrompt*/ null,
                  /*promptTokens*/ null,
                  /*inputTime*/ inputTime,
                  /*response*/ PyUtils.determineStringType(keywordExtractionResponse),
                  /*responseTokens*/ null,
                  /*outputTime*/ outputTime));
      inferenceRecorder.start();
    }

    return keywordExtractionResponse;
  }
}
