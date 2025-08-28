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
package prerna.engine.impl.model.responses;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class AskTTSModelEngineResponse extends AskModelEngineResponse<Map<String, Object>> {

  private static final Logger classLogger = LogManager.getLogger(AskTTSModelEngineResponse.class);
  private static final long serialVersionUID = 1L;

  public static final String SOURCE_KSERVE = "kserve";

  /**
   * Constructor for TTS model response
   *
   * @param response The response map containing TTS data
   * @param numberOfTokensInPrompt Number of tokens in the input prompt
   * @param numberOfTokensInResponse Number of tokens in the response
   */
  public AskTTSModelEngineResponse(
      Map<String, Object> response,
      Integer numberOfTokensInPrompt,
      Integer numberOfTokensInResponse) {
    super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    this.messageType = TTS;
  }

  /**
   * Factory method for KServe TTS responses
   *
   * @param response JSONObject response from KServe TTS model
   * @return AskTTSModelEngineResponse instance
   */
  public static AskTTSModelEngineResponse getKServeTTSResponse(JSONObject response) {
    if (response != null) {
      Map<String, Object> responseMap = new HashMap<>();
      responseMap.put("source", SOURCE_KSERVE);

      if (response.has("output")) {
        Object outputObj = response.get("output");

        if (outputObj instanceof String) {
          responseMap.put("audio_data", outputObj);
          responseMap.put("format", "base64");
        } else if (outputObj instanceof JSONObject) {
          JSONObject outputJson = (JSONObject) outputObj;

          if (outputJson.has("audio")) {
            responseMap.put("audio_data", outputJson.get("audio"));
            responseMap.put("format", "base64");
          }

          if (outputJson.has("duration")) {
            responseMap.put("duration", outputJson.get("duration"));
          }
          if (outputJson.has("voice")) {
            responseMap.put("voice", outputJson.get("voice"));
          }
          if (outputJson.has("sample_rate")) {
            responseMap.put("sample_rate", outputJson.get("sample_rate"));
          }
          if (outputJson.has("speed")) {
            responseMap.put("speed", outputJson.get("speed"));
          }
        } else {
          responseMap.put("audio_data", outputObj);
          responseMap.put("format", "unknown");
        }
      }

      if (response.has("audio_format")) {
        responseMap.put("audio_format", response.getString("audio_format"));
      } else {
        responseMap.put("audio_format", "wav");
      }

      if (response.has("sample_rate")) {
        responseMap.put("sample_rate", response.getInt("sample_rate"));
      } else {
        responseMap.put("sample_rate", 22050);
      }

      if (response.has("duration")) {
        responseMap.put("duration", response.getDouble("duration"));
      }

      if (response.has("voice")) {
        responseMap.put("voice", response.getString("voice"));
      }

      if (response.has("speed")) {
        responseMap.put("speed", response.getString("speed"));
      }

      if (response.has("text")) {
        responseMap.put("text", response.getString("text"));
      }

      if (response.has("error")) {
        responseMap.put("error", response.getString("error"));
      }

      return new AskTTSModelEngineResponse(responseMap, 0, 0);
    } else {
      classLogger.error("Null response from TTS model request");
      Map<String, Object> errorMap = new HashMap<>();
      errorMap.put("status", "error");
      errorMap.put("message", "Null response from TTS model request");
      errorMap.put("source", SOURCE_KSERVE);

      return new AskTTSModelEngineResponse(errorMap, 0, 0);
    }
  }

  /**
   * Get the audio data (typically base64 encoded)
   *
   * @return Audio data as string, or null if not available
   */
  public String getAudioData() {
    Map<String, Object> response = this.getResponse();
    Object audioObj = response.get("audio_data");
    return (audioObj instanceof String) ? (String) audioObj : null;
  }

  /**
   * Get the audio format (wav, mp3, etc.)
   *
   * @return Audio format string
   */
  public String getAudioFormat() {
    Map<String, Object> response = this.getResponse();
    Object formatObj = response.get("audio_format");
    return (formatObj instanceof String) ? (String) formatObj : "wav";
  }

  /**
   * Get the sample rate of the audio
   *
   * @return Sample rate as integer
   */
  public int getSampleRate() {
    Map<String, Object> response = this.getResponse();
    Object sampleRateObj = response.get("sample_rate");
    if (sampleRateObj instanceof Integer) {
      return (Integer) sampleRateObj;
    }
    return 22050;
  }

  /**
   * Get the duration of the audio in seconds
   *
   * @return Duration as double, or -1 if not available
   */
  public double getDuration() {
    Map<String, Object> response = this.getResponse();
    Object durationObj = response.get("duration");
    if (durationObj instanceof Double) {
      return (Double) durationObj;
    } else if (durationObj instanceof Integer) {
      return ((Integer) durationObj).doubleValue();
    }
    return -1.0;
  }

  /**
   * Get the voice used for synthesis
   *
   * @return Voice name as string, or null if not specified
   */
  public String getVoice() {
    Map<String, Object> response = this.getResponse();
    Object voiceObj = response.get("voice");
    return (voiceObj instanceof String) ? (String) voiceObj : null;
  }

  /**
   * Get the speed used for synthesis
   *
   * @return Speed as string, or null if not specified
   */
  public String getSpeed() {
    Map<String, Object> response = this.getResponse();
    Object speedObj = response.get("speed");
    return (speedObj instanceof String) ? (String) speedObj : null;
  }

  /**
   * Get the original text that was synthesized
   *
   * @return Original text as string, or null if not available
   */
  public String getOriginalText() {
    Map<String, Object> response = this.getResponse();
    Object textObj = response.get("text");
    return (textObj instanceof String) ? (String) textObj : null;
  }

  /**
   * Check if the audio data is base64 encoded
   *
   * @return true if base64 format, false otherwise
   */
  public boolean isBase64Format() {
    Map<String, Object> response = this.getResponse();
    Object formatObj = response.get("format");
    return "base64".equals(formatObj);
  }

  /**
   * Get the source of the TTS response (kserve, etc.)
   *
   * @return Source string
   */
  public String getSource() {
    Map<String, Object> response = this.getResponse();
    Object sourceObj = response.get("source");
    return (sourceObj instanceof String) ? (String) sourceObj : "unknown";
  }

  /**
   * Check if there was an error in the TTS generation
   *
   * @return true if there was an error, false otherwise
   */
  public boolean hasError() {
    Map<String, Object> response = this.getResponse();
    return response.containsKey("error") || "error".equals(response.get("status"));
  }

  /**
   * Get the error message if any
   *
   * @return Error message as string, or null if no error
   */
  public String getErrorMessage() {
    Map<String, Object> response = this.getResponse();
    Object errorObj = response.get("error");
    if (errorObj instanceof String) {
      return (String) errorObj;
    }
    Object messageObj = response.get("message");
    return (messageObj instanceof String) ? (String) messageObj : null;
  }

  @Override
  public String getStringResponse() {
    Map<String, Object> response = this.getResponse();
    JSONObject jsonObject = new JSONObject();

    for (Map.Entry<String, Object> entry : response.entrySet()) {
      try {
        jsonObject.put(entry.getKey(), entry.getValue());
      } catch (JSONException e) {
        classLogger.warn(
            "Failed to add key '{}' to JSON response: {}", entry.getKey(), e.getMessage());
      }
    }

    return jsonObject.toString();
  }
}
