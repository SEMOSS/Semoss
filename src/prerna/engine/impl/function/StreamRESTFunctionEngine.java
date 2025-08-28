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
package prerna.engine.impl.function;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hc.client5.http.ClientProtocolException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobThread;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class StreamRESTFunctionEngine extends AbstractFunctionEngine {

  private static final Logger classLogger = LogManager.getLogger(StreamRESTFunctionEngine.class);

  private String httpMethod;
  private String url;
  private Map<String, String> headers;

  private String contentType = "JSON";

  @Override
  public void open(Properties smssProp) throws Exception {
    super.open(smssProp);

    this.httpMethod = smssProp.getProperty("HTTP_METHOD");
    if (this.httpMethod == null
        || (this.httpMethod = this.httpMethod.trim().toUpperCase()).isEmpty()
        || (!this.httpMethod.equals("GET")
            && !this.httpMethod.equals("POST")
            && !this.httpMethod.equals("PUT")
            && !this.httpMethod.equals("HEAD"))) {
      throw new IllegalArgumentException(
          "RESTFunctionEngine only supports GET, HEAD, POST, or PUT requests");
    }

    this.url = smssProp.getProperty("URL");
    if (this.url == null || (this.url = this.url.trim()).isEmpty()) {
      throw new IllegalArgumentException("Must provide a URL");
    }
    Utility.checkIfValidDomain(url);

    String headersStr = smssProp.getProperty("HEADERS");
    if (headersStr != null && !(headersStr = headersStr.trim()).isEmpty()) {
      this.headers =
          new Gson().fromJson(headersStr, new TypeToken<Map<String, String>>() {}.getType());
    }

    if (smssProp.containsKey("CONTENT_TYPE")) {
      this.contentType = smssProp.getProperty("CONTENT_TYPE");
    }
  }

  @Override
  public void close() throws IOException {
    // i dont have anything to do here...

  }

  @Override
  public Object execute(Map<String, Object> parameterValues) {
    String jobId = (String) parameterValues.remove(PixelJobThread.JOB_KEY);
    if (jobId == null) {
      throw new IllegalArgumentException("Must provide the job id for streaming output");
    }

    // validate all the required keys are set
    if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
      Set<String> missingPs = new HashSet<>();
      for (String requiredP : this.requiredParameters) {
        if (!parameterValues.containsKey(requiredP)) {
          missingPs.add(requiredP);
        }
      }
      if (!missingPs.isEmpty()) {
        throw new IllegalArgumentException("Must define required keys = " + missingPs);
      }
    }

    // store the responses combined
    StringBuilder responseAssimilator = new StringBuilder();
    String responseData = null;

    CloseableHttpClient httpClient = null;
    CloseableHttpResponse response = null;
    HttpEntity entity = null;
    try {
      httpClient = HttpHelperUtility.getCustomClient(null, null, null, null);
      response = getResponse(httpClient, parameterValues);
      int statusCode = response.getCode();
      entity = response.getEntity();
      if (statusCode >= 200 && statusCode < 300) {
        // Handle streaming response
        if (entity != null) {
          try (BufferedReader reader =
              new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"))) {
            String line;

            while ((line = reader.readLine()) != null) {
              //                        	System.out.println("My line is = " + line);
              responseAssimilator.append(line);
              PixelJobManager.getManager().addPartialOut(jobId, line);
            }

            // return the combined outputs
            responseData = responseAssimilator.toString();
          } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new IllegalArgumentException(
                "There was an error processing the response from " + url);
          }
        }
      } else {
        responseData = entity != null ? EntityUtils.toString(entity, "UTF-8") : "";
        throw new IllegalArgumentException(
            "Connected to " + url + " but received error = " + responseData);
      }

      return responseData;
    } catch (IOException | ParseException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException("Could not connect to URL at " + url);
    } finally {
      if (entity != null) {
        try {
          EntityUtils.consume(entity);
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
      if (response != null) {
        try {
          response.close();
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
      if (httpClient != null) {
        try {
          httpClient.close();
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
    }
  }

  /**
   * @param httpClient
   * @param parameterValues
   * @return
   * @throws ClientProtocolException
   * @throws IOException
   */
  private CloseableHttpResponse getResponse(
      CloseableHttpClient httpClient, Map<String, Object> parameterValues)
      throws ClientProtocolException, IOException {
    CloseableHttpResponse response = null;
    if (httpMethod.equalsIgnoreCase("GET")) {
      StringBuffer queryString = new StringBuffer();
      boolean first = true;
      for (String k : parameterValues.keySet()) {
        if (!first) {
          queryString.append("&");
        }
        queryString.append(k).append("=").append(parameterValues.get(k));
        first = false;
      }
      String runTimeUrl = url + "?" + queryString;

      HttpGet httpGet = new HttpGet(runTimeUrl);
      addHeaders(httpGet);
      response = httpClient.execute(httpGet);
    } else if (httpMethod.equalsIgnoreCase("HEAD")) {
      StringBuffer queryString = new StringBuffer();
      boolean first = true;
      for (String k : parameterValues.keySet()) {
        if (!first) {
          queryString.append("&");
        }
        queryString.append(k).append("=").append(parameterValues.get(k));
        first = false;
      }
      String runTimeUrl = url + "?" + queryString;

      HttpHead httpHead = new HttpHead(runTimeUrl);
      addHeaders(httpHead);
      response = httpClient.execute(httpHead);
    } else if (httpMethod.equalsIgnoreCase("PUT")) {
      HttpPut httpPut = new HttpPut(url);
      addHeaders(httpPut);

      if (parameterValues != null && !parameterValues.isEmpty()) {
        if (this.contentType.equalsIgnoreCase("JSON")) {
          httpPut.setEntity(
              new StringEntity(new Gson().toJson(parameterValues), ContentType.APPLICATION_JSON));
        } else {
          List<NameValuePair> params = new ArrayList<NameValuePair>();
          for (String key : parameterValues.keySet()) {
            params.add(new BasicNameValuePair(key, parameterValues.get(key) + ""));
          }
          httpPut.setEntity(new UrlEncodedFormEntity(params, Charset.forName("UTF-8")));
        }
      }
      response = httpClient.execute(httpPut);
    } else {
      HttpPost httpPost = new HttpPost(url);
      addHeaders(httpPost);

      if (parameterValues != null && !parameterValues.isEmpty()) {
        if (this.contentType.equalsIgnoreCase("JSON")) {
          httpPost.setEntity(
              new StringEntity(new Gson().toJson(parameterValues), ContentType.APPLICATION_JSON));
        } else {
          List<NameValuePair> params = new ArrayList<NameValuePair>();
          for (String key : parameterValues.keySet()) {
            params.add(new BasicNameValuePair(key, parameterValues.get(key) + ""));
          }
          httpPost.setEntity(new UrlEncodedFormEntity(params, Charset.forName("UTF-8")));
        }
      }
      response = httpClient.execute(httpPost);
    }

    return response;
  }

  /**
   * Add headers to a request
   *
   * @param requestMethod
   */
  private void addHeaders(HttpUriRequestBase requestMethod) {
    if (this.headers != null && !this.headers.isEmpty()) {
      for (String key : this.headers.keySet()) {
        requestMethod.addHeader(key, this.headers.get(key));
      }
    }
  }

  @Override
  public String getCatalogSubType(Properties smssProp) {
    return "REST";
  }
}
