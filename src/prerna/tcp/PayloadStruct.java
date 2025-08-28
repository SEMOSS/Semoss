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
package prerna.tcp;

import java.io.Serializable;

public class PayloadStruct implements Serializable {

  public String epoc = null;

  // other operations to introduce
  // ENGINE
  // SET

  public enum OPERATION {
    R,
    PYTHON,
    CHROME,
    ECHO,
    ENGINE,
    REACTOR,
    INSIGHT,
    PROJECT,
    CMD,
    STDOUT,
    STDERR
  };

  public OPERATION operation = OPERATION.R; // setting default to R
  public String methodName = "method";
  public Object[] payload = null;
  public Class[] payloadClasses = null;

  // this is because python cant marshal java classes
  public String[] payloadClassNames = null;
  public String engineType = null;

  public String ex = null;
  public boolean processed = false;
  public boolean longRunning = false;
  public String env = null;
  public boolean interim = false;

  // this is a little bit of a complex logic
  // the idea here is say you hve something that is not serializable
  // you can keep it on that end and then work it through that
  public String[] inputAlias = null; // this should be the same size as payload
  public String aliasReturn = null;

  public boolean hasReturn = true; // if it is a void set this to true

  // parent epoc
  // to make sure that this is something that is a follow on
  public String parentEpoc = null; // do we need this other than for trace ?
  // is this request or response
  public boolean response = false;

  // object identifier
  // this is specifically useful for things like engine etc.
  public String objId = null;

  // specify the project id for reactor loads
  public String projectId = null;

  // set the project name
  public String projectName = null;

  // specify the portal id for reactor loads
  public String portalId = null;

  // set the insight id
  public String insightId = null;

  // set the job id
  public String jobId = null;

  /*
   * This is really important
   * If we have a User invoking an engine python process
   * The engine python process has its own unique insight for variable encapsulation
   * However, we need to know from what insight is the user invoking this request
   * So that if the engine is making a call back/reactor request
   * It knows which User invoked for security permissions
   */
  public String executionInsightId = null;
}
