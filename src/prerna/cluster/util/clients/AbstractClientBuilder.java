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
package prerna.cluster.util.clients;

public abstract class AbstractClientBuilder implements ICloudClientBuilder {

  public static String RCLONE_PATH = "RCLONE_PATH";

  // s3 prefix is used for minio as well
  public static final String S3_REGION_KEY = "S3_REGION";
  public static final String S3_BUCKET_KEY = "S3_BUCKET";
  public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
  public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
  public static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";

  // gcp keys
  public static final String GCP_SERVICE_ACCOUNT_FILE_KEY = "GCP_SERVICE_ACCOUNT_FILE";
  public static final String GCP_REGION_KEY = "GCP_REGION";
  public static final String GCP_BUCKET_KEY = "GCP_BUCKET";

  // az keys
  public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
  public static final String AZ_NAME = "AZ_NAME";
  public static final String AZ_KEY = "AZ_KEY";
  public static final String AZ_GENERATE_DYNAMIC_SAS = "AZ_GENERATE_DYNAMIC_SAS";
  public static final String SAS_URL = "SAS_URL";
  public static final String AZ_URI = "AZ_URI";
  public static final String STORAGE = "STORAGE"; // says if this is local / cluster
  public static final String KEY_HOME = "KEY_HOME"; // this is where the various keys are cycled

  protected String rclonePath = "rclone";
  protected String rcloneConfigF = null;

  @Override
  public ICloudClientBuilder setRClonePath(String rclonePath) {
    if (rclonePath != null && !rclonePath.isEmpty()) {
      if (rclonePath.contains(" ") && !rclonePath.startsWith("\"")) {
        // need start quote
        rclonePath = "\"" + rclonePath;
      }
      if (rclonePath.contains(" ") && !rclonePath.endsWith("\"")) {
        // need start quote
        rclonePath = rclonePath + "\"";
      }
      this.rclonePath = rclonePath;
    }
    return this;
  }

  @Override
  public ICloudClientBuilder setRCloneConfigFolder(String rcloneConfigF) {
    this.rcloneConfigF = rcloneConfigF;
    return this;
  }

  @Override
  public String getRClonePath() {
    return rclonePath;
  }

  @Override
  public String getRCloneConfigFolder() {
    return rcloneConfigF;
  }
}
