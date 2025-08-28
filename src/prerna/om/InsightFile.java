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
package prerna.om;

public class InsightFile {

  private String fileKey = null;
  private String filePath = null;
  private boolean deleteOnInsightClose = true;
  private boolean isFrameUpload = false;
  private boolean isExport = false;

  public InsightFile() {}

  public String getFileKey() {
    return fileKey;
  }

  public void setFileKey(String fileKey) {
    this.fileKey = fileKey;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public boolean isDeleteOnInsightClose() {
    return deleteOnInsightClose;
  }

  public void setDeleteOnInsightClose(boolean deleteOnInsightClose) {
    this.deleteOnInsightClose = deleteOnInsightClose;
  }

  public boolean isFrameUpload() {
    return isFrameUpload;
  }

  public void setFrameUpload(boolean isFrameUpload) {
    this.isFrameUpload = isFrameUpload;
  }

  public boolean isExport() {
    return isExport;
  }

  public void setExport(boolean isExport) {
    this.isExport = isExport;
  }
}
