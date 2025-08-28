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

public class InsightSheet {

  private String sheetId;
  private String sheetLabel;
  private String backgroundColor;
  private Boolean hideHeaders;
  private Boolean hideBorders;
  private int borderSize = 2;
  private String height;
  private String width;
  private int gutterSize = 2;

  /*
   * Basic setter/getters for the sheet state
   */

  public InsightSheet(String sheetId) {
    this.sheetId = sheetId;
  }

  public InsightSheet(String sheetId, String sheetLabel) {
    this.sheetId = sheetId;
    this.sheetLabel = sheetLabel;
  }

  public String getSheetId() {
    return this.sheetId;
  }

  public String getSheetLabel() {
    return this.sheetLabel;
  }

  public void setSheetLabel(String sheetLabel) {
    this.sheetLabel = sheetLabel;
  }

  public String getBackgroundColor() {
    return this.backgroundColor;
  }

  public void setBackgroundColor(String backgroundColor) {
    this.backgroundColor = backgroundColor;
  }

  public void setHideHeaders(Boolean hideHeaders) {
    this.hideHeaders = hideHeaders;
  }

  public Boolean getHideHeaders() {
    return this.hideHeaders;
  }

  public void setHideBorders(Boolean hideBorders) {
    this.hideBorders = hideBorders;
  }

  public Boolean getHideBorders() {
    return this.hideBorders;
  }

  public int getBorderSize() {
    return borderSize;
  }

  public void setBorderSize(int borderSize) {
    this.borderSize = borderSize;
  }

  public String getHeight() {
    return height;
  }

  public void setHeight(String height) {
    this.height = height;
  }

  public String getWidth() {
    return width;
  }

  public void setWidth(String width) {
    this.width = width;
  }

  public int getGutterSize() {
    return gutterSize;
  }

  public void setGutterSize(int gutterSize) {
    this.gutterSize = gutterSize;
  }
}
