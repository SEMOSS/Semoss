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
package prerna.engine.impl.vector.metadata;

import prerna.date.SemossDate;

public class VectorDatabaseMetadataCSVRow {

  private String source;
  private String attribute;
  private String strValue;
  private Integer intValue;
  private Number numValue;
  private Boolean boolValue;
  private SemossDate dateValue;
  private SemossDate timestampValue;

  public VectorDatabaseMetadataCSVRow(
      String source,
      String attribute,
      String strValue,
      Number intValue,
      Number numValue,
      Boolean boolValue,
      SemossDate dateValue,
      SemossDate timestampValue) {
    this.source = source;
    this.attribute = attribute;
    this.strValue = strValue;
    if (intValue != null) {
      this.intValue = intValue.intValue();
    }
    this.numValue = numValue;
    this.boolValue = boolValue;
    this.dateValue = dateValue;
    this.timestampValue = timestampValue;
  }

  public String getSource() {
    return source;
  }

  public String getAttribute() {
    return attribute;
  }

  public String getStrValue() {
    return strValue;
  }

  public Integer getIntValue() {
    return intValue;
  }

  public Number getNumValue() {
    return numValue;
  }

  public Boolean getBoolValue() {
    return boolValue;
  }

  public SemossDate getDateValue() {
    return dateValue;
  }

  public SemossDate getTimestampValue() {
    return timestampValue;
  }
}
