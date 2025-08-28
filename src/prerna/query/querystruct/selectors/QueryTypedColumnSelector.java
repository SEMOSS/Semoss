/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.query.querystruct.selectors;

import prerna.algorithm.api.SemossDataType;

public class QueryTypedColumnSelector extends QueryColumnSelector {

  //	public enum DATA_TYPE { STRING, INT, DATE, TIMESTAMP, NUMBER };

  protected String dataType;

  public QueryTypedColumnSelector() {
    super();
  }

  public QueryTypedColumnSelector(SemossDataType dataType) {
    super();
    this.dataType = dataType == null ? null : dataType.toString();
  }

  public QueryTypedColumnSelector(String qsValue) {
    super(qsValue);
  }

  public QueryTypedColumnSelector(String qsValue, SemossDataType dataType) {
    super(qsValue);
    this.dataType = dataType == null ? null : dataType.toString();
  }

  public QueryTypedColumnSelector(String qsValue, String alias) {
    super(qsValue, alias);
  }

  public QueryTypedColumnSelector(String qsValue, String alias, SemossDataType dataType) {
    super(qsValue, alias);
    this.dataType = dataType == null ? null : dataType.toString();
  }

  @Override
  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public void setDataType(SemossDataType dataType) {
    this.dataType = dataType == null ? null : dataType.toString();
  }
}
