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
package prerna.engine.impl.json;

import java.io.IOException;
import java.util.Hashtable;
import net.minidev.json.JSONArray;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;

public class JsonWrapper implements IRawSelectWrapper {

  protected IDatabaseEngine engine;
  protected String separator = "_";
  protected int numRows = -1;
  protected int curRow = 0;

  // values for querying
  protected String query;

  // number of return columns
  protected int numColumns = 0;

  // values for return
  protected String[] headers;
  protected SemossDataType[] types;

  // specific for this engine
  // json wrapper 2 deos not use this
  // but shares the above
  private JSONArray[] data = null;

  @Override
  public void execute() throws Exception {
    // sorry for the bad way to transport data
    Hashtable output = (Hashtable) engine.execQuery(query);
    this.data = (JSONArray[]) output.get("DATA");

    this.headers = (String[]) output.get("HEADERS");
    this.numColumns = this.headers.length;
    this.numRows = (Integer) output.get("COUNT");

    if (output.containsKey("SEPARATOR")) {
      separator = (String) output.get("SEPARATOR");
    }

    String[] strTypes = (String[]) output.get("TYPES");
    this.types = new SemossDataType[this.numColumns];
    for (int i = 0; i < this.numColumns; i++) {
      this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
    }
  }

  @Override
  public boolean hasNext() {
    return curRow < numRows;
  }

  @Override
  public void setQuery(String query) {
    this.query = query;
  }

  @Override
  public String getQuery() {
    return this.query;
  }

  @Override
  public void setEngine(IDatabaseEngine engine) {
    this.engine = engine;
  }

  @Override
  public IDatabaseEngine getEngine() {
    return this.engine;
  }

  @Override
  public IHeadersDataRow next() {
    Object[] values = new Object[headers.length];
    for (int colIndex = 0; colIndex < headers.length; colIndex++) {
      JSONArray thisArray = data[colIndex];

      Object thisValue = thisArray.get(curRow);
      if (thisValue instanceof JSONArray) {
        // need to do the magic of delimiters etc.
        JSONArray thisValueArray = (JSONArray) thisValue;
        StringBuffer output = new StringBuffer("");
        for (int valIndex = 0; valIndex < thisValueArray.size(); valIndex++) {
          if (valIndex != 0) {
            output.append(separator);
          }
          output.append(thisValueArray.get(valIndex));
        }
        thisValue = output.toString();
      }

      values[colIndex] = thisValue;
    }
    curRow++;

    IHeadersDataRow retRow = new HeadersDataRow(this.headers, values);
    return retRow;
  }

  @Override
  public String[] getHeaders() {
    return this.headers;
  }

  @Override
  public SemossDataType[] getTypes() {
    return types;
  }

  @Override
  public long getNumRows() {
    return this.numRows;
  }

  @Override
  public long getNumRecords() {
    return this.numRows * this.headers.length;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean flushable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String flush() {
    // TODO Auto-generated method stub
    return null;
  }
}
