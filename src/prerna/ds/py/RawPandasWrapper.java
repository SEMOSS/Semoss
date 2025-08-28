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
package prerna.ds.py;

import java.io.IOException;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;

public class RawPandasWrapper implements IRawSelectWrapper {

  PandasIterator iterator = null;

  @Override
  public void execute() {
    // TODO Auto-generated method stub

  }

  @Override
  public void setQuery(String query) {
    // TODO Auto-generated method stub

  }

  @Override
  public String getQuery() {
    // TODO Auto-generated method stub
    return iterator.getQuery();
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setEngine(IDatabaseEngine engine) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return iterator.hasNext();
  }

  @Override
  public IHeadersDataRow next() {
    // TODO Auto-generated method stub
    return iterator.next();
  }

  @Override
  public String[] getHeaders() {
    // TODO Auto-generated method stub
    return iterator.getHeaders();
  }

  @Override
  public SemossDataType[] getTypes() {
    // TODO Auto-generated method stub
    return iterator.colTypes;
  }

  @Override
  public long getNumRows() {
    return iterator.getInitSize();
  }

  @Override
  public long getNumRecords() {
    return iterator.getInitSize() * getHeaders().length;
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub

  }

  public void setPandasIterator(PandasIterator pi) {
    // TODO Auto-generated method stub
    this.iterator = pi;
  }

  @Override
  public IDatabaseEngine getEngine() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean flushable() {
    return false;
  }

  @Override
  public String flush() {
    return null;
  }
}
