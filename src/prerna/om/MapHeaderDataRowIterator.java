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
package prerna.om;

import java.io.IOException;
import java.util.Iterator;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MapHeaderDataRowIterator implements IRawSelectWrapper {

  private InMemStore<String, NounMetadata> store = null;
  private Iterator<String> keysIterator = null;

  public MapHeaderDataRowIterator(InMemStore store) {
    this.store = store;
    this.keysIterator = store.getKeys().iterator();
  }

  @Override
  public boolean hasNext() {
    return keysIterator.hasNext();
  }

  @Override
  public IHeadersDataRow next() {
    String key = keysIterator.next();
    NounMetadata value = store.get(key);

    String[] header = new String[] {key.toString()};
    Object[] data = new Object[] {value.getValue()};

    return new HeadersDataRow(header, data);
  }

  @Override
  public long getNumRows() {
    return store.getKeys().size();
  }

  @Override
  public long getNumRecords() {
    return store.getKeys().size();
  }

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
    return null;
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
  public IDatabaseEngine getEngine() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getHeaders() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SemossDataType[] getTypes() {
    // TODO Auto-generated method stub
    return null;
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
