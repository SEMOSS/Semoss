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

import java.util.Iterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TransposeMapHeaderDataRowIterator implements Iterator<IHeadersDataRow> {

  private boolean hasNext = true;
  private InMemStore<String, NounMetadata> store = null;
  private String[] headers;

  public TransposeMapHeaderDataRowIterator(InMemStore store) {
    this.store = store;
    headers = this.store.getKeys().toArray(new String[0]);
  }

  public TransposeMapHeaderDataRowIterator(InMemStore store, String[] headers) {
    this.store = store;
    this.headers = headers;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public IHeadersDataRow next() {
    Object[] data = new Object[headers.length];
    for (int i = 0; i < headers.length; i++) {
      data[i] = store.get(headers[i]).getValue();
    }

    hasNext = false;
    return new HeadersDataRow(headers, data);
  }
}
