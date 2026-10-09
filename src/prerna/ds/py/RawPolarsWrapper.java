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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 or GPLv2, as applicable.
 *******************************************************************************/
package prerna.ds.py;

import java.io.IOException;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;

public class RawPolarsWrapper implements IRawSelectWrapper {

	private PolarsIterator iterator;

	public void setIterator(PolarsIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public void execute() {
		// Results are materialized by PolarsFrame before wrapper construction.
	}

	@Override
	public void setQuery(String query) {
		this.iterator.setQuery(query);
	}

	@Override
	public String getQuery() {
		return this.iterator.getQuery();
	}

	@Override
	public void close() throws IOException {
		// No external resource remains after result materialization.
	}

	@Override
	public void setEngine(IDatabaseEngine engine) {
		// Frame wrappers do not own a database engine.
	}

	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		return this.iterator.next();
	}

	@Override
	public String[] getHeaders() {
		return this.iterator.getHeaders();
	}

	@Override
	public SemossDataType[] getTypes() {
		return this.iterator.getTypes();
	}

	@Override
	public long getNumRows() {
		return this.iterator.getInitialSize();
	}

	@Override
	public long getNumRecords() {
		return getNumRows() * getHeaders().length;
	}

	@Override
	public void reset() {
		this.iterator.reset();
	}

	@Override
	public IDatabaseEngine getEngine() {
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
