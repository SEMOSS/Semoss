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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

/**
 * Iterates over a materialized Polars query result while retaining the original
 * result size and supporting reset. The iterator owns an independent copy of
 * the row list supplied by the Python bridge.
 */
public class PolarsIterator implements Iterator<IHeadersDataRow> {

	private final String[] headers;
	private final SemossDataType[] types;
	private final List<Object> data;
	private final int initialSize;
	private int index;
	private String query;

	/**
	 * Creates an iterator for one materialized result.
	 *
	 * @param headers ordered result headers
	 * @param data    rows returned by the Python bridge
	 * @param types   SEMOSS types aligned with the headers
	 */
	public PolarsIterator(String[] headers, List<Object> data, SemossDataType[] types) {
		this.headers = headers;
		this.types = types;
		this.data = data == null ? new ArrayList<>() : new ArrayList<>(data);
		this.initialSize = this.data.size();
		this.index = 0;
	}

	@Override
	public boolean hasNext() {
		return this.index < this.data.size();
	}

	@Override
	public IHeadersDataRow next() {
		Object row = this.data.get(this.index++);
		Object[] values = row instanceof List ? ((List<?>) row).toArray() : new Object[] { row };
		IHeadersDataRow dataRow = new HeadersDataRow(this.headers, values);
		dataRow.setQuery(this.query);
		return dataRow;
	}

	/**
	 * @return ordered result headers
	 */
	public String[] getHeaders() {
		return this.headers;
	}

	/**
	 * @return SEMOSS types aligned with the headers
	 */
	public SemossDataType[] getTypes() {
		return this.types;
	}

	/**
	 * @return row count before iteration began
	 */
	public int getInitialSize() {
		return this.initialSize;
	}

	/**
	 * Associates the structural query plan with rows produced by this iterator.
	 *
	 * @param query serialized query plan
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return serialized query plan associated with the result
	 */
	public String getQuery() {
		return this.query;
	}

	/**
	 * Rewinds iteration to the first row.
	 */
	public void reset() {
		this.index = 0;
	}
}
