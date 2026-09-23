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
package prerna.rdf.engine.wrappers;

import java.io.IOException;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.r.RNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;

public class RawRSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private RIterator output = null;

	@Override
	public void execute() throws Exception {
		this.output = (RIterator) this.engine.execQuery(this.query);
		setDefaults();
	}

	public void execute(SelectQueryStruct qs) {
		this.output = (RIterator) ((RNativeEngine) this.engine).execQuery(this.query, qs);
		setDefaults();
	}

	public void directExecution(RIterator output) {
		this.output = output;
		setDefaults();
	}

	@Override
	public IHeadersDataRow next() {
		return output.next();
	}

	@Override
	public boolean hasNext() {
		return output.hasNext();
	}

	private void setDefaults() {
		this.rawHeaders = output.getHeaders();
		this.headers = this.rawHeaders;

		String[] strTypes = output.getColTypes();
		this.types = new SemossDataType[this.rawHeaders.length];
		for (int i = 0; i < this.rawHeaders.length; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
		}
	}

	@Override
	public String[] getHeaders() {
		return headers;
	}

	@Override
	public SemossDataType[] getTypes() {
		return this.types;
	}

	@Override
	public void reset() throws Exception {
		this.output = (RIterator) this.engine.execQuery(this.query);
	}

	@Override
	public void close() throws IOException {
		this.output.cleanUp();
	}

	@Override
	public long getNumRows() {
		if (this.numRows == 0) {
			this.numRows = this.output.getTotalNumRows();
		}
		return this.numRows;
	}

	@Override
	public long getNumRecords() {
		return getNumRows() * this.headers.length;
	}

	@Override
	public boolean flushable() {
		// WE HAVE ISSUES WITH STRING NA BECOMING NULL
		// WHEN FLUSHING TO JSON
		// SO KEEP AS false
		return false;
	}

	@Override
	public String flush() {
		return this.output.getJsonOfResults();
	}

	@Override
	public String getQuery() {
		return output.getQuery();
	}

	public RIterator getOutput() {
		return this.output;
	}
}
