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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.engine.api.IHeadersDataRow;
import prerna.util.gson.GsonUtility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class HeadersDataRow implements IHeadersDataRow {

	/**
	 * Base components corresponding to a header row
	 */
	private String[] headers;
	private String[] rawHeaders;
	private Object[] values;
	private Object[] rawValues;
	private int recordLength;

	private boolean raw = false;

	private List<Object> vecValues = null;
	private List<String> vecHeaders = null;
	private String query;

	public HeadersDataRow(String[] headers, Object[] values) {
		this(headers, headers, values, values);
	}

	public HeadersDataRow(String[] headers, String[] rawHeaders, Object[] values) {
		this(headers, rawHeaders, values, values);
	}

	public HeadersDataRow(String[] headers, Object[] values, Object[] rawValues) {
		this(headers, headers, values, rawValues);
	}

	public HeadersDataRow(String[] headers, String[] rawHeaders, Object[] values, Object[] rawValues) {
		if (headers.length != values.length && values.length != rawValues.length) {
			throw new IllegalArgumentException("Length of parameters not equal");
		}

		this.headers = headers;
		this.rawHeaders = rawHeaders;
		if (this.rawHeaders == null) {
			this.rawHeaders = this.headers;
		}
		this.values = values;
		this.rawValues = rawValues;

		this.recordLength = headers.length;
	}

	@Override
	public int getRecordLength() {
		return this.recordLength;
	}

	@Override
	public String[] getHeaders() {
		return headers;
	}

	@Override
	public void setRaw(boolean raw) {
		this.raw = raw;
	}

	@Override
	public Object[] getValues() {
		if (this.raw) {
			Object[] stringified = new Object[this.rawValues.length];
			for (int i = 0; i < this.rawValues.length; i++) {
				stringified[i] = this.rawValues[i] != null ? this.rawValues[i].toString() : null;
			}
			return stringified;
		}
		return this.values;
	}

	@Override
	public Object[] getRawValues() {
		return rawValues;
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		int size = headers.length;
		int index = 0;
		ret.append("START ROW\n");
		for (; index < size; index++) {
			ret.append("\tHEADER=").append(headers[index]).append("\tVALUE=").append(values[index]).append("\n");
		}
		ret.append("END ROW\n");

		return ret.toString();
	}

	@Override
	public String toRawString() {
		StringBuilder ret = new StringBuilder();
		int size = headers.length;
		int index = 0;
		ret.append("START ROW\n");
		for (; index < size; index++) {
			ret.append("\tHEADER=").append(headers[index]).append("\tVALUE=").append(rawValues[index]).append("\n");
		}
		ret.append("END ROW\n");

		return ret.toString();
	}

	@Override
	public void addFields(String[] addHeaders, Object[] addValues) {
		int newValuesLength = addHeaders.length;
		if (newValuesLength != addValues.length) {
			throw new IllegalArgumentException("Length of parameters not equal");
		}

		// we will make new arrays and copy over the values
		int totalLength = this.recordLength + newValuesLength;
		String[] newHeaders = new String[totalLength];
		Object[] newValues = new Object[totalLength];

		System.arraycopy(this.headers, 0, newHeaders, 0, this.recordLength);
		System.arraycopy(this.values, 0, newValues, 0, this.recordLength);

		// add the new values into the new headers / values
		int counter = 0;
		for (int i = 0; i < newValuesLength; i++) {
			newHeaders[this.recordLength + i] = addHeaders[counter];
			newValues[this.recordLength + i] = addValues[counter];
			counter++;
		}

		// adjust references
		this.headers = newHeaders;
		this.values = newValues;
		// TODO: expose raw headers and raw values as well
		this.rawHeaders = this.headers;
		this.rawValues = this.values;
	}

	@Override
	public void addFields(String addHeader, Object addValues) {
		// we will make new arrays and copy over the values
		int totalLength = this.recordLength + 1;
		String[] newHeaders = new String[totalLength];
		Object[] newValues = new Object[totalLength];

		System.arraycopy(this.headers, 0, newHeaders, 0, this.recordLength);
		System.arraycopy(this.values, 0, newValues, 0, this.recordLength);

		// add the new values into the new headers / values
		newHeaders[this.recordLength] = addHeader;
		newValues[this.recordLength] = addValues;

		// adjust references
		this.headers = newHeaders;
		this.values = newValues;
		// TODO: expose raw headers and raw values as well
		this.rawHeaders = this.headers;
		this.rawValues = this.values;
	}

	@Override
	public IHeadersDataRow copy() {
		// convert the main portions and return new
		Gson gson = GsonUtility.getDefaultGson();
		String[] cHeaders = gson.fromJson(gson.toJson(this.headers), String[].class);
		String[] cRawHeaders = gson.fromJson(gson.toJson(this.rawHeaders), String[].class);
		Object[] cValues = gson.fromJson(gson.toJson(this.values), Object[].class);
		Object[] cRawValues = gson.fromJson(gson.toJson(this.rawValues), Object[].class);

		return new HeadersDataRow(cHeaders, cRawHeaders, cValues, cRawValues);
	}

	@Override
	public Map<String, Object> flushRowToMap() {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < headers.length; i++) {
			if (values[i] instanceof java.sql.Clob) {
				String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
				map.put(headers[i], value);
			} else {
				map.put(headers[i], values[i]);
			}
		}
		return map;
	}

	/////////////////////////////////////////////

	@Override
	public String toJson() {
		return null;
	}

	@Override
	public void open() {
		vecHeaders = new ArrayList<String>();
		vecValues = new ArrayList<Object>();
		vecHeaders.addAll(Arrays.asList(headers));
		vecValues.addAll(Arrays.asList(values));
	}

	@Override
	public void addField(String fieldName, Object value) {
		vecHeaders.add(fieldName);
		vecValues.add(value);
	}

	public boolean isValue(String fieldName) {
		return vecHeaders.indexOf(fieldName) >= 0;
	}

	@Override
	public Object getField(String fieldName) {
		int fieldIndex = vecHeaders.indexOf(fieldName);
		if (fieldIndex >= 0) {
			return vecValues.get(fieldIndex);
		}
		return null;
	}

	@Override
	public String[] getRawHeaders() {
		return rawHeaders;
	}

	@Override
	public HEADERS_DATA_ROW_TYPE getHeaderType() {
		return IHeadersDataRow.HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW;
	}

	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return query;
	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		this.query = query;
	}
}
