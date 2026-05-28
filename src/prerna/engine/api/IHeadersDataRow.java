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
package prerna.engine.api;

import java.util.Map;

import com.google.gson.TypeAdapter;

import prerna.util.gson.HeadersDataRowAdapter;

public interface IHeadersDataRow {

	// Right now there is only one implementation of IHeadersDataRow
	enum HEADERS_DATA_ROW_TYPE {
		HEADERS_DATA_ROW
	};

	/**
	 * Get the type of the header
	 * 
	 * @return
	 */
	HEADERS_DATA_ROW_TYPE getHeaderType();

	/**
	 * Get the headers corresponding to the values by index
	 * 
	 * @return
	 */
	String[] getHeaders();

	/**
	 * Get the raw headers This is useful when we alias headers to be unique during
	 * loops
	 * 
	 * @return
	 */
	String[] getRawHeaders();

	/**
	 * Get the values of the row
	 * 
	 * @return
	 */
	Object[] getValues();

	/**
	 * Get the raw values This is useful if you want to see full URIs from a RDF
	 * engine
	 * 
	 * @return
	 */
	Object[] getRawValues();

	/**
	 * Set whether getValues() should return raw (full URI / typed literal) or clean
	 * (processed instance name) values.
	 * 
	 * @param raw
	 */
	void setRaw(boolean raw);

	/**
	 * Get the number of records in the row
	 * 
	 * @return
	 */
	int getRecordLength();

	/**
	 * This is really only for testing purposes
	 * 
	 * @return
	 */
	String toRawString();

	/**
	 * Add new values into an existing headers data row
	 * 
	 * @param addHeaders
	 * @param addValues
	 */
	void addFields(String[] addHeaders, Object[] addValues);

	/**
	 * Add a single new column and value
	 * 
	 * @param addHeader
	 * @param addValues
	 */
	void addFields(String addHeader, Object addValues);

	/**
	 * Copy the headers row
	 * 
	 * @return
	 */
	IHeadersDataRow copy();

	// <<<<<<< Methods to be used for other purposes

	String toJson();

	// gets a particular value
	void open();

	// add a tuple
	void addField(String fieldName, Object value);

	// gets a particular field
	Object getField(String fieldName);

	String getQuery();

	void setQuery(String query);

	Map<String, Object> flushRowToMap();

	/*
	 * 
	 * Methods around serialization
	 * 
	 */

	// Right now only one ;)
	static TypeAdapter getAdapterForHeader(HEADERS_DATA_ROW_TYPE type) {
		if (type == HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW) {
			return new HeadersDataRowAdapter();
		}
		return null;
	}

	/**
	 * Convert string to SELECTOR_TYPE
	 * 
	 * @param s
	 * @return
	 */
	static HEADERS_DATA_ROW_TYPE convertStringToHeaderType(String s) {
		if (s.equals(HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW.toString())) {
			return HEADERS_DATA_ROW_TYPE.HEADERS_DATA_ROW;
		}
		return null;
	}
}
