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
package prerna.util.gson;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static prerna.util.gson.IHeadersDataRowAdapter.deserializeValues;
import static prerna.util.gson.IHeadersDataRowAdapter.serializeValues;
import static prerna.util.gson.IHeadersDataRowAdapter.toPrettyFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import prerna.engine.api.IHeadersDataRow;
import prerna.junit.pixel.JUnit;
import prerna.om.HeadersDataRow;
import prerna.util.gson.IHeadersDataRowAdapter.SerializedValuesAndTypes;

public class IHeadersDataRowAdapterTest extends JUnit {
	
	private static final String[] HEADERS = new String[] {"STRING_H", "NULL_H", "INT_H", "DOUBLE_H", "LONG_H", "FLOAT_H", "BOOLEAN_H", "CHAR_H", "BYTE_H", "SHORT_H", "ENCODED_H"};
	private static final String[] RAW_HEADERS = new String[] {"R_STRING_H", "R_NULL_H", "R_INT_H", "R_DOUBLE_H", "R_LONG_H", "R_FLOAT_H", "R_BOOLEAN_H", "R_CHAR_H", "R_BYTE_H", "R_SHORT_H", "R_ENCODED_H"};
	private static final Object[] VALUES = new Object[] {"foo", null, 1, 2.1D, 3L, 4.1F, true, 'a', new Byte("0"), new Short("1"), new Date()};
	private static final Object[] RAW_VALUES = new Object[] {"R_foo", null, 2, 3000000000.0D, 4L, 5.1F, false, 'b', new Byte("1"), new Short("0"), new Date()};
	
	@Test
	public void testSerializeDeserialize() {
		SerializedValuesAndTypes result = serializeValues(VALUES);
		Object[] newValues = deserializeValues(result.getSerializedValues(), result.getSerializedValueTypes());
		assertTrue(Arrays.equals(newValues, VALUES));
	}
	
	@Test
	public void testIHeadersDataRowAdapter() {
		try {
			IHeadersDataRow dataRowBefore = new HeadersDataRow(HEADERS, RAW_HEADERS, VALUES, RAW_VALUES);
			IHeadersDataRowAdapter adapter = new IHeadersDataRowAdapter();
			LOGGER.info(toPrettyFormat(adapter.toJson(dataRowBefore)));
			IHeadersDataRow dataRowAfter = adapter.fromJson(adapter.toJson(dataRowBefore));
			assertTrue(dataRowBefore.toRawString().equals(dataRowAfter.toRawString()));
		} catch (IOException e) {
			LOGGER.error(e);
			fail();
		}	
	}

}
