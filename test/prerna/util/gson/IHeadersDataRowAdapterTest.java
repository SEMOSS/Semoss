package prerna.util.gson;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import prerna.engine.api.IHeadersDataRow;
import prerna.junit.JUnit;
import prerna.om.HeadersDataRow;
import prerna.util.gson.IHeadersDataRowAdapter.SerializedValuesAndTypes;

import static prerna.util.gson.IHeadersDataRowAdapter.serializeValues;
import static prerna.util.gson.IHeadersDataRowAdapter.deserializeValues;
import static prerna.util.gson.IHeadersDataRowAdapter.toPrettyFormat;

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
