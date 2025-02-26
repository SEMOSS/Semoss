package prerna.testing.query.querystruct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.query.querystruct.SetParamsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class SetParamsReactorApiTests extends AbstractBaseSemossApiTests {
	private List<String> metaValues;

	@Test
	void executeOnColumn() {
		metaValues = new ArrayList<>();
		metaValues.add("testPixelId");
		metaValues.add("testValue");
		metaValues.add("testCol");
		String pixel = ApiSemossTestUtils.buildPixelCall(SetParamsReactor.class, ReactorKeysEnum.PIXEL_ID, ReactorKeysEnum.VALUE,
			ReactorKeysEnum.COLUMN, metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Object reactorValue = nm.getValue();
		assertNotNull(reactorValue);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

	}

	@Test
	void executeOnAllColumnTable () {
		metaValues = new ArrayList<>();
		metaValues.add("testPixelId");
		metaValues.add("testValue");
		metaValues.add("testCol");
		metaValues.add("testTable");
		String pixel = ApiSemossTestUtils.buildPixelCall(SetParamsReactor.class, ReactorKeysEnum.PIXEL_ID, ReactorKeysEnum.VALUE,
			ReactorKeysEnum.COLUMN, ReactorKeysEnum.TABLE, metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Object reactorValue = nm.getValue();
		assertNotNull(reactorValue);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

	}

	@Test
	void executeOnAllColumnTableOperator () {
		metaValues = new ArrayList<>();
		metaValues.add("testPixelId");
		metaValues.add("testValue");
		metaValues.add("testCol");
		metaValues.add("testTable");
		metaValues.add("testOperator");
		String pixel = ApiSemossTestUtils.buildPixelCall(SetParamsReactor.class, ReactorKeysEnum.PIXEL_ID, ReactorKeysEnum.VALUE,
			ReactorKeysEnum.COLUMN, ReactorKeysEnum.TABLE, ReactorKeysEnum.OPERATOR, metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Object reactorValue = nm.getValue();
		assertNotNull(reactorValue);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

	}

	@Test
	void executeOnAllColumnTableOperatorU () {
		metaValues = new ArrayList<>();
		metaValues.add("testPixelId");
		metaValues.add("testValue");
		metaValues.add("testCol");
		metaValues.add("testTable");
		metaValues.add("testOperator");
		metaValues.add("testOperatorU");
		String pixel = ApiSemossTestUtils.buildPixelCall(SetParamsReactor.class, ReactorKeysEnum.PIXEL_ID, ReactorKeysEnum.VALUE,
			ReactorKeysEnum.COLUMN, ReactorKeysEnum.TABLE, ReactorKeysEnum.OPERATOR, ReactorKeysEnum.OPERATORU, metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Object reactorValue = nm.getValue();
		assertNotNull(reactorValue);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

	}
}
