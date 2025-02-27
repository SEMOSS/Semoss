package prerna.unit.query.querystruct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.query.querystruct.SetParamsReactor;
import prerna.reactor.insights.recipemanagement.ImportParamOptionsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetParamsReactorUnitTests {
	private SetParamsReactor reactor;
	private Insight insight;
	private Map<String, String> keyValues;

    @BeforeEach
	void setup() {
		reactor = new SetParamsReactor();
		keyValues = reactor.keyValue;
		insight = mock(Insight.class);
		reactor.setInsight(insight);
	}

	@Test
	void executeNoParams() {
		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn("");

		keyValues.put("PIXEL_ID", "");
		keyValues.put("VALUE", "testVal");
		keyValues.put("COLUMN", "testCol");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		// assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No such pixel ", nm.getValue().toString());
	}

	@Test
	void executeNoPixekId() {
		keyValues.put("PIXEL_ID", "");
		keyValues.put("VALUE", "testVal");
		keyValues.put("COLUMN", "testCol");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		// assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No such pixel ", nm.getValue().toString());
	}

	@Test
	void executeNoColumn () {
		keyValues.put("PIXEL_ID", "testId");
		keyValues.put("VALUE", "testVal");
		keyValues.put("COLUMN", "testCol");
		keyValues.put("TABLE", "testTable");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No Column key specified ", nm.getValue().toString());
	}

	@Test
	void executeNoColumnTable () {
		keyValues.put("PIXEL_ID", "testId");
		keyValues.put("VALUE", "testVal");
		keyValues.put("COLUMN", "testCol");
		keyValues.put("TABLE", "testTable");
		keyValues.put("OPERATOR", "testOperator");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No column or table key specified ", nm.getValue().toString());
	}

	@Test
	void executeNoColumnTableOperator () {
		keyValues.put("PIXEL_ID", "testId");
		keyValues.put("VALUE", "testVal");
		keyValues.put("COLUMN", "testCol");
		keyValues.put("TABLE", "testTable");
		keyValues.put("OPERATOR", "testOperator");
		keyValues.put("OPERATORU", "testOperatorU");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No column, table or operator key specified ", nm.getValue().toString());
	}
	
	@Test
	void executeNoColumnTableOperatorOperatorU () {
		keyValues.put("PIXEL_ID", "testId");
		keyValues.put("VALUE", "testVal");
		keyValues.put("COLUMN", "testCol");
		keyValues.put("TABLE", "testTable");
		keyValues.put("OPERATOR", "testOperator");
		keyValues.put("OPERATORU", "testOperatorU");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No column, table, operator or unique operator key specified ", nm.getValue().toString());
	}
}
