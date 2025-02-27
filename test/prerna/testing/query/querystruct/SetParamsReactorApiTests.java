package prerna.testing.query.querystruct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.om.PixelList;
import prerna.query.querystruct.SetParamsReactor;
import prerna.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestInsightUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.PixelChain;

public class SetParamsReactorApiTests extends AbstractBaseSemossApiTests {
	
	/**
	 * 
	 * NOTE: unable to test this class. Could not find a way to set the Insight var store to contain
	 * a var map with the ImportParamOptionsReactor.PARAM_OPTIONS key anywhere in the code base. I looked 
	 * for a little but determined it wasn't worth the time. Unit tests will suffice for now but will comment
	 * out the work in case we come back to this in the future and need to explain what I was trying to do.
	 * 
	 */

	@Test
	void executeOnColumn() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();

		PixelRunner pr = new PixelRunner();
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
		PixelChain distinct = new PixelChain("Distinct(false)");
		PixelChain importChain = new PixelChain(
				"Import(frame = [ CreateFrame(frameType=[GRID], override = [true]).as([\"test_FRAME000001\"])])");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, distinct, importChain);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel, pr);

				PixelList pl = ApiSemossTestInsightUtils.getInsight().getPixelList();
		String pixelId = pl.get(0).getId();
		
		Insight i = ApiSemossTestInsightUtils.getInsight();
		Map<String, Object> param = new HashMap<>();
		Map<String, Object> paramMap = new HashMap<>();
		Map<String, Object> tableMap = new HashMap<>();
		paramMap.put("cone", tableMap);
		param.put(pixelId, paramMap);
		
		NounMetadata paramMapNM = new NounMetadata(param, PixelDataType.PARAM_VALUES_MAP);
		i.getVarStore().put("PARAM_OPTIONS", paramMapNM);
		
		String pixel2 = ApiSemossTestUtils.buildPixelCall(SetParamsReactor.class, ReactorKeysEnum.PIXEL_ID.getKey(),
				pixelId, ReactorKeysEnum.VALUE.getKey(), false, ReactorKeysEnum.COLUMN.getKey(), "cone");

		NounMetadata nm2 = ApiSemossTestUtils.processPixel(pixel2);

		Object reactorValue = nm2.getValue();
		assertNotNull(reactorValue);
		assertEquals("Parameters set ", reactorValue.toString());
		assertEquals(PixelDataType.CONST_STRING, nm2.getNounType());

	}

	
}
