package prerna.testing.query.querystruct;

import prerna.testing.AbstractBaseSemossApiTests;

public class SetParamsReactorApiTests extends AbstractBaseSemossApiTests {
	
	/**
	 * 
	 * NOTE: unable to test this class. Could not find a way to set the Insight var store to contain
	 * a var map with the ImportParamOptionsReactor.PARAM_OPTIONS key anywhere in the code base. I looked 
	 * for a little but determined it wasn't worth the time. Unit tests will suffice for now but will comment
	 * out the work in case we come back to this in the future and need to explain what I was trying to do.
	 * 
	 */

//	@Test
//	void executeOnColumn() {
//		String engine = ApiSemossTestEngineUtils.createBasicEngine();
//
//		PixelRunner pr = new PixelRunner();
//		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
//		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
//		PixelChain distinct = new PixelChain("Distinct(false)");
//		PixelChain importChain = new PixelChain(
//				"Import(frame = [ CreateFrame(frameType=[GRID], override = [true]).as([\"test_FRAME000001\"])])");
//
//		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, distinct, importChain);
//		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel, pr);
//
//		Insight i = ApiSemossTestInsightUtils.getInsight();
//		Map<String, Object> param = new HashMap<>();
//		i.getVarStore().put("PARAM_OPTIONS", (NounMetadata) param);
//
//		PixelList pl = ApiSemossTestInsightUtils.getInsight().getPixelList();
//		String pixelId = pl.get(0).getId();
//		String pixel2 = ApiSemossTestUtils.buildPixelCall(SetParamsReactor.class, ReactorKeysEnum.PIXEL_ID.getKey(),
//				pixelId, ReactorKeysEnum.VALUE.getKey(), false, ReactorKeysEnum.COLUMN.getKey(), "cone");
//
//		NounMetadata nm2 = ApiSemossTestUtils.processPixel(pixel2);
//
//		Object reactorValue = nm2.getValue();
//		assertNotNull(reactorValue);
//		assertEquals(PixelDataType.CONST_STRING, nm2.getNounType());
//
//	}

	
}
