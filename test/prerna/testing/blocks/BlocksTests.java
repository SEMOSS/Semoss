package prerna.testing.blocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import prerna.reactor.blocks.AddBlockReactor;
import prerna.reactor.blocks.EditBlockReactor;
import prerna.reactor.blocks.GetBlockReactor;
import prerna.reactor.blocks.ListThemeDataReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.theme.BlocksThemeUtils;

// Tests the Blocks reactors as well as some of the utils

public class BlocksTests extends AbstractBaseSemossApiTests {
	
	private static final Logger classLogger = LogManager.getLogger(BlocksTests.class);
	
	@Test
	public void checkDefaultBlocks() throws SQLException {
		List<String> blocksNames = BlocksThemeUtils.getBlockNames();
		assertEquals(BlocksThemeUtils.BASE_BLOCKS, blocksNames);
	}
	
	@Test
	public void checkGetBlock() {
		String pixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", "BT001", "tableName", "BLOCKS_TEMPLATE");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(PixelDataType.ERROR, nm.getValue());
		Object name = ((HashMap<String, Object>) nm.getValue()).get("NAME");
		assertEquals("Audio Player", name);
	}
	
	@Test
	public void checkAdd() {
		validateAdd();
	}

	private void validateAdd() {
		Map<String, Object> inputMap = makeTestObject();
		String pixel = ApiSemossTestUtils.buildPixelCall(AddBlockReactor.class, ReactorKeysEnum.DATA_TYPE_MAP.getKey(), inputMap);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
	}

	private static Map<String, Object> makeTestObject() {
		Map<String, Object> inputMap = new HashMap<>();
		inputMap.put("id", "837b2crot7");
		inputMap.put("name", "Test Block");
		inputMap.put("section", "SECTION_TEST");
		inputMap.put("image", "test_url.png");
		inputMap.put("block_json", "{widget: 'test-block', data: {label: 'Test Block', source: ''}, listeners: {}, slots: {}}");
		inputMap.put("classification", "TEST");
		return inputMap;
	}
	
	private static Map<String, Object> makeEditObject() {
		Map<String, Object> inputMap = new HashMap<>();
		inputMap.put("id", "837b2crot7");
		inputMap.put("name", "Test Block 2");
		inputMap.put("section", "SECTION_TEST_2");
		inputMap.put("image", "test_url_2.png");
		inputMap.put("block_json", "{widget: 'test-block-2', data: {label: 'Test Block 2', source: ''}, listeners: {}, slots: {}}");
		inputMap.put("classification", "TEST_2");
		return inputMap;
	}
	
	@Test
	public void checkEdit() {
		validateAdd();
		Map<String, Object> inputMap = makeEditObject();
		String pixel = ApiSemossTestUtils.buildPixelCall(EditBlockReactor.class, ReactorKeysEnum.DATA_TYPE_MAP.getKey(), inputMap);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
		
//		Get object and check vals
		String editPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", "837b2crot7", "tableName", "BLOCKS_TEMPLATE");
		NounMetadata editNm = ApiSemossTestUtils.processPixel(editPixel);
		assertNotEquals(PixelDataType.ERROR, editNm.getValue());
		Object name = ((HashMap<String, Object>) editNm.getValue()).get("NAME");
		assertEquals("Test Block 2", name);
	}
	
	@Test
	public void checkSoftDelete() {
		validateAdd();
		String pixel = ApiSemossTestUtils.buildPixelCall(EditBlockReactor.class, "blockId", "837b2crot7", "tableName", "BLOCKS_TEMPLATE");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
	}
	
	@Test
	public void checkHardDelete() {
		validateAdd();
		String pixel = ApiSemossTestUtils.buildPixelCall(EditBlockReactor.class, "blockId", "837b2crot7", "tableName", "BLOCKS_TEMPLATE", "hardDelete", true);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
	}
	
	@Test
	public void listThemeData() {
		Map<String, Object> inputMap = new HashMap<>();
		String pixel = ApiSemossTestUtils.buildPixelCall(ListThemeDataReactor.class, "tableName", "BLOCKS_TEMPLATE", ReactorKeysEnum.FILTERS.getKey(), inputMap);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(PixelDataType.ERROR, nm.getValue());
		assertEquals(PixelDataType.MAP, nm.getValue());
	}

}
