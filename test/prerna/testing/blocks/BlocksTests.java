package prerna.testing.blocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import prerna.reactor.blocks.AddBlockReactor;
import prerna.reactor.blocks.DeleteBlockReactor;
import prerna.reactor.blocks.EditBlockReactor;
import prerna.reactor.blocks.GetBlockReactor;
import prerna.reactor.blocks.ListThemeDataReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

// Tests the Blocks reactors as well as some of the utils

public class BlocksTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void checkDefaultBlocks() throws SQLException {
		List<String> blocksNames = BlocksThemeUtils.getBlockNames();
		Set<String> test = new HashSet<String>(blocksNames);
		assert blocksNames.size() == test.size();
		Set<String> ground = new HashSet<String>(BlocksThemeUtils.BASE_BLOCKS);
		assert test.containsAll(ground);
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

	private String validateAdd() {
		Map<String, Object> inputMap = makeTestObject();
		String pixel = ApiSemossTestUtils.buildPixelCall(AddBlockReactor.class, ReactorKeysEnum.DATA_TYPE_MAP.getKey(), inputMap, "tableName", "BLOCKS_TEMPLATE");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String outputId = nm.getValue().toString();
		
		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
		Object name = ((HashMap<String, Object>) checkNm.getValue()).get("NAME");
		assertEquals("Test Block", name);
		
		return outputId;
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
	
	private static Map<String, Object> makeEditObject(String id) {
		Map<String, Object> inputMap = new HashMap<>();
		inputMap.put("id", id);
		inputMap.put("name", "Test Block 2");
		inputMap.put("section", "SECTION_TEST_2");
		inputMap.put("image", "test_url_2.png");
		inputMap.put("block_json", "{widget: 'test-block-2', data: {label: 'Test Block 2', source: ''}, listeners: {}, slots: {}}");
		inputMap.put("classification", "TEST_2");
		return inputMap;
	}
	
	@Test
	public void checkEdit() {
		String outputId = validateAdd();
		Map<String, Object> inputMap = makeEditObject(outputId);
		String pixel = ApiSemossTestUtils.buildPixelCall(EditBlockReactor.class, "tableName", "BLOCKS_TEMPLATE", ReactorKeysEnum.MAP.getKey(), inputMap);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
		
//		Get object and check vals
		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
		Object name = ((HashMap<String, Object>) checkNm.getValue()).get("NAME");
		assertEquals("Test Block 2", name);
	}
	
	@Test
	public void checkSoftDelete() {
		String outputId = validateAdd();
		String pixel = ApiSemossTestUtils.buildPixelCall(DeleteBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
		
//		Get object and check vals
		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
		assertEquals(new HashMap<>(), checkNm.getValue());
		
		String filter = ApiSemossTestUtils.buildFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "IS_LATEST", "==", 0);
		String checkExistsPixel = ApiSemossTestUtils.buildPixelCall(ListThemeDataReactor.class, "tableName", "BLOCKS_TEMPLATE", ReactorKeysEnum.FILTERS.getKey(), filter);
		NounMetadata checkExistsNm = ApiSemossTestUtils.processPixel(checkExistsPixel);
		assertNotEquals(new HashMap<>(), checkExistsNm.getValue());
		List<Object> output = (List<Object>) checkExistsNm.getValue();
		assert output.size() == 1;
		Map<String, Object> element = (Map<String, Object>) output.get(0);
		assertEquals(outputId, element.get("ID"));
	}

	@Test
	public void checkHardDelete() {
		String outputId = validateAdd();
		String pixel = ApiSemossTestUtils.buildPixelCall(DeleteBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE", "hardDelete", true);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
		
//		Get object and check vals
		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
		assertEquals(new HashMap<>(), checkNm.getValue());
		
		String filter = ApiSemossTestUtils.buildFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "IS_LATEST", "==", 0);
		String checkExistsPixel = ApiSemossTestUtils.buildPixelCall(ListThemeDataReactor.class, "tableName", "BLOCKS_TEMPLATE", ReactorKeysEnum.FILTERS.getKey(), filter);
		NounMetadata checkExistsNm = ApiSemossTestUtils.processPixel(checkExistsPixel);
		assertEquals(new ArrayList<>(), checkExistsNm.getValue());
	}
	
	@Test
	public void listThemeData() {
		String filter = ApiSemossTestUtils.buildFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "ID", "==", "BT002");
		String pixel = ApiSemossTestUtils.buildPixelCall(ListThemeDataReactor.class, "tableName", "BLOCKS_TEMPLATE", ReactorKeysEnum.FILTERS.getKey(), filter);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(new ArrayList<>(), nm.getValue());
		List<Object> output = (List<Object>) nm.getValue();
		assert output.size() == 1;
		Map<String, Object> element = (Map<String, Object>) output.get(0);
		assertEquals("BT002", element.get("ID"));
	}

}
