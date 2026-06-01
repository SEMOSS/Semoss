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
import prerna.reactor.blocks.GetClientBlocksReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

// Tests the Blocks reactors as well as some of the utils

public class BlocksTests extends AbstractBaseSemossApiTests {
	
//	@Test
//	public void checkGetBlock() {
//		String pixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", "BT001", "tableName", "BLOCKS_TEMPLATE");
//		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
//		assertNotEquals(PixelDataType.ERROR, nm.getValue());
//		Object name = ((HashMap<String, Object>) nm.getValue()).get("NAME");
//		assertEquals("Audio Player", name);
//	}
//	
	@Test
	public void checkAdd() {
		validateAdd();
	}

	private String validateAdd() {
		Map<String, Object> inputMap = makeTestObject();
		String pixel = ApiSemossTestUtils.buildPixelCall(AddBlockReactor.class,
				ReactorKeysEnum.NAME.getKey(), inputMap.get("name"),
				ReactorKeysEnum.SECTION.getKey(), inputMap.get("section"),
				ReactorKeysEnum.JSON.getKey(), inputMap.get("block_json"));
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String outputId = nm.getValue().toString();

//		TODO: Add a getter for that particular string value
//		String filter = ApiSemossTestUtils.buildFilter(ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "IS_LATEST", "==", 1);
//		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetClientBlocksReactor.class,  ReactorKeysEnum.FILTERS.getKey(), filter);
//		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
//		Object name = ((HashMap<String, Object>) checkNm.getValue()).get("NAME");
//		assertEquals("Test Block", name);
		
		return outputId;
	}

	private static Map<String, Object> makeTestObject() {
		Map<String, Object> inputMap = new HashMap<>();
		inputMap.put("name", "Test Block");
		inputMap.put("section", "SECTION_TEST");
		inputMap.put("block_json", "{widget: 'test-block', data: {label: 'Test Block', source: ''}, listeners: {}, slots: {}}");
		return inputMap;
	}
//	
//	private static Map<String, Object> makeEditObject(String id) {
//		Map<String, Object> inputMap = new HashMap<>();
//		inputMap.put("id", id);
//		inputMap.put("name", "Test Block 2");
//		inputMap.put("section", "SECTION_TEST_2");
//		inputMap.put("image", "test_url_2.png");
//		inputMap.put("block_json", "{widget: 'test-block-2', data: {label: 'Test Block 2', source: ''}, listeners: {}, slots: {}}");
//		inputMap.put("classification", "TEST_2");
//		return inputMap;
//	}
//	
//	@Test
//	public void checkEdit() {
//		String outputId = validateAdd();
//		Map<String, Object> inputMap = makeEditObject(outputId);
//		String pixel = ApiSemossTestUtils.buildPixelCall(EditBlockReactor.class, "tableName", "BLOCKS_TEMPLATE", ReactorKeysEnum.MAP.getKey(), inputMap);
//		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
//		assertEquals(true, nm.getValue());
//		
////		Get object and check vals
//		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
//		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
//		Object name = ((HashMap<String, Object>) checkNm.getValue()).get("NAME");
//		assertEquals("Test Block 2", name);
//	}
//	
	@Test
	public void checkSoftDelete() {
		String outputId = validateAdd();
		String pixel = ApiSemossTestUtils.buildPixelCall(DeleteBlockReactor.class, "blockId", outputId);
		System.out.println(pixel);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
		
////		Get object and check vals
//		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
//		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
//		assertEquals(new HashMap<>(), checkNm.getValue());
		
		String filter = ApiSemossTestUtils.buildFilter(ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "IS_LATEST", "==", 0);
		String checkExistsPixel = ApiSemossTestUtils.buildPixelCall(GetClientBlocksReactor.class, ReactorKeysEnum.FILTERS.getKey(), filter);
		NounMetadata checkExistsNm = ApiSemossTestUtils.processPixel(checkExistsPixel);
		List<Object> output = (List<Object>) checkExistsNm.getValue();
		assert output.size() == 1;
		Map<String, Object> element = (Map<String, Object>) output.get(0);
		assertEquals(outputId, element.get("id"));
	}

	@Test
	public void checkHardDelete() {
		String outputId = validateAdd();
		String pixel = ApiSemossTestUtils.buildPixelCall(DeleteBlockReactor.class, "blockId", outputId, "hardDelete", true);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(true, nm.getValue());
		
////		Get object and check vals
//		String checkPixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", outputId, "tableName", "BLOCKS_TEMPLATE");
//		NounMetadata checkNm = ApiSemossTestUtils.processPixel(checkPixel);
//		assertEquals(new HashMap<>(), checkNm.getValue());
		
		String filter = ApiSemossTestUtils.buildFilter(ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "IS_LATEST", "==", 0);
		String checkExistsPixel = ApiSemossTestUtils.buildPixelCall(GetClientBlocksReactor.class, ReactorKeysEnum.FILTERS.getKey(), filter);
		NounMetadata checkExistsNm = ApiSemossTestUtils.processPixel(checkExistsPixel);
		assertEquals(new ArrayList<>(), checkExistsNm.getValue());
	}
	
	@Test
	public void getAllClientBlocks() {
//		Add multiple blocks to the db
		String outputId1 = validateAdd();
		String outputId2 = validateAdd();
		
//		String filter = ApiSemossTestUtils.buildFilter(ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "ID", "==", "BT002");
		String pixel = ApiSemossTestUtils.buildPixelCall(GetClientBlocksReactor.class);
		System.out.println(pixel);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		List<Object> output = (List<Object>) nm.getValue();
		assert output.size() == 2;
	}

}
