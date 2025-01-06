package prerna.testing.blocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.blocks.GetBlockReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.theme.BlocksThemeUtils;

// Tests the Blocks reactors as well as some of the utils

public class BlocksTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void checkDefaultBlocks() throws SQLException {
		List<String> blocksNames = BlocksThemeUtils.getBlockNames();
		assert blocksNames.equals(BlocksThemeUtils.BASE_BLOCKS);
	}
	
	@Test
	public void checkGetBlock() {
//		Map<String, Object> inputMap = new HashMap<>();
//		inputMap.put("blockId", "BT001");
//		inputMap.put("tableName", "BLOCKS_TEMPLATE");
		String pixel = ApiSemossTestUtils.buildPixelCall(GetBlockReactor.class, "blockId", "BT001", "tableName", "BLOCKS_TEMPLATE");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(PixelDataType.ERROR, nm.getValue());
		Object name = ((HashMap<String, Object>) nm.getValue()).get("NAME");
		assertEquals("Audio Player", name);
	}
	
	@Test
	public void checkAddEditDeleteBlock() throws Exception {
//		throw new Exception("hello there");
	}

}
