package prerna.reactor.blocks;

import java.sql.SQLException;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class GetBlockReactor extends AbstractReactor {

	public GetBlockReactor() {
		this.keysToGet = new String[] { "blockId", "tableName" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}

		this.organizeKeys();
		String blockId = this.keyValue.get("blockId");
		String tableName = this.keyValue.get("tableName");
		Map<String, Object> block = null;
		try {
			block = BlocksThemeUtils.getBlock(blockId, tableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new NounMetadata(block, PixelDataType.MAP);
	}

}
