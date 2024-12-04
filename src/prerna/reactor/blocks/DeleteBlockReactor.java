package prerna.reactor.blocks;

import java.sql.SQLException;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class DeleteBlockReactor extends AbstractReactor {

	public DeleteBlockReactor() {
		this.keysToGet =  new String[] {"blockId"};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String blockId = this.keyValue.get("blockId");
		boolean result = false;
		try {
			result = BlocksThemeUtils.deleteBlock(blockId, ThemeDbTable.BLOCKS_TEMPLATE);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}

}
