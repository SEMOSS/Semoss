package prerna.reactor.blocks;

import java.sql.SQLException;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class ListBlocksReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		Map<String, Map<String, Object>> blocks = null;
		try {
			blocks = BlocksThemeUtils.getBlocks(ThemeDbTable.BLOCKS_TEMPLATE);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new NounMetadata(blocks, PixelDataType.MAP);
	}

}
