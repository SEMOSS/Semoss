package prerna.reactor.blocks;

import java.sql.SQLException;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class ListBlocksReactor extends AbstractReactor {
	
	public ListBlocksReactor() {
		this.keysToGet =  new String[] {"tableName"};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String tableName = this.keyValue.get("tableName");
		Object blocks = null;
		try {
			blocks = BlocksThemeUtils.getBlocks(tableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new NounMetadata(blocks, PixelDataType.MAP);
	}

}
