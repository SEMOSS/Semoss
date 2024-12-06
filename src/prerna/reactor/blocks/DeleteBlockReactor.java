package prerna.reactor.blocks;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class DeleteBlockReactor extends AbstractReactor {

	public DeleteBlockReactor() {
		this.keysToGet = new String[] { "blockId", "tableName", "hardDelete" };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		boolean hardDelete = false;
		GenRowStruct grs = this.store.getNoun("hardDelete");
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.BOOLEAN);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				hardDelete = (boolean) mapNouns.get(0).getValue();
			}
		}
		String blockId = this.keyValue.get("blockId");
		String tableName = this.keyValue.get("tableName");
		boolean result = false;
		try {
			result = BlocksThemeUtils.deleteBlock(blockId, tableName, hardDelete);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}

}
