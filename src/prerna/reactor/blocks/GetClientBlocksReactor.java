package prerna.reactor.blocks;

import java.sql.SQLException;
import java.util.Map;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class GetClientBlocksReactor extends AbstractReactor {

	public GetClientBlocksReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTERS.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {

		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}

		this.organizeKeys();
		String tableName = ThemeDbTable.BLOCKS_TABLE.toString();
		GenRowFilters additionalFilters = getFilters();
		Object blocks = null;
		try {
			blocks = BlocksThemeUtils.getThemeData(tableName, additionalFilters);
		} catch (SQLException e) {
			throw new SemossPixelException(e);
		}
		return new NounMetadata(blocks, PixelDataType.MAP);
	}

	protected GenRowFilters getFilters() {
		GenRowStruct inputsGRS = this.store.getNoun(ReactorKeysEnum.FILTERS.getKey());
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata filterNoun = inputsGRS.getNoun(0);
			SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
			GenRowFilters filters = qs.getCombinedFilters();
			return filters;
		}
		return null;
	}

}
