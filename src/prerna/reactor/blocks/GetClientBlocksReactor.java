package prerna.reactor.blocks;

import java.sql.SQLException;
import java.util.List;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
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
		this.keysToGet = new String[] { ReactorKeysEnum.ALL_BLOCKS_VISIBLE.getKey(), ReactorKeysEnum.FILTERS.getKey() };
		this.keyRequired = new int[] { 0,0 };
	}

	
	@Override
	public NounMetadata execute() {
	    String userId = this.insight.getUserId();
	    if (userId == null || userId.isEmpty()) {
	        throw new IllegalArgumentException("User is not properly logged in.");
	    }
	 
	    this.organizeKeys();
	    
	    boolean allBlocksVisible = false;
	    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ALL_BLOCKS_VISIBLE.getKey());
	    if (grs != null && !grs.isEmpty()) {
	        List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.BOOLEAN);
	        if (mapNouns != null && !mapNouns.isEmpty()) {
	            allBlocksVisible = (boolean) mapNouns.get(0).getValue();
	        }
	    }
	    
	    String tableName = ThemeDbTable.BLOCKS_TABLE.toString();
	    GenRowFilters filters = getFilters();
	    
	    if (!allBlocksVisible) {
	        if (filters == null) {
	            filters = new GenRowFilters();
	        }
	        // filters blocks by userId
	        filters.addFilters(SimpleQueryFilter.makeColToValFilter(
	            ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "CREATED_BY", "==", userId,PixelDataType.CONST_STRING ));
	    }
	    Object blocks;
	    try {
	        blocks = BlocksThemeUtils.getClientBlocks(tableName, filters);
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
			System.out.println(qs);
			GenRowFilters filters = qs.getCombinedFilters();
			return filters;
		}
		return null;
	}

}
