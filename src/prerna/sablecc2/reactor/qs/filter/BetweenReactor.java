package prerna.sablecc2.reactor.qs.filter;

import java.util.List;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class BetweenReactor extends FilterReactor {
	
	public BetweenReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.START.getKey(), ReactorKeysEnum.END.getKey()};
	}

	protected AbstractQueryStruct createQueryStruct() {
		// for now we can only handle simple values
		List<Object> filters = this.curRow.getAllValues();
		// there should be three values
		// first one is the column
		// second is the start
		// third is the end
		BetweenQueryFilter bqf = new BetweenQueryFilter();
		
		Object oColumn = filters.get(0);
		if(oColumn instanceof QueryColumnSelector)
			bqf.setColumn((QueryColumnSelector)oColumn);
	
		if(filters.size() >= 1)
			bqf.setStart(filters.get(1));
		
		if(filters.size() >= 2)
			bqf.setEnd(filters.get(2));
		
		qs.addExplicitFilter(bqf);
		
		return qs;
	}
}
