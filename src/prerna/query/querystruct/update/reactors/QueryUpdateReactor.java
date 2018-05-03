package prerna.query.querystruct.update.reactors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class QueryUpdateReactor extends AbstractQueryStructReactor {

	public QueryUpdateReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey()};
	}
	
	@Override
	protected UpdateQueryStruct createQueryStruct() {
		UpdateQueryStruct qs = new UpdateQueryStruct();
		// merge any existing values
		qs.merge(this.qs);
		
		GenRowStruct col_grs = this.store.getNoun(this.keysToGet[0]);
		GenRowStruct val_grs = this.store.getNoun(this.keysToGet[1]);
		
		List<IQuerySelector> columns = new Vector<IQuerySelector>();
		List<Object> values = new Vector<Object>();
		
		for(int i = 0; i < col_grs.size(); i++) {
			String col = col_grs.get(i) + "";
			Object val = val_grs.get(i);
			
			QueryColumnSelector colS = new QueryColumnSelector(col);
			columns.add(colS);
			values.add(val);
		}
		
		qs.setSelectors(columns);
		qs.setValues(values);
		this.qs = qs;
		return qs;
	}
}
