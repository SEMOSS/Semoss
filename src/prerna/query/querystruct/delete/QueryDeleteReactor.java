package prerna.query.querystruct.delete;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class QueryDeleteReactor extends AbstractQueryStructReactor {
	
	@Override
	public AbstractQueryStruct createQueryStruct() {
		SelectQueryStruct qs = new SelectQueryStruct();
		// merge any existing values
		if(this.qs != null) {
			qs.merge(this.qs);
		}
		
		// Table
		GenRowStruct tab_grs = this.store.getNoun("from");
		List<IQuerySelector> selectors = new Vector<IQuerySelector>();

		QueryColumnSelector sel = new QueryColumnSelector(tab_grs.get(0).toString());
		selectors.add(sel);
		
		qs.setSelectors(selectors);
		
		this.qs = qs;
		return qs;
	}
	
	public void setNounStore(NounStore ns) {
		this.store = ns;
	}
	
	public void setQs(SelectQueryStruct qs) {
		this.qs = qs;
	}
}
