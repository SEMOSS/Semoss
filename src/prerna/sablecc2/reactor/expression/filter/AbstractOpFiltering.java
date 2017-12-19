package prerna.sablecc2.reactor.expression.filter;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.expression.OpBasic;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.reactor.qs.QueryFilterReactor;

public abstract class AbstractOpFiltering extends OpBasic {

	public boolean isQuery() {
		if(this.parentReactor instanceof QueryFilterReactor || this.parentReactor instanceof AbstractFilterReactor) {
			return true;
		}
		if(this.parentReactor instanceof AbstractOpFiltering) {
			return ((AbstractOpFiltering) this.parentReactor).isQuery();
		}
		return false;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VALUES.getKey())) {
			return "The list of conditional values to evaluate";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
