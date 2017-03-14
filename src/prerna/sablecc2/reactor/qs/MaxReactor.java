package prerna.sablecc2.reactor.qs;

import prerna.ds.QueryStructSelector;

public class MaxReactor extends SelectReactor {

	@Override
	public QueryStructSelector getSelector(String table, String column) {
		QueryStructSelector selector = super.getSelector(table, column);
		selector.setMath("MAX");
		selector.setAlias("MAX_"+selector.getAlias());
		return selector;
	}
}