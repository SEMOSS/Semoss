package prerna.sablecc2.reactor.qs;

import prerna.ds.QueryStructSelector;

public class SumReactor extends SelectReactor {

	@Override
	public QueryStructSelector getSelector(String table, String column) {
		QueryStructSelector selector = super.getSelector(table, column);
		selector.setMath("SUM");
		selector.setAlias("SUM_"+selector.getAlias());
		return selector;
	}
}
