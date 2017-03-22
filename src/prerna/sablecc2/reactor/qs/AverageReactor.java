package prerna.sablecc2.reactor.qs;

import prerna.ds.querystruct.QueryStructSelector;

public class AverageReactor extends SelectReactor{

	@Override
	public QueryStructSelector getSelector(String table, String column) {
		QueryStructSelector selector = super.getSelector(table, column);
		selector.setMath("AVG");
		selector.setAlias("AVERAGE_"+selector.getAlias());
		return selector;
	}
}
