package prerna.rpa.quartz.jobs.insight;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import prerna.engine.api.IHeadersDataRow;

public class IteratorLambdaFunctions {

	public static Set<IHeadersDataRow> getRowsSatisfyingCondition(Iterator<IHeadersDataRow> iterator, Predicate<IHeadersDataRow> tester) {
		Set<IHeadersDataRow> rows = new HashSet<IHeadersDataRow>();
		while (iterator.hasNext()) {
			IHeadersDataRow row = iterator.next();
			if (tester.test(row)) {
				rows.add(row);
			}
		}
		return rows;
	}
	
}
