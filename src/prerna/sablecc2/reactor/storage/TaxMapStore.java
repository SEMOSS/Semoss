package prerna.sablecc2.reactor.storage;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;

public class TaxMapStore extends MapStore {

	@Override
	public Iterator<IHeadersDataRow> getIterator() {
		return new TaxMapHeaderDataRowIterator(this);
	}
}
