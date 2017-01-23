package prerna.ds.h2;

import java.util.Iterator;
import java.util.NoSuchElementException;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;

public class H2HeadersDataRowIterator implements Iterator<IHeadersDataRow>{

	H2Iterator baseIterator;
	
	public H2HeadersDataRowIterator(H2Iterator iterator) {
		this.baseIterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		if(baseIterator == null) return false;
		return baseIterator.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		if(hasNext()) {
			Object[] values = baseIterator.next();
			String[] headers = baseIterator.getHeaders();
			HeadersDataRow nextData = new HeadersDataRow(headers, values, values);
			return nextData;
		}
		throw new NoSuchElementException("No more elements in Array");
	}

}
