package prerna.engine.impl.solr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import prerna.ds.QueryStruct;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.rdf.engine.wrappers.AbstractWrapper;

public class RawSolrSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private SolrDocumentList list = null;
	private long totalCount = 0;
	private int returnSize = 0;
	private int curIndex = 0;
	
	//TODO not sure how i should take into consideration the count...
	private boolean returnCount;
	
	@Override
	public void execute() {
		// we cast interp so that we can compose iterator since all other engines return a query string
		SolrInterpreter interp = (SolrInterpreter) this.engine.getQueryInterpreter();
		if(qs.getLimit() == -1) {
			// the user has no defined a limit
			// so we will need to return everything
			// but solr adds a limit regardless
			// so we will query to get the total amount
			// and then add that as the limit
			qs.setLimit(0);
			interp.setQueryStruct(qs);
			SolrQuery query = interp.composeSolrQuery();
			SolrDocumentList emptyList = ((SolrEngine) this.engine).execQuery(query);
			long newLimit = emptyList.getNumFound();
			qs.setLimit(newLimit);
		}
		interp.setQueryStruct(qs);
		SolrQuery query = interp.composeSolrQuery();
		this.list = ((SolrEngine) this.engine).execQuery(query);
		this.returnSize = this.list.size();
		
		// if this is meant to be a count query
		// set the size to 1
		// and return
		if(qs.getPerformCount() == QueryStruct.COUNT_CELLS || qs.getPerformCount() == QueryStruct.COUNT_DISTINCT_SELECTORS) {
			this.returnCount = true;
			this.returnSize = 1;
			this.totalCount = list.getNumFound();
		}
		
		getDisplayVariables();
	}
	
	@Override
	public boolean hasNext() {
		if(this.curIndex < this.returnSize) {
			return true;
		} else {
			return false;
		}
	}

	
	@Override
	public IHeadersDataRow next() {
		if(returnCount) {
			Object[] dataRow = new Object[]{totalCount * getReturnHeaders(false).length};
			HeadersDataRow row = new HeadersDataRow(new String[]{"NUM_FOUND"}, dataRow, dataRow);
			curIndex++;
			return row;
		}
		// get the next document
		SolrDocument doc = list.get(curIndex);
		// get the fields that compose this document
		Map<String, Object> docValues = doc.getFieldValueMap();
		// loop through and turn the values into an array
		int size = this.displayVar.length;
		Object[] values = new Object[size];
		for(int i = 0; i < size; i++) {
			values[i] = docValues.get(displayVar[i]);
		}
		
		// update the curIndex
		this.curIndex++;
		
		// return the row
		HeadersDataRow row = new HeadersDataRow(displayVar, values, values);
		return row;
	}

	@Override
	public String[] getDisplayVariables() {
		if(this.displayVar == null) {
			this.displayVar = getReturnHeaders(this.returnCount);
		}
		return this.displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		return getDisplayVariables();
	}
	
	/**
	 * Get the headers
	 * This uses the qs to determine the ordering
	 * since the solr query returns a map
	 * @param returnCount
	 * @return
	 */
	private String[] getReturnHeaders(boolean returnCount) {
		String[] returnHeaders = null;
		if(returnCount) {
			returnHeaders = new String[]{"NUM_FOUND"};
		}
		Map<String, List<String>> selectors = this.qs.selectors;
		ArrayList<String> dV = new ArrayList<>();
		for (String selectorKey : selectors.keySet()) {
			List<String> props = selectors.get(selectorKey);
			dV.add(selectorKey);
			for (String prop : props) {
				if (!prop.contains("PRIM_KEY_PLACEHOLDER")) {
					dV.add(prop);
				}
			}

		}
		returnHeaders = dV.toArray(new String[dV.size()]);
		return returnHeaders;
	}

}
