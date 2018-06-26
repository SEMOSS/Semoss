//package prerna.engine.impl.solr;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//
//import prerna.algorithm.api.SemossDataType;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.om.HeadersDataRow;
//import prerna.query.querystruct.SelectQueryStruct;
//import prerna.query.querystruct.selectors.IQuerySelector;
//import prerna.rdf.engine.wrappers.AbstractWrapper;
//
//public class SolrIterator extends AbstractWrapper implements IRawSelectWrapper {
//	
//	private SolrDocumentList list = null;
//	private long totalCount = 0;
//	private int returnSize = 0;
//	private int curIndex = 0;
//	private SelectQueryStruct qs2;
//	protected String[] headers = null;
//	private boolean returnCount;
//
//	public SolrIterator(SolrDocumentList list, SelectQueryStruct qs2) {
//		this.list = list;
//		this.qs2 = qs2;
//		this.returnCount = false;
//		this.headers = getReturnHeaders(this.returnCount);
//		this.returnSize = this.list.size();
//	}
//
//	@Override
//	public boolean hasNext() {
//		if (this.curIndex < this.returnSize) {
//			return true;
//		} else {
//			return false;
//		}
//	}
//
//	@Override
//	public IHeadersDataRow next() {
//		if (returnCount) {
//			Object[] dataRow = new Object[] { totalCount * getReturnHeaders(false).length };
//			HeadersDataRow row = new HeadersDataRow(new String[] { "NUM_FOUND" }, dataRow);
//			curIndex++;
//			return row;
//		}
//		// get the next document
//		SolrDocument doc = list.get(curIndex);
//		// get the fields that compose this document
//		Map<String, Object> docValues = doc.getFieldValueMap();
//		// loop through and turn the values into an array
//		int size = this.headers.length;
//		Object[] values = new Object[size];
//		for (int i = 0; i < size; i++) {
//			values[i] = docValues.get(headers[i]);
//		}
//
//		// update the curIndex
//		this.curIndex++;
//
//		// return the row
//		HeadersDataRow row = new HeadersDataRow(headers, values);
//		return row;
//	}
//
//	/**
//	 * Get the headers This uses the qs to determine the ordering since the solr
//	 * query returns a map
//	 * 
//	 * @param returnCount
//	 * @return
//	 */
//	private String[] getReturnHeaders(boolean returnCount) {
//		String[] returnHeaders = null;
//		if (returnCount) {
//			returnHeaders = new String[] { "NUM_FOUND" };
//		}
//		List<IQuerySelector> selectorData = this.qs2.getSelectors();
//		ArrayList<String> dV = new ArrayList<>();
//		for (IQuerySelector selector : selectorData) {
//			List<String> props = new ArrayList<>();
//			dV.add(selector.getAlias());
//			for (String prop : props) {
//				if (!prop.contains("PRIM_KEY_PLACEHOLDER")) {
//					dV.add(prop);
//				}
//			}
//
//		}
//		returnHeaders = dV.toArray(new String[dV.size()]);
//		return returnHeaders;
//	}
//
//	@Override
//	public String[] getHeaders() {
//		return this.headers;
//	}
//
//	@Override
//	public SemossDataType[] getTypes() {
//		return new SemossDataType[this.headers.length];
//	}
//
//	@Override
//	public long getNumRecords() {
//		return this.returnSize * this.headers.length;
//	}
//
//	@Override
//	public void reset() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void execute() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void cleanUp() {
//		// TODO Auto-generated method stub
//		
//	}
//}
