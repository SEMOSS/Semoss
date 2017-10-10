package prerna.ds.nativeframe;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.QueryAggregationEnum;
import prerna.query.querystruct.QueryColumnSelector;
import prerna.query.querystruct.QueryMathSelector;
import prerna.query.querystruct.QueryStruct2;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Utility;

public class NativeFrame extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "NativeFrame";

	private QueryStruct2 qs;
	
	public NativeFrame() {
		super();
		this.qs = new QueryStruct2();
	}

	public void setConnection(String engineName) {
		qs.setEngineName(engineName);
	}

	@Override
	public Double getMax(String columnHeader) {
		if (this.metaData.getHeaderTypeAsEnum(columnHeader, null) == IMetaData.DATA_TYPES.NUMBER) {
			QueryMathSelector selector = new QueryMathSelector();
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			String[] split = columnHeader.split("__");
			innerSelector.setTable(split[0]);
			innerSelector.setColumn(split[1]);
			selector.setInnerSelector(innerSelector);
			selector.setMath(QueryAggregationEnum.MAX);

			QueryStruct2 mQs = new QueryStruct2();
			mQs.addSelector(selector);
			// merge the base filters
			mQs.mergeFilters(qs.getFilters());
			// merge the additional filters added to frame
			mQs.mergeFilters(this.grf);
			// merge the joins
			mQs.mergeRelations(qs.getRelations());

			Iterator<IHeadersDataRow> it = query(mQs);
			return ((Number) it.next().getValues()[1]).doubleValue();
		}
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		if (this.metaData.getHeaderTypeAsEnum(columnHeader, null) == IMetaData.DATA_TYPES.NUMBER) {
			QueryMathSelector selector = new QueryMathSelector();
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			String[] split = columnHeader.split("__");
			innerSelector.setTable(split[0]);
			innerSelector.setColumn(split[1]);
			selector.setInnerSelector(innerSelector);
			selector.setMath(QueryAggregationEnum.MIN);

			QueryStruct2 mQs = new QueryStruct2();
			mQs.addSelector(selector);
			// merge the base filters
			mQs.mergeFilters(qs.getFilters());
			// merge the additional filters added to frame
			mQs.mergeFilters(this.grf);
			// merge the joins
			mQs.mergeRelations(qs.getRelations());

			Iterator<IHeadersDataRow> it = query(mQs);
			return ((Number) it.next().getValues()[1]).doubleValue();
		}
		return null;
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, List<String> attributeUniqueHeaderName) {
		return null;
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		QueryStruct2 newQs = new QueryStruct2();
		QueryColumnSelector selector = new QueryColumnSelector();
		String[] split = columnHeader.split("__");
		selector.setTable(split[0]);
		selector.setColumn(split[1]);
		newQs.addSelector(selector);
		// merge the base filters
		newQs.mergeFilters(qs.getFilters());
		// merge the additional filters added to frame
		newQs.mergeFilters(this.grf);
		// merge the joins
		newQs.mergeRelations(qs.getRelations());

		Iterator<IHeadersDataRow> it = query(newQs);
		List<Object> values = new Vector<Object>();
		while(it.hasNext()) {
			values.add(it.next().getValues()[0]);
		}
		return values.toArray(new Double[]{});
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		QueryStruct2 newQs = new QueryStruct2();
		QueryColumnSelector selector = new QueryColumnSelector();
		String[] split = columnHeader.split("__");
		selector.setTable(split[0]);
		selector.setColumn(split[1]);
		newQs.addSelector(selector);
		// merge the base filters
		newQs.mergeFilters(qs.getFilters());
		// merge the additional filters added to frame
		newQs.mergeFilters(this.grf);
		// merge the joins
		newQs.mergeRelations(qs.getRelations());

		Iterator<IHeadersDataRow> it = query(newQs);
		List<Object> values = new Vector<Object>();
		while(it.hasNext()) {
			values.add(it.next().getValues()[0]);
		}
		return values.toArray();
	}

	@Override
	public String getDataMakerName() {
		return DATA_MAKER_NAME;
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.DATA_CONNECTDB, "prerna.sablecc.DataConnectDBReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.NativeApiReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.NativeImportDataReactor");

		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		
		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.NativeApiReactor");
		return reactorNames;
	}
	
	public void mergeQueryStruct(QueryStruct2 qs) {
		this.qs.merge(qs);
	}
	
	public String getEngineName() {
		return qs.getEngineName();
	}
	
	public QueryStruct2 getQueryStruct() {
		return this.qs;
	}
	
	@Override
	public boolean isEmpty() {
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(Utility.getEngine(this.qs.getEngineName()), this.qs);
		return iterator.hasNext();
	}
	
	@Override
	public Iterator<IHeadersDataRow> query(String query) {
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(Utility.getEngine(this.qs.getEngineName()), query);
		return iterator;
	}

	@Override
	public Iterator<IHeadersDataRow> query(QueryStruct2 qs) {
		// we need to merge the hard filters that were added onto the frame
		qs.mergeFilters(this.qs.getFilters());
		qs.mergeRelations(this.qs.getRelations());
		qs.mergeGroupBy(this.qs.getGroupBy());
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(Utility.getEngine(this.qs.getEngineName()), qs);
		return iterator;
	}
	
	/******************************* UNNECESSARY ON NATIVE FRAME FOR NOW BUT NEED TO OVERRIDE *************************************************/
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
	}

	@Override
	public ITableDataFrame open(String fileName, String userId) {
		return null;
	}

	@Override
	public void removeColumn(String columnHeader) {
	}
	
	@Override
	public void addRow(Object[] cleanCells, String[] headers) {
	}

	@Override
	public void save(String fileName) {
	}

	@Override
	public void removeRelationship(String[] columns, Object[] values) {
	}
}
