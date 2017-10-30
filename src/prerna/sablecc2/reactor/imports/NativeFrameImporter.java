package prerna.sablecc2.reactor.imports;

import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.QueryStruct2.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.QueryStructConverter;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class NativeFrameImporter implements IImporter {

	private NativeFrame dataframe;
	private QueryStruct2 qs;
	
	public NativeFrameImporter(NativeFrame dataframe, QueryStruct2 qs) {
		this.dataframe = dataframe;
		this.qs = qs;
	}
	
	@Override
	public void insertData() {
		ImportUtility.parseQueryStructIntoMeta(this.dataframe, this.qs);
		this.dataframe.mergeQueryStruct(this.qs);
	}

	@Override
	public void insertData(OwlTemporalEngineMeta metaData) {
		this.dataframe.setMetaData(metaData);
		this.dataframe.mergeQueryStruct(this.qs);
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		QUERY_STRUCT_TYPE qsType = this.qs.getQsType();
		if(qsType == QUERY_STRUCT_TYPE.ENGINE && this.dataframe.getEngineName().equals(this.qs.getEngineName())) {
			// this is the case where we can do an easy merge
			ImportUtility.parseQueryStructIntoMeta(this.dataframe, this.qs);
			this.dataframe.mergeQueryStruct(this.qs);
			return this.dataframe;
		} else {
			// this is the case where we are going across databases or doing some algorithm
			// need to persist the data so we can do this
			// we will flush and return a new frame to accomplish this
			return generateNewFrame(joins);
		}
	}
	
	/**
	 * Generate a new frame from the existing native query
	 * @param joins
	 * @return
	 */
	private ITableDataFrame generateNewFrame(List<Join> joins) {
		// first, load the entire native frame into rframe
		QueryStruct2 nativeQs = this.dataframe.getQueryStruct();
		// need to convert the native QS to properly form the RDataTable
		nativeQs = QueryStructConverter.getPhysicalQs(nativeQs, this.dataframe.getMetaData());
		Iterator<IHeadersDataRow> nativeFrameIt = this.dataframe.query(nativeQs);
		RDataTable rFrame = new RDataTable(Utility.getRandomString(8));
		RImporter rImporter = new RImporter(rFrame, nativeQs, nativeFrameIt);
		rImporter.insertData();
		
		// now, we want to merge this new data into it
		rImporter = new RImporter(rFrame, this.qs, this.dataframe.query(this.qs));
		rImporter.mergeData(joins);
		return rFrame;
	}
}
