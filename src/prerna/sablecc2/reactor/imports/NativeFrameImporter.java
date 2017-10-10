package prerna.sablecc2.reactor.imports;

import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.Join;

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
		if(this.qs.getQsType() != QueryStruct2.QUERY_STRUCT_TYPE.ENGINE || !this.qs.getEngineName().equals(this.dataframe.getEngineName()) ) {
			// first, load the entire native frame into h2frame
			QueryStruct2 nativeQs = dataframe.getQueryStruct();
			Iterator<IHeadersDataRow> nativeFrameIt = dataframe.query(nativeQs);
			H2Frame h2Frame = new H2Frame();
			H2Importer h2Importer = new H2Importer(h2Frame, nativeQs, nativeFrameIt);
			h2Importer.insertData();
			
			// now, we want to merge this new data into it
			h2Importer = new H2Importer(h2Frame, this.qs);
			h2Importer.mergeData(joins);
			return h2Frame;
		} else {
			ImportUtility.parseQueryStructIntoMeta(this.dataframe, this.qs);
			this.dataframe.mergeQueryStruct(this.qs);
			return this.dataframe;
		}
	}

}
