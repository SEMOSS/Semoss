package prerna.sablecc2.reactor.imports;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class TinkerImporter implements IImporter {

	private TinkerFrame dataframe;
	private QueryStruct2 qs;
	private Iterator<IHeadersDataRow> it;
	
	// ugh... edge hash never seems to go away does it
	Map<String, Set<String>> edgeHash;
	public TinkerImporter(TinkerFrame dataframe, QueryStruct2 qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = it;
	}
	public TinkerImporter(TinkerFrame dataframe, QueryStruct2 qs) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
	}

	@Override
	public void insertData() {
		// get the edge hash so we know how to add data connections
		this.edgeHash = ImportUtility.getEdgeHash(this.qs);
		// create the metadata
		ImportUtility.parseQueryStructAsGraph(this.dataframe, this.qs, this.edgeHash);
				
		Map<Integer, Set<Integer>> cardinality = null;
		while(this.it.hasNext()) {
			IHeadersDataRow row = it.next();
			String[] headers = row.getHeaders();
			if(cardinality == null) {
				cardinality = Utility.getCardinalityOfValues(headers, this.edgeHash);
			}
			dataframe.addRelationship(headers, row.getValues(), cardinality);
		}
	}
	
	@Override
	public void insertData(OwlTemporalEngineMeta metaData) {
		this.dataframe.setMetaData(metaData);
		// get the edge hash so we know how to add data connections
		this.edgeHash = ImportUtility.getEdgeHash(this.qs);
		Map<Integer, Set<Integer>> cardinality = null;
		while(this.it.hasNext()) {
			IHeadersDataRow row = it.next();
			String[] headers = row.getHeaders();
			if(cardinality == null) {
				cardinality = Utility.getCardinalityOfValues(headers, this.edgeHash);
			}
			dataframe.addRelationship(headers, row.getValues(), cardinality);
		}
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		// get the edge hash so we know how to add data connections
		this.edgeHash = ImportUtility.getEdgeHash(this.qs);
		// create the metadata
		
		//TODO: take into consideration the join column names
		//TODO: take into consideration the join column names
		//TODO: take into consideration the join column names
		//TODO: take into consideration the join column names
		//TODO: take into consideration the join column names

		ImportUtility.parseQueryStructAsGraph(this.dataframe, this.qs, this.edgeHash);
				
		Map<Integer, Set<Integer>> cardinality = null;
		while(this.it.hasNext()) {
			IHeadersDataRow row = it.next();
			String[] headers = row.getHeaders();
			if(cardinality == null) {
				cardinality = Utility.getCardinalityOfValues(headers, this.edgeHash);
			}
			dataframe.addRelationship(headers, row.getValues(), cardinality);
		}
		return this.dataframe;
	}
}
