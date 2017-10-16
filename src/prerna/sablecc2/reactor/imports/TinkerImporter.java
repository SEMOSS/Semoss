package prerna.sablecc2.reactor.imports;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
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
		// this edge hash will be used as part of the cardinality
		this.edgeHash = ImportUtility.getEdgeHash(this.qs);
		
		// create the metadata
		// modify the QS directly
		// and will be easily able to update 
		Map<String, String> joinMods = qsJoinMod(joins);
		if(!joinMods.isEmpty()) {
			modifyQsSelectorAliasWithJoin(joinMods);
			// since we are updating the QS, we need to get an updated edge hash as well
			ImportUtility.parseQueryStructAsGraph(this.dataframe, this.qs, ImportUtility.getEdgeHash(this.qs));
		} else {
			// dont need to update the QS
			// just use the same edge hash we have
			ImportUtility.parseQueryStructAsGraph(this.dataframe, this.qs, this.edgeHash);
		}
				
		Map<Integer, Set<Integer>> cardinality = null;
		String[] headers = null;
		while(this.it.hasNext()) {
			IHeadersDataRow row = it.next();
			if(cardinality == null) {
				headers = row.getHeaders();
				// get the cardinality with the original headers and original edge hash
				cardinality = Utility.getCardinalityOfValues(headers, this.edgeHash);
				// but once we have that update the headers
				// so we create the vertices correctly
				if(!joinMods.isEmpty()) {
					for(int i = 0; i < headers.length; i++) {
						if(joinMods.containsKey(headers[i])) {
							headers[i] = joinMods.get(headers[i]);
							continue;
						}
					}
				}
			}
			dataframe.addRelationship(headers, row.getValues(), cardinality);
		}
		return this.dataframe;
	}
	
	private void modifyQsSelectorAliasWithJoin(Map<String, String> joinMods) {
		for(String valToFind : joinMods.keySet()) {
			String newValue = joinMods.get(valToFind);
			// loop through the selectors
			// and see if one of them has the alias we are looking for
			for(IQuerySelector selector : this.qs.getSelectors()) {
				if(selector.getAlias().equals(valToFind)) {
					// alright, set the alias to be the same as the join one
					// so we can easily update the metadata
					selector.setAlias(newValue);
					break;
				}
			}
		}
	}
	
	private Map<String, String> qsJoinMod(List<Join> joins) {
		Map<String, String> joinMap = new HashMap<String, String>();
		for(Join j : joins) {
			// s is the frame name
			String s = j.getSelector();
			// q is the query name
			String q = j.getQualifier();
			// if they are not equal, we need to replace q with s
			if(!s.equals(q)) {
				joinMap.put(q, s);
			}
		}
		return joinMap;
	}
}
