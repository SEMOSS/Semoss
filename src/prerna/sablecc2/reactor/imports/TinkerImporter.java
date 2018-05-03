package prerna.sablecc2.reactor.imports;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class TinkerImporter implements IImporter {

	private TinkerFrame dataframe;
	private SelectQueryStruct qs;
	private Iterator<IHeadersDataRow> it;
	
	public TinkerImporter(TinkerFrame dataframe, SelectQueryStruct qs) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
	}
	
	public TinkerImporter(TinkerFrame dataframe, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = it;
	}
	
	@Override
	public void insertData() {
		// get the edge hash so we know how to add data connections
		Map<String, Set<String>> edgeHash = ImportUtility.getEdgeHash(this.qs);
		// create the metadata
		ImportUtility.parseQueryStructAsGraph(this.dataframe, this.qs, edgeHash);
		// add the data
		processImport(edgeHash, null);
	}
	
	@Override
	public void insertData(OwlTemporalEngineMeta metaData) {
		this.dataframe.setMetaData(metaData);
		// get the edge hash so we know how to add data connections
		Map<String, Set<String>> edgeHash = ImportUtility.getEdgeHash(this.qs);
		// add the data
		processImport(edgeHash, null);
	}
	
	/**
	 * Flush out the iterator into the tinker frame using the specified edge relationships
	 * @param edgeHash
	 */
	private void processImport(Map<String, Set<String>> edgeHash, Map<String, String> headerAlias) {
		Map<Integer, Set<Integer>> cardinality = null;
		String[] headers = null;
		while(this.it.hasNext()) {
			IHeadersDataRow row = it.next();
			if(cardinality == null) {
				headers = row.getHeaders();
				// update the headers with the join info
				// so we create the vertices correctly
				if(headerAlias != null && !headerAlias.isEmpty()) {
					for(int i = 0; i < headers.length; i++) {
						if(headerAlias.containsKey(headers[i])) {
							headers[i] = headerAlias.get(headers[i]);
							continue;
						}
					}
				}
				// get the cardinality with the new headers since the edge hash is also modified
				cardinality = Utility.getCardinalityOfValues(headers, edgeHash);
			}
			dataframe.addRelationship(headers, row.getValues(), cardinality);
		}
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		List<String[]> existingRels = this.dataframe.getMetaData().getAllRelationships();
		// get the edge hash so we know how to add data connections
		// this edge hash will be used as part of the cardinality
		Map<String, String> joinMods = qsJoinMod(joins);
		if(!joinMods.isEmpty()) {
			modifyQsSelectorAlias(joinMods);
		}
		
		// get the edge hash from the qs
		Map<String, Set<String>> edgeHash = ImportUtility.getEdgeHash(this.qs);
		// determine if there are loops
		List<String[]> loopRels = getLoopRels(edgeHash, existingRels);
		if(loopRels.isEmpty()) {
			return processMerge(edgeHash, joinMods);
		} else {
			return processLoop(loopRels, joinMods, joins);
		}
	}
	
	/**
	 * This is the default method to merge data into the frame
	 * @param edgeHash 
	 * @param joinMods 
	 * @return
	 */
	private ITableDataFrame processMerge(Map<String, Set<String>> edgeHash, Map<String, String> joinMods) {
		// create the metadata
		// note this has the updated qs based on the join mods already
		ImportUtility.parseQueryStructAsGraph(this.dataframe, this.qs, edgeHash);
		processImport(edgeHash, joinMods);
		return this.dataframe;
	}
	
	private ITableDataFrame processLoop(List<String[]> loopRels, Map<String, String> joinMods, List<Join> joins) {
		Map<String, Set<String>> originalEdgeHash = ImportUtility.getEdgeHash(this.qs);
		// so we have a -> b -> a
		// but i need the last a to be a_1
		// so my loop node is going the be the second index the the loopRels
		// all i need to do is go through, and assign the alias everywhere, and then i'm golden
		
		Set<String> joinCols = new HashSet<String>();
		for(Join j : joins) {
			joinCols.add(j.getQualifier());
		}
		// update qs selectors with new alias
		Map<String, String> oldAliasToNew = new HashMap<String, String>();
		for(IQuerySelector selector : this.qs.getSelectors()) {
			String curAlias = selector.getAlias();
			// we do not want to do this for the join column!
			if(joinCols.contains(curAlias)) {
				continue;
			}
			for(String[] loop : loopRels) {
				if(loop[0].equals(curAlias)) {
					String newAlias = curAlias + "_2";
					selector.setAlias(newAlias);
					oldAliasToNew.put(curAlias, newAlias);
				} else if(loop[1].equals(curAlias)) {
					String newAlias = curAlias + "_2";
					selector.setAlias(newAlias);
					oldAliasToNew.put(curAlias, newAlias);
				}
			}
		}
		
		Map<String, Set<String>> updatedEdgeHash = ImportUtility.getEdgeHash(this.qs);
		// we need to define something to say
		// that we are actually adding these with a different type
		// remember: on tinker, we want to reuse the same node
		ImportUtility.parseQueryStructAsGraph(this.dataframe, this.qs, updatedEdgeHash);
		OwlTemporalEngineMeta meta = this.dataframe.getMetaData();
		for(String oldAlias : oldAliasToNew.keySet()) {
			meta.setPhysicalNameToVertex(oldAliasToNew.get(oldAlias), oldAlias);
		}
		
		// note, we use the original edge hash since the headers from the iterator
		// do not know that we have modified the meta
		processImport(originalEdgeHash, joinMods, oldAliasToNew);
		return this.dataframe;
	}
	
	/**
	 * Flush out the iterator into the tinker frame using the specified edge relationships
	 * @param edgeHash
	 */
	private void processImport(Map<String, Set<String>> edgeHash, Map<String, String> headerAlias, Map<String, String> oldAliasToNew) {
		Map<Integer, Set<Integer>> cardinality = null;
		String[] headers = null;
		while(this.it.hasNext()) {
			IHeadersDataRow row = it.next();
			if(cardinality == null) {
				headers = row.getHeaders();
				// update the headers with the join info
				// so we create the vertices correctly
				if(headerAlias != null && !headerAlias.isEmpty()) {
					for(int i = 0; i < headers.length; i++) {
						if(headerAlias.containsKey(headers[i])) {
							headers[i] = headerAlias.get(headers[i]);
							continue;
						}
					}
				}
				// get the cardinality with the new headers since the edge hash is also modified
				cardinality = Utility.getCardinalityOfValues(headers, edgeHash);
			}
			dataframe.addRelationship(headers, row.getValues(), cardinality, oldAliasToNew);
		}
	}
	
	
	//////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////

	/*
	 * Utility methods
	 */
	
	private List<String[]> getLoopRels(Map<String, Set<String>> edgeHash, List<String[]> existingRels) {
		// we are only searching for a simple loop
		// i.e. a -> b -> a
		List<String[]> loopRels = new Vector<String[]>();
		for(String[] relArray : existingRels) {
			// so we just need to do a comparison
			// if we already have a -> b
			// is there b -> a in the edge hash
			String upNode = relArray[0];
			String downNode = relArray[1];
			
			// if the edge hash doesn't have downNode as a key
			// just continue
			if(edgeHash.containsKey(downNode)) {
				// we found it, lets go and see if it goes back to the up node
				if(edgeHash.get(downNode).contains(upNode)) {
					// we have a loop!
					loopRels.add(new String[]{downNode, upNode});
				}
			}
		}
		return loopRels;
	}
	
	private void modifyQsSelectorAlias(Map<String, String> joinMods) {
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
