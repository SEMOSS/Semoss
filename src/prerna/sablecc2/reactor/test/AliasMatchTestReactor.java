package prerna.sablecc2.reactor.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ArrayUtilityMethods;

public class AliasMatchTestReactor extends AbstractReactor {

	private String aliasHeader = "Alias_1";
	private String hashCodeHeader = "Hashcode";
	
	@Override
	public NounMetadata execute() {
		Iterator<IHeadersDataRow> inputIterator = getInputIterator();
		Iterator<IHeadersDataRow> proposalIterator = getProposalIterator();
		
		//need to check if all aliases and all hashcodes are the same
		Map<String, String> inputHash = new HashMap<>();
		Map<String, String> proposalHash = new HashMap<>();
		
		while(inputIterator.hasNext()) {
			IHeadersDataRow nextData = inputIterator.next();
			String[] headers = nextData.getHeaders();
			Object[] values = nextData.getValues();
			int aliasIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
			int hashIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, hashCodeHeader);
			inputHash.put(values[aliasIndex].toString(), values[hashIndex].toString());
		}
		
		while(proposalIterator.hasNext()) {
			IHeadersDataRow nextData = proposalIterator.next();
			String[] headers = nextData.getHeaders();
			Object[] values = nextData.getValues();
			int aliasIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
			int hashIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, hashCodeHeader);
			proposalHash.put(values[aliasIndex].toString(), values[hashIndex].toString());
		}
		
		int count = 0;
		for(String proposalKey : proposalHash.keySet()) {
			if(inputHash.containsKey(proposalKey)) {
				String proposalHashValue = proposalHash.get(proposalKey);
				String inputHashValue = inputHash.get(proposalKey);
				if(!proposalHashValue.equals(inputHashValue)) {
					count++;
					System.out.println(proposalKey);
					System.out.println("input: "+inputHashValue);
					System.out.println("proposal: "+proposalHashValue);
					System.out.println("________________________");
					System.out.println();
				}
			}
		}
		
		System.out.println("TOTAL NOT MATCHING: "+count);
		return null;
	}
	
	private Iterator<IHeadersDataRow> getInputIterator() {
		GenRowStruct allNouns = getNounStore().getNoun("INPUT");
		Iterator<IHeadersDataRow> iterator = null;

		if(allNouns != null) {
			Job job = (Job)allNouns.get(0);
			iterator = job.getIterator();
		}
		return iterator;
	}
	
	private Iterator<IHeadersDataRow> getProposalIterator() {
		GenRowStruct allNouns = getNounStore().getNoun("PROPOSALS");
		Iterator<IHeadersDataRow> iterator = null;

		if(allNouns != null) {
			Job job = (Job)allNouns.get(0);
			iterator = job.getIterator();
		}
		return iterator;
	}
}
