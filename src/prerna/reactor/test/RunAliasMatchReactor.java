package prerna.reactor.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunAliasMatchReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(RunAliasMatchReactor.class);

	private String aliasHeader = "Alias_1";
	private String hashCodeHeader = "Hashcode";
	
	@Override
	public NounMetadata execute() {
		Iterator<IHeadersDataRow> inputIterator = null;
		try {
			inputIterator = getInputIterator();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		Iterator<IHeadersDataRow> proposalIterator = null;
		try {
			proposalIterator = getProposalIterator();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		//need to check if all aliases and all hashcodes are the same
		Map<String, String> inputHash = new HashMap<>();
		Map<String, String> proposalHash = new HashMap<>();

		if (inputIterator != null) {
			while(inputIterator.hasNext()) {
				IHeadersDataRow nextData = inputIterator.next();
				String[] headers = nextData.getHeaders();
				Object[] values = nextData.getValues();
				int aliasIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
				int hashIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, hashCodeHeader);
				inputHash.put(values[aliasIndex].toString(), values[hashIndex].toString());
			}
		}

		if (proposalIterator != null) {
			while(proposalIterator.hasNext()) {
				IHeadersDataRow nextData = proposalIterator.next();
				String[] headers = nextData.getHeaders();
				Object[] values = nextData.getValues();
				int aliasIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, aliasHeader);
				int hashIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(headers, hashCodeHeader);
				proposalHash.put(values[aliasIndex].toString(), values[hashIndex].toString());
			}
		}
		
		int count = 0;
		for(String proposalKey : proposalHash.keySet()) {
			if(inputHash.containsKey(proposalKey)) {
				String proposalHashValue = proposalHash.get(proposalKey);
				String inputHashValue = inputHash.get(proposalKey);
				if(!proposalHashValue.equals(inputHashValue)) {
					count++;
					logger.info(Utility.cleanLogString(proposalKey));
					logger.info("input: "+inputHashValue);
					logger.info("proposal: "+proposalHashValue);
					logger.info("________________________");
				}
			}
		}
		
		logger.info("TOTAL NOT MATCHING: "+count);
		return null;
	}
	
	private Iterator<IHeadersDataRow> getInputIterator() throws Exception {
		GenRowStruct allNouns = getNounStore().getNoun("INPUT");
		Iterator<IHeadersDataRow> iterator = null;

		if(allNouns != null) {
			BasicIteratorTask task = (BasicIteratorTask)allNouns.get(0);
			iterator = task.getIterator();
		}
		return iterator;
	}
	
	private Iterator<IHeadersDataRow> getProposalIterator() throws Exception {
		GenRowStruct allNouns = getNounStore().getNoun("PROPOSALS");
		Iterator<IHeadersDataRow> iterator = null;

		if(allNouns != null) {
			BasicIteratorTask task = (BasicIteratorTask)allNouns.get(0);
			iterator = task.getIterator();
		}
		return iterator;
	}
}
