package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class InstanceMatcher extends ExactStringMatcher {

	private static final Logger LOGGER = LogManager.getLogger(InstanceMatcher.class.getName());
	
	protected int compare(TreeNode obj1, TreeNode obj2){
		if(obj1.leaf.isEqual(obj2.leaf)){
			return 0;
		}
		else if(obj1.leaf.isLeft(obj2.leaf)){ // this means node 2 is ahead
			return -1;
		}
		return 1; // else node 1 is ahead
	}

	@Override
	public String getName() {
		return "Instace Matcher";
	}

	@Override
	public String getResultDescription() {
		return "This routine matches based solely on the instance value. It constructs full uris based on the owl base uri value and adds as bindings to the query.";
	}

	@Override
	public MATCHER_ACTION getQueryModType() {
		LOGGER.info("Getting query mod type");
		return MATCHER_ACTION.BIND;
	}

	@Override
	public List<Object> getQueryModList(ITableDataFrame dm, String columnNameInDM, IEngine engine, String columnNameInNewEngine) {
		LOGGER.info("Beginning to get query mod list for instance matcher");
		
		// if column is a node in the engine, we must get the base uri and append
		// else it is a property and does not need a base uri
		LOGGER.info("getting base uri for engine " + engine.getEngineName());
		String baseUri = engine.getNodeBaseUri();
		String physURI = Utility.getTransformedNodeName(engine, Constants.DISPLAY_URI + columnNameInNewEngine, false);
		Boolean newColIsNode = engine.getConcepts().contains(physURI);
		LOGGER.info("New Col is node :::: " + newColIsNode + " and base uri is ::::: " + baseUri);
		List<Object> retList = new Vector<Object>();

		Iterator<Object> it = dm.uniqueValueIterator(columnNameInDM, false);
		while(it.hasNext()) {
			Object value = it.next();
			if(newColIsNode){
				value = baseUri + Utility.getInstanceName(physURI) +"/" + value;
			}
			retList.add(value);
		}
		
		return retList;
	}
}
