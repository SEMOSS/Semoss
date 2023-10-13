package prerna.reactor.export;

import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CollectPivotReactor extends AbstractReactor {
	
	public CollectPivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey(), ReactorKeysEnum.FRAME_TYPE.getKey() };
	}
	
	public NounMetadata execute() {
		// default this to use Python
		// if Python not present
		// try in R
		// default is R
		String frameType = "R";
		if(store.getNoun(keysToGet[3]) != null) {
			frameType = keyValue.get(keysToGet[3]);
		}
		
		TaskBuilderReactor reactor = null;
		//frameType.equalsIgnoreCase("Py") && 
		if(PyUtils.pyEnabled()) {
			reactor = new prerna.reactor.frame.py.CollectPivotReactor();
		} else {
			reactor = new prerna.reactor.frame.r.CollectPivotReactor();
		}
		
		// pass the references/values
		// return the execution result
		reactor.In();
		reactor.setInsight(this.insight);
		reactor.setNounStore(this.store);
		return reactor.execute();
	}
}
