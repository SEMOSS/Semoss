package prerna.sablecc2.translations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.reactor.IReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AbstractDatasourceModificationTranslation extends LazyTranslation {

	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public HashMap<String, String> encodedToOriginal = new HashMap<String, String>();
	public List<String> encodingList = new Vector<String>();

	protected static Set<String> importTypes = new HashSet<String>();

	static {
		importTypes.add("Database");
		importTypes.add("FileRead");
		importTypes.add("GoogleSheetSource");
	}
	
	public AbstractDatasourceModificationTranslation(Insight insight) {
		super(insight);
	}
	
	/**
	 * Same method as in lazy with addition of addRoutine method
	 */
	@Override
    protected void deInitReactor() {
    	if(curReactor != null) {
    		// merge up and update the plan
    		try {
    			curReactor.mergeUp();
    			curReactor.updatePlan();
    		} catch(Exception e) {
    			e.printStackTrace();
    			throw new IllegalArgumentException(e.getMessage());
    		}
    		
    		// get the parent
    		Object parent = curReactor.Out();
    		// set the parent as the curReactor if it is present
    		prevReactor = curReactor;
    		if(parent instanceof IReactor) {
    			curReactor = (IReactor) parent;
    		} else {
    			curReactor = null;
    		}

    		// account for moving qs
    		if(curReactor == null && prevReactor instanceof AbstractQueryStructReactor) {
    			AbstractQueryStruct qs = ((AbstractQueryStructReactor) prevReactor).getQs();
	    		this.planner.addVariable(this.resultKey, new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
    		}
    		
    	}
    }
}
