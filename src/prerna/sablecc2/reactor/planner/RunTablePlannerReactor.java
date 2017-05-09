package prerna.sablecc2.reactor.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.Translation;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.TablePKSLPlanner;

public class RunTablePlannerReactor extends AbstractTablePlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	@Override
	public NounMetadata execute()
	{
		long start = System.currentTimeMillis();
		
		TablePKSLPlanner planner = getPlanner();
		List<String> pksls = collectPksls(planner);		
		Translation translation = new Translation();
		translation.planner = planner;
		try {
			PkslUtility.addPkslToTranslation(translation, new ArrayList<String>(pksls));
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("****************    END RUN PLANNER "+(end - start)+"ms      *************************");
		
		return new NounMetadata(translation.planner, PkslDataTypes.PLANNER);
	}
	
	private TablePKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		TablePKSLPlanner planner = null;
		if(allNouns != null) {
			planner = (TablePKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return (TablePKSLPlanner)this.planner;
		}
	}
}
