package prerna.sablecc2.reactor.planner.table;

import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.Translation;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.TablePKSLPlanner;
import prerna.sablecc2.reactor.planner.graph.LoadGraphClient;

public class ExecuteTablePlannerReactor extends AbstractTablePlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadGraphClient.class.getName());

	@Override
	public NounMetadata execute()
	{
		long start = System.currentTimeMillis();
		
		TablePKSLPlanner planner = getPlanner();
		
		List<String> pksls = collectRootPksls(planner);		
		Translation translation = new Translation();
		translation.planner = planner;
		while(!pksls.isEmpty()) {
			try {
				PkslUtility.addPkslToTranslation(translation, pksls);
			} catch(Exception e) {
				e.printStackTrace();
			}
//			updateTable(planner, pksls);
			pksls = collectNextPksls(planner);
		}
		resetTable(planner);
		
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
