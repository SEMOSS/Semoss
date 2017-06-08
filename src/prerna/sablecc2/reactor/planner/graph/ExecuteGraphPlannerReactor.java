package prerna.sablecc2.reactor.planner.graph;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.GreedyTranslation;
import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.PKSLPlanner;

public class ExecuteGraphPlannerReactor extends AbstractPlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadGraphClient.class.getName());

	@Override
	public NounMetadata execute()
	{
		long start = System.currentTimeMillis();
		
		PKSLPlanner planner = getPlanner();
		List<String> pksls = new Vector<String>();

		// get the list of the root vertices
		// these are the vertices we can run right away
		// and are the starting point for the plan execution
		Set<Vertex> rootVertices = getRootPksls(planner);
		// using the root vertices
		// iterate down all the other vertices and add the signatures
		// for the desired travels in the appropriate order
		// note: this is adding to the list of undefined variables
		// calculated at beginning of class 
		traverseDownstreamVertsAndOrderProcessing(planner, rootVertices, pksls);
		
		GreedyTranslation translation = new GreedyTranslation();
		translation.planner = planner;
		PkslUtility.addPkslToTranslation(translation, pksls);
		
		long end = System.currentTimeMillis();
		LOGGER.info("****************    END RUN PLANNER "+(end - start)+"ms      *************************");
		
		return new NounMetadata(translation.planner, PkslDataTypes.PLANNER);
	}
	
	private PKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		PKSLPlanner planner = null;
		if(allNouns != null) {
			planner = (PKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}
}
