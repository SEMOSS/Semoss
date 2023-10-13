package prerna.reactor.planner.graph;
//package prerna.sablecc2.reactor.planner.graph;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Set;
//import java.util.Vector;
//
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//
//import prerna.sablecc2.GreedyTranslation;
//import prerna.sablecc2.LazyTranslation;
//import prerna.sablecc2.PkslUtility;
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.NounMetadata;
//import prerna.sablecc2.om.PkslDataTypes;
//import prerna.sablecc2.reactor.PKSLPlanner;
//
//public class UpdateGraphPlannerReactor extends AbstractPlannerReactor {
//
//	public static final String PKSL_NOUN = "pksls";
//	public static final String STORE_NOUN = "store";
//	
//	@Override
//	public NounMetadata execute()
//	{
////		long start = System.currentTimeMillis();
//		
//		// grab all the pksls
//		GenRowStruct pksls = this.store.getNoun(PKSL_NOUN);
//		
//		// store them in a list
//		// and also keep a builder with all the executions
//		List<String> pkslsToAdd = new Vector<String>();
//		int numPksls = pksls.size();
//		for(int i = 0; i < numPksls; i++) {
////			pkslsToAdd.add(pksls.get(i).toString());
//		}
//		
//		List<PKSLPlanner> myPlanners = getPlanners();
//		
//		for(PKSLPlanner myPlanner : myPlanners) {
//			// bs
//			long start = System.currentTimeMillis();
//			addOrderToNonExistentVerts(myPlanner);
//			long end = System.currentTimeMillis();
//			System.out.println("Add order = " + (end - start));
//			
//			// to properly update
//			// we need to reset the "PROCESSED" property
//			// that are currently set on the planner
//			// when we first executed the plan
//			start = System.currentTimeMillis();
//			resetProcessedBoolean(myPlanner);
//			end = System.currentTimeMillis();
//			System.out.println("Reset boolean = " + (end - start));
//			
//			// know we execute these on a new planner
//			// and then we will figure out the roots of these new values
//			start = System.currentTimeMillis();
//			LazyTranslation plannerT = new LazyTranslation();
//			PkslUtility.addPkslToTranslation(plannerT, pkslsToAdd);
//			end = System.currentTimeMillis();
//			System.out.println("Add initial values = " + (end - start));
//			
//			// using this planner
//			// get the roots
//			start = System.currentTimeMillis();
//			Set<Vertex> roots = getRootPksls(plannerT.planner);
//			end = System.currentTimeMillis();
//			System.out.println("Get roots = " + (end - start));
//			
//			// now we want to get all the output nouns of these roots
//			// and go downstream to all the ops within the original planner 
//			// that we are updating
//			start = System.currentTimeMillis();
//			Set<Vertex> newRoots = getDownstreamEffectsInPlanner(roots, myPlanner);
//			end = System.currentTimeMillis();
//			System.out.println("Find nouns of roots = " + (end - start));
//			
//			List<String> downstreamVertIds = new Vector<String>();
//			// traverse downstream and get all the other values we need to update
//			start = System.currentTimeMillis();
//			getAllDownstreamVertsBasedOnTraverseOrder(newRoots, downstreamVertIds);
//			end = System.currentTimeMillis();
//			System.out.println("Find downstream of new roots = " + (end - start));
//			
//			// since we have the order based on the first execution
//			// use that in order to add these pksls in the correct order
//			// for the execution
//			start = System.currentTimeMillis();
//			List<String> pkslsToRun = new ArrayList<>();
//			pkslsToRun.addAll(pkslsToAdd);
//			pkslsToRun.addAll(orderVertsAndGetPksls(myPlanner, downstreamVertIds));
//			end = System.currentTimeMillis();
//			System.out.println("Order verts " + (end - start));
//			
//			// now run through all the pksls and execute
//			// we will use the same planner which has all the assignments
//			// set and have those values automatically updated
//			start = System.currentTimeMillis();
//			GreedyTranslation translation = new GreedyTranslation();
//			translation.planner = myPlanner;
//			System.out.println("Need to run thorugh " + pkslsToRun.size() + " pksls!!!");
//			PkslUtility.addPkslToTranslation(translation, pkslsToRun);
//			end = System.currentTimeMillis();
//			System.out.println("Run through pksls " + (end - start));
//		}
//		
////		long end = System.currentTimeMillis();
////		System.out.println("****************    END UPDATE "+(end - start)+"ms      *************************");
//		
//		return new NounMetadata(myPlanners, PkslDataTypes.PLANNER);
//	}
//	
//	private List<PKSLPlanner> getPlanners() {
//		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
//		List<PKSLPlanner> planners = new ArrayList<>(allNouns.size());
//		if(allNouns != null) {
//			
//			for(int i = 0; i < allNouns.size(); i++) {
//				Object nextNoun = allNouns.get(i);
//				if(nextNoun instanceof List) {
//					 List nounList = (List)nextNoun;
//					 for(Object n : nounList) {
//						 planners.add((PKSLPlanner)n);
//					 }
//				} else {
//					planners.add((PKSLPlanner)nextNoun);
//				}
//			}
//		}
//		
//		return planners;
//	}
//}
