package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.reactor.export.FormatReactor;
import prerna.sablecc2.reactor.imports.ImportDataReactor;
import prerna.sablecc2.reactor.imports.MergeDataReactor;
import prerna.sablecc2.reactor.qs.AverageReactor;
import prerna.sablecc2.reactor.qs.DatabaseReactor;
import prerna.sablecc2.reactor.qs.FrameReactor;
import prerna.sablecc2.reactor.qs.GroupByReactor;
import prerna.sablecc2.reactor.qs.JoinReactor;
import prerna.sablecc2.reactor.qs.LimitReactor;
import prerna.sablecc2.reactor.qs.OffsetReactor;
import prerna.sablecc2.reactor.qs.QueryFilterReactor;
//import prerna.sablecc2.reactor.qs.QueryReactor;
import prerna.sablecc2.reactor.qs.SelectReactor;
import prerna.sablecc2.reactor.qs.SumReactor;
//import prerna.util.Utility;

public class ReactorFactory {

	//This holds the reactors that are frame agnostic and can be used by pixel
	private static Map<String, Class> reactorHash;
	
	//This holds the list of expressions we have that can be used as operational formulas and reducers
	//Ex: Sum(), Max(), Min(), etc. 
	private static Set<String> expressions;
	static {
		reactorHash = new HashMap<>();
		createReactorHash(reactorHash);
		
		expressions = new HashSet<>();
		populateExpressionSet(expressions);
	}
	
	//populates the types of expressions we have available (list taken from h2frame)
	private static void populateExpressionSet(Set<String> expressions) {
		expressions.add(PKQLEnum.SUM.toString());
		expressions.add(PKQLEnum.MAX.toString());
		expressions.add(PKQLEnum.MIN.toString());
		expressions.add(PKQLEnum.AVERAGE.toString());
		expressions.add(PKQLEnum.COUNT.toString());
		expressions.add(PKQLEnum.COUNT_DISTINCT.toString());
		expressions.add(PKQLEnum.CONCAT.toString());
		expressions.add(PKQLEnum.GROUP_CONCAT.toString());
		expressions.add(PKQLEnum.UNIQUE_GROUP_CONCAT.toString());
		expressions.add(PKQLEnum.ABSOLUTE.toString());
		expressions.add(PKQLEnum.ROUND.toString());
		expressions.add(PKQLEnum.COS.toString());
		expressions.add(PKQLEnum.SIN.toString());
		expressions.add(PKQLEnum.TAN.toString());
		expressions.add(PKQLEnum.CEILING.toString());
		expressions.add(PKQLEnum.FLOOR.toString());
		expressions.add(PKQLEnum.LOG.toString());
		expressions.add(PKQLEnum.LOG10.toString());
		expressions.add(PKQLEnum.SQRT.toString());
		expressions.add(PKQLEnum.POWER.toString());
		expressions.add(PKQLEnum.CORRELATION_ALGORITHM.toString());
	}
	
	//populates the frame agnostic reactors used by pixel
	private static void createReactorHash(Map<String, Class> reactorHash) {
		
		//Import Reactors
		reactorHash.put("Import", ImportDataReactor.class); //takes in a query struct and imports data to a new frame
		reactorHash.put("Merge", MergeDataReactor.class); //takes in a query struct and merges data to an existing frame
		
		//Query Struct Reactors
		reactorHash.put("Select", SelectReactor.class); //builds the select portion of the QS
		reactorHash.put("Average", AverageReactor.class);
		reactorHash.put("Sum", SumReactor.class);
		
		reactorHash.put("Group", GroupByReactor.class);
		reactorHash.put("Limit", LimitReactor.class);
		reactorHash.put("Offset", OffsetReactor.class);
		reactorHash.put("Join", JoinReactor.class);
		reactorHash.put("Filter", QueryFilterReactor.class);
//		reactorHash.put("Query", QueryReactor.class); //takes in a direct query
		
		//Data Source Reactors
		reactorHash.put("Database", DatabaseReactor.class); //specifies that our pksl operations after this point are dealing with the specified database
		reactorHash.put("Frame", FrameReactor.class); //specifes that our pksl operations after this point are dealing with the specified frame
		reactorHash.put("CreateFrame", CreateFrame.class);
		
		//Reducers
		reactorHash.put("Iterate", IterateReactor.class); //this takes in a query struct and produces an iterator
		
		//Exporting Reactors
		reactorHash.put("Job", JobReactor.class);
		reactorHash.put("Format", FormatReactor.class);
		reactorHash.put("Export", ExportReactor.class);
		reactorHash.put("Collect", CollectReactor.class);
		
		//
		reactorHash.put("if", IfReactor.class);
		
	}
	
	/**
	 * 
	 * @param reactorId - reactor name
	 * @param nodeString - pixel
	 * @param frame - frame we will be operating on
	 * @param parentReactor - the parent reactor
	 * @return
	 * 
	 * This will simply return the IReactor responsible for execution based on the reactorId
	 * 
	 * Special case:
	 * 		if we are dealing with an expression, we determine if this expression is part of a select query or should be reduced
	 * 		If it is a reducing expression we
	 * 			1. create an expr reactor
	 * 			2. grab the reducing expression reactor from the frame
	 * 			3. set that reactor to the expr reactor and return the expr reactor
	 * 		The expr reactor when executed will use that reducing expression reactor to evaluate
	 */
    public static IReactor getReactor(String reactorId, String nodeString, ITableDataFrame frame, IReactor parentReactor) {
    	
    	IReactor reactor;
		try {
			
			//is this an expression?
			//we need to determine if we are treating this expression as a reducer or as a selector
			if(expressions.contains(reactorId.toUpperCase())) {
				
				//if this expression is not a selector
				if(!(parentReactor instanceof SelectReactor)) {
							
					//this expression is a reducer not a selector
					
					//so first we create an ExprReactor
					reactor = new ExprReactor();
					reactor.setPKSL(reactorId, nodeString);
			        reactor.setName("OPERATION_FORMULA");
			        
			        //grab the expression reducer from the frame
			        Map<String, String> scriptReactors = new H2Frame().getScriptReactors();
			        String reactorName = scriptReactors.get(reactorId.toUpperCase());
			        
//			        if(reactorName == null)
//			        	reactorName = Utility.toCamelCase(reactorId);
			        //set the name of that reducer in the reactor so it can reduce properly
			        reactor.setProp("REACTOR_NAME", reactorName);
			        return reactor;
				}
			}
			reactor = (IReactor)reactorHash.get(reactorId).newInstance();
			reactor.setPKSL(reactorId, nodeString);
	    	return reactor;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
    	
		return new SampleReactor();
    }
}
