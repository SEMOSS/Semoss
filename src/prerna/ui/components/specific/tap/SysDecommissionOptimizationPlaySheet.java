package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;

public class SysDecommissionOptimizationPlaySheet extends GridPlaySheet{

	private ArrayList<String> listToCompare = new ArrayList<String>();
	
	@Override
	public void createData() {
		SysDecommissionOptimizationFunctions optFunctions = new SysDecommissionOptimizationFunctions();
		optFunctions.optimizeTime();
		
		names = new String[6];
		names[0] = "System";
		names[1] ="Min Time in Years at One Site";
		names[2] ="Work Volume in Years at One Site";
		names[3] ="Number of Sites Deployed At";
		names[4] ="Resource Allocation";
		names[5] ="Number of Systems Transformed Simultaneously";
		
		list = optFunctions.outputList;
		
		
		
		
	}


}
