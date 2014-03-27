package prerna.ui.components.specific.tap;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.ArrayList;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.ui.main.listener.impl.GridPlaySheetListener;

public class SysDecommissionOptimizationPlaySheet extends GridPlaySheet{

	public int resource;
	public double time;
	
	@Override
	public void createData() {
		SysDecommissionOptimizationFunctions optFunctions = new SysDecommissionOptimizationFunctions();
		
		names = new String[6];
		names[0] = "System";
		names[1] ="Time to Transform";
		names[2] = "Number of Sites Deployed At";
		names[3] = "Resource Allocation";
		names[4] = "Number of Systems Transformed Simultaneously";
		names[5] = "Total Cost for System";
//		names[5] = "Min time for system";
		
		if(query.equals("Constrain Resource"))
		{
			optFunctions.resourcesConstraint = resource;
			optFunctions.optimizeTime();
			list = optFunctions.outputList;
		}
		else
		{
			optFunctions.timeConstraint = time;
			optFunctions.optimizeResource();
			list = optFunctions.outputList;
		}

	}

	public void runPlaySheet(String typeConstraint,int resource, double time) {
		query = typeConstraint;
		this.resource = resource;
		this.time = time;
		createData();
		runAnalytics();
		createView();
	}

}
