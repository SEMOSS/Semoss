package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Set;

import javax.swing.JButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.om.GraphDataModel;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.util.StatementCollector;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.main.listener.impl.ColumnChartGroupedStackedListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ColumnChartPlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(ColumnChartPlaySheet.class.getName());
	GraphDataModel gdm = new GraphDataModel();
	
	public ColumnChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/columnchart.html";
	}
	
	@Override
	public void createControlPanel(){
		controlPanel = new ChartControlPanel();
		controlPanel.addExportButton(1);
		
		ChartControlPanel ctrlPanel = this.getControlPanel();
		GridBagConstraints gbc_btnGroupedStacked = new GridBagConstraints();
		gbc_btnGroupedStacked.insets = new Insets(10, 0, 0, 5);
		gbc_btnGroupedStacked.anchor = GridBagConstraints.EAST;
		gbc_btnGroupedStacked.gridx = 0;
		gbc_btnGroupedStacked.gridy = 0;
		JButton transitionGroupedStacked = new JButton("Transition Grouped");
		ColumnChartGroupedStackedListener gsListener = new ColumnChartGroupedStackedListener();
		gsListener.setBrowser(this.browser);
		transitionGroupedStacked.addActionListener(gsListener);
		ctrlPanel.add(transitionGroupedStacked, gbc_btnGroupedStacked);

		this.controlPanel.setPlaySheet(this);
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList< ArrayList<Hashtable<String, Object>>>();
		//series name - all objects in that series (x : ... , y : ...)
		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			for( int j = 1; j < elemValues.length; j++)
			{
				ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
				if(dataObj.size() >= j)
					seriesArray = dataObj.get(j-1);
				else
					dataObj.add(j-1, seriesArray);
				Hashtable<String, Object> elementHash = new Hashtable();
				elementHash.put("x", elemValues[0].toString());
				elementHash.put("y", elemValues[j]);
				elementHash.put("seriesName", names[j]);
				seriesArray.add(elementHash);
			}
		}
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		Object var = sjss.getRawVar(varName);
			if( var != null && var instanceof Literal) {
				var = sjss.getVar(varName);
			} 
		return var;
	}
}
