package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SparqlShowListener implements IChakraListener {

	JTextArea view = null;
	
	
	public void setModel(JComponent model)
	{
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		JToggleButton spql = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.SPARQLBTN);
		JTextArea sparqlArea = (JTextArea)DIHelper.getInstance().getLocalProp(Constants.SPARQL_AREA_FIELD);
		JRadioButton rdBtnGraph = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.RADIO_GRAPH);
		JRadioButton rdBtnGrid = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.RADIO_GRID);
		JRadioButton rdBtnRaw = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.RADIO_RAW);
		if (spql.isSelected())
		{
			rdBtnGraph.setEnabled(true);
			rdBtnGrid.setEnabled(true);
			rdBtnRaw.setEnabled(true);
			sparqlArea.setEnabled(true);
		}
		else
		{
			rdBtnGraph.setEnabled(false);
			rdBtnGrid.setEnabled(false);
			rdBtnRaw.setEnabled(false);
			sparqlArea.setEnabled(false);
		}

	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		this.view = (JTextArea)view;
		
	}


}
