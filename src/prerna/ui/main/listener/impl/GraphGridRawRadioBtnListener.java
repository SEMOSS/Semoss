package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JRadioButton;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GraphGridRawRadioBtnListener implements IChakraListener {
	
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		JRadioButton rdBtnGraph = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.RADIO_GRAPH);
		JRadioButton rdBtnGrid = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.RADIO_GRID);
		JRadioButton rdBtnRaw = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.RADIO_RAW);
		
		if (actionevent.getSource().equals(rdBtnGraph)&& rdBtnGraph.isSelected())
		{
			rdBtnGrid.setSelected(!rdBtnGraph.isSelected());
			rdBtnRaw.setSelected(!rdBtnGraph.isSelected());
		}
		else if (actionevent.getSource().equals(rdBtnGraph)&& !rdBtnGraph.isSelected())
		{
			rdBtnGraph.setSelected(true);
		}
		else if (actionevent.getSource().equals(rdBtnGrid)&& rdBtnGrid.isSelected())
		{
			rdBtnGraph.setSelected(!rdBtnGrid.isSelected());
			rdBtnRaw.setSelected(!rdBtnGrid.isSelected());
		}
		else if (actionevent.getSource().equals(rdBtnGrid)&& !rdBtnGrid.isSelected())
		{
			rdBtnGrid.setSelected(true);
		}
		else if (actionevent.getSource().equals(rdBtnRaw)&& rdBtnRaw.isSelected())
		{
			rdBtnGraph.setSelected(!rdBtnRaw.isSelected());
			rdBtnGrid.setSelected(!rdBtnRaw.isSelected());
		}
		else if (actionevent.getSource().equals(rdBtnRaw)&& !rdBtnRaw.isSelected())
		{
			rdBtnRaw.setSelected(true);
		}
		
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}
}