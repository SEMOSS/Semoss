package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.ui.comparison.specific.tap.GenericDBComparisonWriter;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class DatabaseComparisonListener implements IChakraListener
{
	
	@SuppressWarnings("unchecked")
	@Override
	public void actionPerformed(ActionEvent arg0)
	{
		// get selected values
		JComboBox<String> newDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.NEW_DB_COMBOBOX);
		String newDBName = newDBComboBox.getSelectedItem() + "";
		
		JComboBox<String> oldDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.OLD_DB_COMBOBOX);
		String oldDBName = oldDBComboBox.getSelectedItem() + "";
		
		// get associated engines
		IEngine newDB = (IEngine) DIHelper.getInstance().getLocalProp(newDBName);
		IEngine oldDB = (IEngine) DIHelper.getInstance().getLocalProp(oldDBName);
		
		RDFFileSesameEngine newMetaDB = ((AbstractEngine) newDB).getBaseDataEngine();
		RDFFileSesameEngine oldMetaDB = ((AbstractEngine) oldDB).getBaseDataEngine();
		
		try
		{
			GenericDBComparisonWriter comparisonWriter = new GenericDBComparisonWriter(newDB, oldDB, newMetaDB, oldMetaDB);
			comparisonWriter.runAllInstanceTests();
			comparisonWriter.runAllMetaTests();
			comparisonWriter.writeWB();
		} catch (EngineException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void setView(JComponent view)
	{
		// TODO Auto-generated method stub
		
	}
	
}
