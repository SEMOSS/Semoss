package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.EAPropertyCreator;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class EAPropertyCreatorListener extends AbstractListener {
	static final Logger LOGGER = LogManager.getLogger(EAPropertyCreatorListener.class.getName());
	
	public final String hrCoreDBName = "HR_Core";
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		// int response = JOptionPane.showOptionDialog(playPane, "This move cannot be undone.\n\n" +
		// "Would you still like to continue?\n",
		// "Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
		//
		// JOptionPane.showMessageDialog(playPane, "The Cost DB Loading Sheet can be found here:\n\n" + file);
		//
		// if (response == 1)
		// {
		IEngine hrCoreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDBName);
		
		LOGGER.info("Adding EA properties to " + hrCoreDBName);
		
		EAPropertyCreator creator = new EAPropertyCreator(hrCoreDB);
		try {
			creator.addProperties();
			Utility.showMessage("EA properties have been added to HR_Core!");
		} catch (EngineException e) {
			Utility.showError("Error with generating new DB. Make sure HR_Core properly defined.");
			e.printStackTrace();
		} catch (RepositoryException e) {
			Utility.showError("Error with generating new DB");
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			Utility.showError("Error with generating new DB");
			e.printStackTrace();
		}
		// }
	}
	
	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}
	
}
