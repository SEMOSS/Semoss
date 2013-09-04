package prerna.ui.components;

import javax.swing.JList;
import javax.swing.JMenuItem;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.helpers.PlaysheetExtendRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

// abstract base class for performing all of the work around
// getting the specific queries and painting it etc. 

public class PropertyMenuItem extends JMenuItem{
	String query; 
	IEngine engine = null;
	String predicateURI = null;
	String name = null;
	
	Logger logger = Logger.getLogger(getClass());

	public PropertyMenuItem(String name, String query, IEngine engine)
	{
		super(name);
		this.name = name;
		this.query = query;
		this.engine = engine;
	}
	
	public void paintNeighborhood()
	{
		// compose the query and paint the neighborhood
		IPlaySheet playSheet = QuestionPlaySheetStore.getInstance().getActiveSheet();
		logger.debug("Extending ");
		Runnable playRunner = null;
		// Here I need to get the active sheet
		// get everything with respect the selected node type
		// and then create the filter on top of the query
		// use the @filter@ to get this done / some of the 			
		playSheet = QuestionPlaySheetStore.getInstance().getActiveSheet();

		// need to create playsheet extend runner
		playRunner = new PlaysheetExtendRunner(playSheet);
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object [] repos = (Object [])list.getSelectedValues();

		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
			playSheet.setRDFEngine(engine);
			playSheet.setQuery(query);
		
		
			// thread
			Thread playThread = new Thread(playRunner);
			playThread.start();
		}
		
	}
	
	
}
