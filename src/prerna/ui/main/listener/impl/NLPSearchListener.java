package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JList;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.NLPSearchPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

public class NLPSearchListener extends AbstractListener{

	static final Logger LOGGER = LogManager.getLogger(NLPSearchListener.class.getName());

	@Override
	public void actionPerformed(ActionEvent arg0) {
		
		// get the selected repository, in case someone selects multiple, it'll always use first one
		JList<String> list = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		String engineName = (String)list.getSelectedValue();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		
		JTextField searchField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.SEARCH_DATABASE_TEXT);
		String query = searchField.getText();
		String playSheetTitle = "Searching for answer to... "+query;
		QuestionPlaySheetStore.getInstance().idCount++;
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+". "+playSheetTitle;
		
		NLPSearchPlaySheet playSheet = new NLPSearchPlaySheet();

		QuestionPlaySheetStore.getInstance().put(insightID,  playSheet);
		playSheet.setQuery(query);
		playSheet.setRDFEngine(engine);
		playSheet.setQuestionID(insightID);
		playSheet.setTitle(playSheetTitle);
	
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);
		Runnable playRunner = new PlaysheetCreateRunner(playSheet);
		
		Thread playThread = new Thread(playRunner);
		playThread.start();
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}

}
