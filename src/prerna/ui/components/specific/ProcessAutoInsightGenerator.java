package prerna.ui.components.specific;

import java.util.Collections;
import java.util.List;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JList;

import prerna.poi.main.insights.AutoInsightExecutor;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ProcessAutoInsightGenerator implements Runnable{

	private AbstractEngine engine;

	public ProcessAutoInsightGenerator(AbstractEngine engine) {
		this.engine = engine;
	}
	
	public ProcessAutoInsightGenerator(String engineName) {
		this.engine = (AbstractEngine) DIHelper.getInstance().getLocalProp(engineName);
	}
	
	@Override
	public void run() {
		AutoInsightExecutor executor = new AutoInsightExecutor(engine);
		executor.run();
		if(executor.isSuccess()) {
			reloadDB(engine.getEngineName());
			Utility.showMessage("Done auto generating insights.");
		} else {
			Utility.showError("Error auto generating insights");
		}
	}
	
	private void reloadDB(String engineName) {
		// selects the db in repolist so the questions refresh with the changes
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		List selectedList = list.getSelectedValuesList();
		String selectedValue = selectedList.get(selectedList.size() - 1).toString();
		AbstractEngine engine = (AbstractEngine) DIHelper.getInstance().getLocalProp(engineName);
		// don't need to refresh if selected db is not the db you're modifying.
		// when you click to it it will refresh anyway.
		if (engine.getEngineName().equals(selectedValue)) {
			Vector<String> perspectives = engine.getPerspectives();
			Collections.sort(perspectives);

			JComboBox<String> box = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR);
			box.removeAllItems();
			for (int itemIndex = 0; itemIndex < perspectives.size(); itemIndex++) {
				box.addItem(perspectives.get(itemIndex).toString());
			}
		}
	}
	
}
