package prerna.ui.components.playsheets;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.unsupervised.som.SelfOrganizingMap;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.util.Utility;

public class SelfOrganizingMapPlaySheet extends GridPlaySheet{
	
	private static final Logger LOGGER = LogManager.getLogger(SelfOrganizingMapPlaySheet.class.getName());

	private SelfOrganizingMap alg;

	public SelfOrganizingMapPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		generateData();
		runAlgorithm();
	}

	private void generateData() {
		if(query!=null) {
			list = new ArrayList<Object[]>();

			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			names = sjsw.getVariables();
			int length = names.length;
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				Object[] row = new Object[length];
				int i = 0;
				for(; i < length; i++) {
					row[i] = sjss.getVar(names[i]);
				}
				list.add(row);
			}
		}
	}

	public void runAlgorithm() {
		LOGGER.info("Creating apriori algorithm for instance: " + names[0]);
		alg = new SelfOrganizingMap(list, names);
		boolean success = alg.execute();
		if(success == false) {
			Utility.showError("Error occured running SOM Algorithm!");
		}
	}


}
