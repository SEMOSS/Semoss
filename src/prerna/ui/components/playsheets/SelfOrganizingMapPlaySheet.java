package prerna.ui.components.playsheets;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.unsupervised.som.SelfOrganizingMap;
import prerna.algorithm.learning.unsupervised.som.SelfOrganizingMapGridViewer;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.util.Utility;
import cern.colt.Arrays;

public class SelfOrganizingMapPlaySheet extends GridPlaySheet{
	
	private static final Logger LOGGER = LogManager.getLogger(SelfOrganizingMapPlaySheet.class.getName());

	private SelfOrganizingMap alg;
	private double[][] coordinates;
	
	
	public SelfOrganizingMapPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		generateData();
		runAlgorithm();
		processAlgorithm();
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
		} else {
			coordinates = SelfOrganizingMapGridViewer.getGridCoordinates(alg.getLength(), alg.getHeight(), alg.getNumInstancesInGrid());
		}
	}
	
	public void processAlgorithm() {
		if(coordinates != null) {
			int i = 0;
			for(; i < coordinates.length; i++) {
				System.out.println(Arrays.toString(coordinates[i]));
			}
			int[] gridAssignmentForInstance = alg.getGridAssignmentForInstances();
			
			int gridLength = alg.getLength();
			
			i = 0;
			int numRows = list.size();
			int numColumns = list.get(0).length;
			ArrayList<Object[]> retList = new ArrayList<Object[]>();
			for(; i < numRows; i++) {
				Object[] values = new Object[numColumns + 3];
				Object[] oldValues = list.get(i);
				int j = 0;
				for(; j < numColumns; j++) {
					values[j] = oldValues[j];
				}
				values[j] = gridAssignmentForInstance[i];
				j++;
				int[] cellPosition = SelfOrganizingMapGridViewer.getCoordinatesOfCell(gridAssignmentForInstance[i], gridLength);
				values[j] = cellPosition[0];
				j++;
				values[j] = cellPosition[1];
				
				retList.add(values);
			}
			list = retList;
			
			i = 0;
			String[] retNames = new String[numColumns + 3];
			for(; i < numColumns; i++) {
				retNames[i] = names[i];
			}
			retNames[i] = "Cell";
			i++;
			retNames[i] = "X-Pos";
			i++;
			retNames[i] = "Y-Pos";
			names = retNames;
		}
	}

}
