package prerna.poi.specific;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.specific.tap.CONUSMapExporter;
import prerna.ui.components.specific.tap.HealthGridExporter;
import prerna.ui.components.specific.tap.OCONUSMapExporter;
import prerna.util.DIHelper;

public class FactSheetImageExportProcessor {

	Logger logger = Logger.getLogger(getClass());
	
	public void runImageExport() {
		//Select Systems
		ArrayList<String> sysList = new ArrayList<String>();
		HashMap<String,String> catHash= new HashMap<String,String>();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		String query = "SELECT DISTINCT ?System ?Owner WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?Has ?Owner}}ORDER BY ?Owner ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/Service/Army>)(<http://health.mil/ontologies/Concept/Service/Navy>)(<http://health.mil/ontologies/Concept/Service/Air_Force>)(<http://health.mil/ontologies/Concept/Service/Central>)}";

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				String sys = (String)sjss.getVar(names[0]);
				String cat = (String)sjss.getVar(names[1]);
				sysList.add(sys);
				catHash.put(sys,cat);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		query = "SELECT DISTINCT ?System ?SystemCategory WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}} ORDER BY ?SystemCategory ?System BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)(<http://health.mil/ontologies/Concept/SystemCategory/Central>)}";

		wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				String sys = (String)sjss.getVar(names[0]);
				String cat = (String)sjss.getVar(names[1]);
				if(!catHash.containsKey(sys))
				{
					sysList.add(sys);
					catHash.put(sys,cat);
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		//Call the Image Exporters
//		CONUSMapExporter conusExporter = new CONUSMapExporter();
//		conusExporter.processData(sysList);
//
//		OCONUSMapExporter oconusExporter = new OCONUSMapExporter();
//		oconusExporter.processData(sysList);

		HealthGridExporter healthExporter = new HealthGridExporter();
		healthExporter.processData(sysList,catHash);

		logger.info("Map and Grid Export Button Pushed.");
	}

}
