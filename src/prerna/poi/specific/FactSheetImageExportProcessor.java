package prerna.poi.specific;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.specific.tap.HealthGridExporter;
import prerna.util.DIHelper;

public class FactSheetImageExportProcessor {

	private static final Logger logger = LogManager.getLogger(FactSheetImageExportProcessor.class.getName());

	public void runImageExport() {
		//Select Systems
		ArrayList<String> sysList = new ArrayList<String>();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data");

		//for services
		String query = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?Owner}}ORDER BY ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";

		//for central
		//	String query = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?Owner}}ORDER BY ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)}";

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			SesameJenaSelectStatement sjss = wrapper.next();
			String sys = (String)sjss.getVar(names[0]);
			sysList.add(sys);
		}

		//Call the Image Exporters
		//		CONUSMapExporter conusExporter = new CONUSMapExporter();
		//		conusExporter.processData(sysList);

		//		OCONUSMapExporter oconusExporter = new OCONUSMapExporter();
		//		oconusExporter.processData(sysList);

		HealthGridExporter healthExporter = new HealthGridExporter();
		healthExporter.processData(sysList);

		logger.info("Map and Grid Export Button Pushed.");
	}

}
