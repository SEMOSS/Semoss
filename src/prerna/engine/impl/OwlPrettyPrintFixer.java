package prerna.engine.impl;

import java.io.File;
import java.util.Properties;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.OWLER;

@Deprecated
public class OwlPrettyPrintFixer {

	@Deprecated
	public static void fixOwl(Properties prop) {
		File owlFile = SmssUtilities.getOwlFile(prop);
		if(owlFile != null && owlFile.exists()) {
			String conceptualRel = OWLER.SEMOSS_URI + OWLER.DEFAULT_RELATION_CLASS + "/" + OWLER.CONCEPTUAL_RELATION_NAME;
			
			// owl is stored as RDF/XML file
			RDFFileSesameEngine rfse = new RDFFileSesameEngine();
			rfse.openFile(owlFile.getAbsolutePath(), null, null);
	
			String query = "select ?s ?p ?o where {"
					+ "bind(<http://www.w3.org/2002/07/owl#Conceptual> as ?p)"
					+ "{?s ?p ?o}"
					+ "}";
			
			boolean write = false;
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
			while(wrapper.hasNext()) {
				write = true;
				Object[] badTriples = wrapper.next().getRawValues();
				rfse.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{badTriples[0], badTriples[1], badTriples[2], true});
				rfse.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{badTriples[0], conceptualRel, badTriples[2], true});
			}
			
			if(write) {
				try {
					rfse.exportDB();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
