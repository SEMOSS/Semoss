package prerna.sablecc2.reactor.app.metaeditor.concepts;

import java.util.List;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.Utility;

public class RemoveOwlConceptReactor extends AbstractMetaEditorReactor {

	public RemoveOwlConceptReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String appId = this.keyValue.get(this.keysToGet[0]);
		// perform translation if alias is passed
		// and perform security check
		appId = getAppId(appId, true);
		
		String concept = this.keyValue.get(this.keysToGet[1]);
		if(concept == null || concept.isEmpty()) {
			throw new IllegalArgumentException("Must define the concept being added to the app metadata");
		}
		
		// since we are deleting the node
		// i need to delete the properties of this node
		// and then everything related to this node
		IEngine engine = Utility.getEngine(appId);
		RDFFileSesameEngine owlEngine = engine.getBaseDataEngine();
		String conceptPhysical = engine.getConceptPhysicalUriFromConceptualUri(concept);
		List<String> properties = engine.getProperties4Concept(conceptPhysical, false);
		StringBuilder bindings = new StringBuilder();
		for(String prop : properties) {
			bindings.append("(<").append(prop).append(">)");
		}
		
		if(bindings.length() > 0) {
			// get everything downstream of the props
			{
				String query = "select ?s ?p ?o where "
						+ "{ "
						+ "{?s ?p ?o} "
						+ "} bindings ?s {" + bindings.toString() + "}";
			
				IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
				while(it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					Object[] raw = headerRows.getRawValues();
					String s = raw[0].toString();
					String p = raw[1].toString();
					String o = raw[2].toString();
					boolean bool = !objectIsLiteral(p);
					owlEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[]{s, p, o, bool});
				}
			}
			
			// repeat for upstream of prop
			{
				String query = "select ?s ?p ?o where "
						+ "{ "
						+ "{?s ?p ?o} "
						+ "} bindings ?o {" + bindings.toString() + "}";
			
				IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
				while(it.hasNext()) {
					IHeadersDataRow headerRows = it.next();
					Object[] raw = headerRows.getRawValues();
					String s = raw[0].toString();
					String p = raw[1].toString();
					String o = raw[2].toString();
					boolean bool = !objectIsLiteral(p);
					owlEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[]{s, p, o, bool});
				}
			}
		}
		
		boolean hasTriple = false;
		
		// now repeat for the node itself
		// remove everything downstream of the node
		{
			String query = "select ?s ?p ?o where "
					+ "{ "
					+ "bind(<" + conceptPhysical + "> as ?s) "
					+ "{?s ?p ?o} "
					+ "}";
		
			IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
			while(it.hasNext()) {
				hasTriple = true;
				IHeadersDataRow headerRows = it.next();
				Object[] raw = headerRows.getRawValues();
				String s = raw[0].toString();
				String p = raw[1].toString();
				String o = raw[2].toString();
				boolean bool = !objectIsLiteral(p);
				owlEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[]{s, p, o, bool});
			}
		}
		
		// repeat for upstream of the node
		{
			String query = "select ?s ?p ?o where "
					+ "{ "
					+ "bind(<" + conceptPhysical + "> as ?o) "
					+ "{?s ?p ?o} "
					+ "}";
		
			IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(owlEngine, query);
			while(it.hasNext()) {
				hasTriple = true;
				IHeadersDataRow headerRows = it.next();
				Object[] raw = headerRows.getRawValues();
				String s = raw[0].toString();
				String p = raw[1].toString();
				String o = raw[2].toString();
				boolean bool = !objectIsLiteral(p);
				owlEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[]{s, p, o, bool});
			}
		}
		
		if(!hasTriple) {
			throw new IllegalArgumentException("Cannot find concept in existing metadata to remove");
		}
		
		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to remove the desired concept", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
	
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed concept and all its dependencies", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
}
