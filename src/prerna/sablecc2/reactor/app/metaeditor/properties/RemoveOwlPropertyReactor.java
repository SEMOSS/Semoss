package prerna.sablecc2.reactor.app.metaeditor.properties;

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

public class RemoveOwlPropertyReactor extends AbstractMetaEditorReactor {

	public RemoveOwlPropertyReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey() };
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
		String column = this.keyValue.get(this.keysToGet[2]);
		if( column == null || column.isEmpty()) {
			throw new IllegalArgumentException("Must define the property being added to the app metadata");
		}
		// if RDBMS, we need to know the prim key of the column
		IEngine engine = Utility.getEngine(appId);
		RDFFileSesameEngine owlEngine = engine.getBaseDataEngine();
		String physicalPropUri = engine.getPhysicalUriFromConceptualUri(column, concept);
		if(physicalPropUri == null) {
			throw new IllegalArgumentException("Cannot find property in existing metadata to remove");
		}
		
		// remove everything downstream of the property
		{
			String query = "select ?s ?p ?o where "
					+ "{ "
					+ "bind(<" + physicalPropUri + "> as ?s) "
					+ "{?s ?p ?o} "
					+ "}";
		
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
		
		// repeat for upstream of the property
		{
			String query = "select ?s ?p ?o where "
					+ "{ "
					+ "bind(<" + physicalPropUri + "> as ?o) "
					+ "{?s ?p ?o} "
					+ "}";
		
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
		
		try {
			owlEngine.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to remove the desired property", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
	
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully removed property", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
