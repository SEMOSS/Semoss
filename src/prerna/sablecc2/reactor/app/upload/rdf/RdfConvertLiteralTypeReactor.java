package prerna.sablecc2.reactor.app.upload.rdf;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import cern.colt.Arrays;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class RdfConvertLiteralTypeReactor extends AbstractReactor {

	private static final String CLASS_NAME = RdfConvertLiteralTypeReactor.class.getName();
	
	public RdfConvertLiteralTypeReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey(), 
				ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.DATA_TYPE.getKey()
		};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String concept = this.keyValue.get(this.keysToGet[1]);
		String property = this.keyValue.get(this.keysToGet[2]);
		String dataType = this.keyValue.get(this.keysToGet[3]);

		if(appId == null || appId.isEmpty()) {
			throw new NullPointerException("Must provide an app id");
		}
		if(AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityAppUtils.userIsOwner(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("Database " + appId + " does not exist or user is not an owner to the database");
			}
		}
		if(concept == null || concept.isEmpty()) {
			throw new NullPointerException("Must provide a value for concept");
		}
		if(property == null || property.isEmpty()) {
			throw new NullPointerException("Must provide a value for property");
		}
		if(dataType == null || dataType.isEmpty()) {
			throw new NullPointerException("Must provide a value for data type");
		}
		
		Logger logger = getLogger(CLASS_NAME);
		
		final SemossDataType newDataType = SemossDataType.convertStringToDataType(dataType);
		
		final String propertyUri = "http://semoss.org/ontologies/Relation/Contains/" + property;
		String query = "select ?concept ?property where { "
				+ "{?concept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/" + concept + ">} "
				+ "{?concept <" + propertyUri + "> ?property} "
				+ "}";
		
		List<Object[]> collection = new Vector<>();
		
		IEngine engine = Utility.getEngine(appId);
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(engine, query);
			while(iterator.hasNext()) {
				IHeadersDataRow row = iterator.next();
				String rawUri = row.getRawValues()[0] + "";
				Object literal = row.getValues()[1];
				collection.add(new Object[] {rawUri, literal});
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(iterator != null) {
				iterator.cleanUp();
			}
		}
		
		String warning = null;
		for(Object[] modification : collection) {
			String subject = modification[0] + "";
			Object object = modification[1];
			
			logger.info("Modifying object " + Arrays.toString(modification));
			// remove
			engine.doAction(ACTION_TYPE.REMOVE_STATEMENT, 
					new Object[] {subject, propertyUri, object, false});
			
			// now we try to convert the object
			try {
				Object newObject = null;
				if(newDataType == SemossDataType.STRING) {
					newObject = object + "";
				} else if(newDataType == SemossDataType.INT 
						|| newDataType == SemossDataType.DOUBLE) {
					newObject = Double.parseDouble(object + "");
				}
				
				// add
				engine.doAction(ACTION_TYPE.ADD_STATEMENT, 
						new Object[] {subject, propertyUri, newObject, false});
			} catch(Exception e) {
				warning = "Some values did not property parse";
			}
		}
		engine.commit();
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if(warning == null) {
			noun.addAdditionalReturn(getSuccess("Successfully modified the data type of " + property + " to " + newDataType));
		} else {
			noun.addAdditionalReturn(getWarning(warning));
		}
		return noun;
	}

}
 