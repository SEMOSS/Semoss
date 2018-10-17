package prerna.sablecc2.reactor.app.metaeditor;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.OWLER;
import prerna.util.Utility;

public class AddBulkOwlRelationshipsReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = AddBulkOwlRelationshipsReactor.class.getName();
	private static final String PROP_MAX = "propagation";
	
	/*
	 * This class assumes that the start table, start column, end table, and end column have already been defined
	 */
	
	public AddBulkOwlRelationshipsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FRAME.getKey(), PROP_MAX};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String distanceStr = this.keyValue.get(this.keysToGet[2]);
		if(distanceStr == null || distanceStr.isEmpty()) {
			// default to direct matches only
			distanceStr = "0";
		}
		double distance = Double.parseDouble(distanceStr);
		ITableDataFrame frame = getFrame();

		// we may have the alias
		appId = getAppId(appId);

		OWLER owler = getOWLER(appId);
		// set all the existing values into the OWLER
		// so that its state is updated
		IEngine engine = Utility.getEngine(appId);
		boolean isRdbms = (engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS || 
				engine.getEngineType() == IEngine.ENGINE_TYPE.IMPALA);
		setOwlerValues(engine, owler);
		
		// get tables
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("sourceTable"));
		qs.addSelector(new QueryColumnSelector("targetTable"));
		qs.addSelector(new QueryColumnSelector("sourceCol"));
		qs.addSelector(new QueryColumnSelector("targetCol"));
		qs.addSelector(new QueryColumnSelector("distance"));

		// add a filter
		NounMetadata lComparison = new NounMetadata(new QueryColumnSelector("distance"), PixelDataType.COLUMN);
		NounMetadata rComparison = new NounMetadata(distance, PixelDataType.CONST_DECIMAL);
		SimpleQueryFilter filter = new SimpleQueryFilter(lComparison, "<=" , rComparison);
		qs.addExplicitFilter(filter);
		
		int counter = 0;
		logger.info("Retrieving values to insert");
		IRawSelectWrapper iterator = frame.query(qs);
		while(iterator.hasNext()) {
			if(counter % 100 == 0) {
				logger.info("Adding relationship : #" + (counter+1));
			}
			IHeadersDataRow row = iterator.next();
			Object[] values = row.getValues();
			
			String startT = values[0].toString();
			String endT = values[1].toString();
			String startC = values[2].toString();
			String endC = values[3].toString();
			double relDistance = ((Number) values[4]).doubleValue();
			
			// generate the relationship
			String rel = startT + "." + startC + "." + endT + "." + endC;
			
			if(isRdbms) {
				// the relation has the startC and endC
				// what I really need is the primary key for the tables
				startC = Utility.getClassName(engine.getPhysicalUriFromConceptualUri(OWLER.BASE_URI + OWLER.DEFAULT_NODE_CLASS + "/" + startT));
				endC = Utility.getClassName(engine.getPhysicalUriFromConceptualUri(OWLER.BASE_URI + OWLER.DEFAULT_NODE_CLASS + "/" + endT));
			}
			
			// add the relationship
			owler.addRelation(startT, startC, endT, endC, rel);
			counter++;
		}
		logger.info("Done adding relationships");
		logger.info("Total relationships added = " + counter);

		// commit all the changes
		logger.info("Committing relationships");
		owler.commit();

		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add the relationships", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully adding relationships", 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS_MESSAGE));
		return noun;
	}

	private ITableDataFrame getFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[1]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		frameGrs = this.store.getNoun(PixelDataType.FRAME.toString());
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<Object> frames = this.curRow.getValuesOfType(PixelDataType.FRAME);
		if(frames != null && !frames.isEmpty()) {
			return (ITableDataFrame) frames.get(0);
		}
		
		throw new IllegalArgumentException("Need to define the frame which contains the inforamtion to bulk insert");
	}

}
