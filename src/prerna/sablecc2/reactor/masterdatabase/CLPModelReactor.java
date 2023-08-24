package prerna.sablecc2.reactor.masterdatabase;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class CLPModelReactor extends AbstractReactor {

	private static final String CLASS_NAME = CLPModelReactor.class.getName();
	
	/**
	 * Class is used to visualize the conceptual -> logical -> physical model
	 */
	
	public CLPModelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONCEPTUAL_NAMES.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		List<String> cNames = getConceptulNamesList();
		List<String> engineFilters = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
		logger.info("Querying to retrieve conceptual to physical to logical model");
		List<String[]> values = MasterDatabaseUtility.getConceptualToLogicalToPhysicalModel(cNames, engineFilters);
		if(values == null) {
			throw new IllegalArgumentException("Query returned no results using the conceptual names passed in");
		}
		int size = values.size();
		if(size == 0) {
			throw new IllegalArgumentException("Query returned no results using the conceptual names passed in");
		}
		logger.info("Done querying");

		String[] headers = new String[]{"ConceptualName", "LogicalName", "PhysicalName", "Datasource"};
		String[] types = new String[]{"STRING", "STRING", "STRING", "STRING"};

		H2Frame frame = new H2Frame(headers, types);
		logger.info("Loading data into frame");
		PreparedStatement ps = null;
		try {
			ps = frame.createInsertPreparedStatement(headers);
			for(int i = 0; i < size; i++) {
				String[] row = values.get(i);
				ps.setString(1, row[0]);
				ps.setString(2, row[1]);
				ps.setString(3, row[2]);
				ps.setString(4, row[3]);
				
				ps.addBatch();
				if((i+1) % 100 == 0) {
					ps.executeBatch();
					logger.info("Finished inserting row " + i);
				}
			}
			
			// execute any remaining
			ps.executeBatch();
		} catch(SQLException e) {
			throw new IllegalArgumentException("Error occurred attempting to insert CLP model into a frame");
		} finally {
		    if(ps != null) {
                try {
            ps.close();
          } catch (SQLException e) {
              logger.error(Constants.STACKTRACE, e);
          	}
          }
		}
		logger.info("Done loading data");

		NounMetadata noun = null;
		if(this.insight.getDataMaker() == null) {
			// if no frame
			// set as default
			noun = new NounMetadata(frame, PixelDataType.FRAME, 
					PixelOperationType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
			this.insight.setDataMaker(frame);
			this.insight.getVarStore().put(frame.getName(), noun);
		} else {
			noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME);
		}
		
		return noun;
	}
	
	private List<String> getConceptulNamesList() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			List<String> cNames = grs.getAllStrValues();
			if(cNames == null || cNames.isEmpty()) {
				throw new IllegalArgumentException("Conceptual names list is empty");
			}
			return cNames;
		}
		
		List<String> cNames = this.curRow.getAllStrValues();
		if(cNames == null || cNames.isEmpty()) {
			throw new IllegalArgumentException("Conceptual names list is empty");
		}
		return cNames;
	}

}
