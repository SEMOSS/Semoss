package prerna.sablecc2.reactor.frame.py;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class JoinColumnsReactor extends AbstractFramePyReactor {

	/**
	 * This reactor joins columns, and puts the joined string into a new column
	 * with values separated by a separator The inputs to the reactor are: 
	 * 1) the new column name 
	 * 2) the delimiter 
	 * 3) the columns to join
	 */
	
	public JoinColumnsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DELIMITER.getKey(), ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() 
	{
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get table name
		String table = frame.getName();

		// create string builder to build the r script
		StringBuilder rsb = new StringBuilder();

		// first input is what we want to name the new column
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null) {
			newColName = getNewColName();
		}
		// check if new colName is valid
		newColName = getCleanNewColName(table, newColName);
		
		// second input is the delimeter/separator
		String separator = this.keyValue.get(this.keysToGet[1]);
		if (separator == null) {
			separator = getSeparator();
		}

		StringBuilder colList = new StringBuilder("");
		// the remaining inputs are all of the columns that we want to join
		List<String> columnList = getColumns();
		for (int i = 0; i < columnList.size(); i++) {
			String column = columnList.get(i);
			// separate the column name from the frame name
			if (column.contains("__")) {
				column = column.split("__")[1];
			}
			
			if(i > 0)
				colList.append(".str.cat(" + table + ".cache['data']['").append(column).append("'], sep='" + separator + "')");
			else
				colList.append(table + ".cache['data']['").append(column).append("'").append("]");			
		}
		
		// change it
		frame.runScript(table + ".cache['data']['" + newColName + "'] = " + colList);
		//frame.runScript(table + "w.cache['data'] = " + table);
		
		recreateMetadata(frame, false);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"JoinColumns", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getNewColName() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first input is what we want to name the new column
			String newColName = inputsGRS.getNoun(0).getValue() + "";
			if (newColName.length() == 0) {
				throw new IllegalArgumentException("Need to define the new column name");
			}
			return newColName;
		}
		throw new IllegalArgumentException("Need to define the new column name");
	}

	private String getSeparator() {
		// get separator by index
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String separator = input2.getValue() + "";
		return separator;
	}

	private List<String> getColumns() {
		List<String> columns = new Vector<>();
		// get columns by key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		NounMetadata noun;
		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				noun = grs.getNoun(i);
				if (noun != null) {
					String column = noun.getValue() + "";
					if (column.length() > 0) {
						columns.add(column);
					}
				}
			}
		} else {

			// get columns by index
			int inputSize = this.getCurRow().size();
			for (int i = 2; i < inputSize; i++) {
				NounMetadata input = this.getCurRow().getNoun(i);
				String column = input.getValue() + "";
				columns.add(column);
			}
		}
		return columns;
	}
	
}
