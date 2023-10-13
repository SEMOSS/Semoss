package prerna.reactor.frame.r;

import java.util.ArrayList;
import java.util.List;

import prerna.ds.r.RDataTable;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class SummaryStatsReactor extends AbstractRFrameReactor {
	/**
	 * SummaryStats(column=["Species"], panel[99])
	 */
	public SummaryStatsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		RDataTable frame = (RDataTable) getFrame();
		String dtName = frame.getName();
		
		//get inputs
		String panelId = getPanelId();
		String column = getColumn();
		//clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		
		String script = "round(summary(" + dtName + "$" + column + "),3)";
		double[] values = this.rJavaTranslator.getDoubleArray(script);
		String[] headers = new String[] {"Min", "1st Quartile", "Median", "Mean", "3rd Quartile", "Max"};
		
		List<Object[]> data = new ArrayList<Object[]>();
		for(int i = 0; i < headers.length; i++) {
			Object[] row = new Object[] {headers[i], values[i]};
			data.add(row);
		}
		
		//task data includes task options
		ITask taskData = ConstantTaskCreationHelper.getGridData(panelId, new String[] {"metric", "value"}, data);
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getColumn() {
		GenRowStruct columnGRS = this.store.getNoun(keysToGet[0]);
		if (columnGRS != null && !columnGRS.isEmpty()) {
			return (String) columnGRS.getNoun(0).getValue();
		} else {
			throw new IllegalArgumentException("Column must be specified.");
		}
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

}
