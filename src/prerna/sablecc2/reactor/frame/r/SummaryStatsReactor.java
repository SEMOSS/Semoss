package prerna.sablecc2.reactor.frame.r;

import java.util.List;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.Utility;

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
		String smryDt = "smryDt" + Utility.getRandomString(6);
		StringBuilder sb = new StringBuilder();
		// summary stats r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\SummaryStats.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\");");
		sb.append(smryDt + "<- getSmryDt(" + dtName + ",'" + column + "');");
		
		// execute R
		System.out.println(sb.toString());
		this.rJavaTranslator.runR(sb.toString());
		
		String[] smryDtCols = this.rJavaTranslator.getColumns(smryDt);
		List<Object[]> data = this.rJavaTranslator.getBulkDataRow(smryDt, smryDtCols);
		
		// clean up r temp variables
		this.rJavaTranslator.runR("rm(" + smryDt + ",getSmryDt);gc();");

		if (smryDtCols.length == 0) {
			throw new IllegalArgumentException("Summary stats are not available.");
		}
		
		//task data includes task options
		ITask taskData = ConstantTaskCreationHelper.getGridData(panelId, smryDtCols, data);
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
