package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class CollapseReactor extends AbstractRFrameReactor {

	public CollapseReactor() {
		this.keysToGet = new String[] { "groupByColumn", ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.DELIMITER.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getTableName();
		String groupByCol = this.keyValue.get(this.keysToGet[0]);
		String valueCol = this.keyValue.get(this.keysToGet[1]);
		String delim = this.keyValue.get(this.keysToGet[2]);

		// pull in script
		String collapseScriptPath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Collapse.r";
		StringBuilder rsb2 = new StringBuilder();
		collapseScriptPath = collapseScriptPath.replace("\\", "/");
		rsb2.append("source(\"" + collapseScriptPath + "\");");
		
		// add inputs to script
		String groupByRInput = "'"+groupByCol+"'";
		String valueColRInput = "'"+valueCol+"'";
		String delimRInput = "'"+delim+"'";
		String collapsedColName = valueCol + "_Collapse";
		
		// get resulting df
		String resultsDf = "resultsDf" + Utility.getRandomString(8);
		rsb2.append(resultsDf+"<- collapse("+frameName+", "+groupByRInput+", "+valueColRInput+", "+delimRInput+");");
		
		// TODO check for errors 
		// set new frame if no error occurs
		rsb2.append(frameName+"<- "+"as.data.table("+resultsDf+");");
		this.rJavaTranslator.runR(rsb2.toString());

		// update the metadata to include this new collapse column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(frameName, frameName + "__" + collapsedColName);
		metaData.setAliasToProperty(frameName + "__" + collapsedColName, collapsedColName);
		metaData.setDataTypeToProperty(frameName + "__" + collapsedColName, "STRING");


		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(collapsedColName));
		return retNoun;
	}

}
