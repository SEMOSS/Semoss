package prerna.sablecc2.reactor.frame.r;

import org.rosuda.REngine.Rserve.RConnection;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.IRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;

public class GenerateFrameFromRVariableReactor extends AbstractRFrameReactor {
	
	public GenerateFrameFromRVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		String varName = getVarName();
		this.rJavaTranslator.executeEmptyR(varName + " <- as.data.table(" + varName + ")");
		// recreate a new frame and set the frame name
		String[] colNames = this.rJavaTranslator.getColumns(varName);
		String[] colTypes = this.rJavaTranslator.getColumnTypes(varName);

		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}
		
		VarStore vars = this.insight.getVarStore();
		RDataTable newTable = null;
		if (vars.get(IRJavaTranslator.R_CONN) != null && vars.get(IRJavaTranslator.R_PORT) != null) {
			newTable = new RDataTable(varName, 
					(RConnection) vars.get(IRJavaTranslator.R_CONN).getValue(), 
					(String) vars.get(IRJavaTranslator.R_PORT).getValue());
		} else {
			newTable = new RDataTable(varName);
		}
		ImportUtility.parserRTableColumnsAndTypesToFlatTable(newTable, colNames, colTypes, varName);
		this.insight.setDataMaker(newTable);
		return new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	/**
	 * Get the input being the r variable name
	 * @return
	 */
	private String getVarName() {
		return this.curRow.get(0).toString();
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VARIABLE.getKey())) {
			return "Name of the r variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
