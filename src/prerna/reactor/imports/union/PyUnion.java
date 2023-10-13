package prerna.reactor.imports.union;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * Concrete Py union class.
 *
 */

public class PyUnion extends AbstractUnion {

	private Logger logger;
	private Map<String, String> colMappings;
	private PyTranslator pyT;

	public PyUnion() {

	}

	@Override
	public ITableDataFrame performUnion(ITableDataFrame a, ITableDataFrame b, String unionType, Insight insight,
			Logger logger) {
		List<String> aCols = getSemossCols(a.getQsHeaders());
		List<String> bCols = getSemossCols(b.getQsHeaders());
		checkPyBaseCases(a, b, aCols, bCols);
		this.logger = logger;
		logger.info("Running union on Py frame.");
		pyT = insight.getPyTranslator();
		PandasFrame frameA = (PandasFrame) a;
		PandasFrame frameB = (PandasFrame) b;
		ITableDataFrame[] frameArr;
		try {
			frameArr = matchColMetadata(insight, frameA, frameB, aCols, bCols);
		} catch (Exception e) {
			throw new SemossPixelException("Union frame array does not contain the frames for union.");
		}

		//String varName = "Union_Frame_" + Utility.getRandomString(5);
		String varName = frameArr[0].getName();
		String dropDups = ".drop_duplicates()";
		StringBuilder script = new StringBuilder();
		script.append(varName).append(" = pd.concat([").append(frameArr[0].getName()).append(",")
				.append(frameArr[1].getName()).append("]").append(", ignore_index=True").append(")");
		if (unionType.equals("union")) {
			script.append(dropDups);
		}
		script.append(".dropna()");
		String strScript = script.toString();
		pyT.runScript(strScript);
		return createFrameFromPyOutput(varName, pyT);
	}

	/**
	 * Below method flushes out the underlying py dataframe into a java PandasFrame.
	 * 
	 * @param varName
	 * @param pyT
	 * @return
	 */

	private ITableDataFrame createFrameFromPyOutput(String varName, PyTranslator pyT) {
		logger.info("Generating result.");
		String[] colNames = pyT.getStringArray(PandasSyntaxHelper.getColumns(varName));
		pyT.runScript(PandasSyntaxHelper.cleanFrameHeaders(varName, colNames));
		colNames = pyT.getStringArray(PandasSyntaxHelper.getColumns(varName));
		String[] colTypes = pyT.getStringArray(PandasSyntaxHelper.getTypes(varName));
		if (colNames == null || colTypes == null) {
			throw new IllegalArgumentException(
					"Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}
		PandasFrame frame = new PandasFrame(varName);
		pyT.runPyAndReturnOutput(PandasSyntaxHelper.makeWrapper(frame.getWrapperName(), varName));
		frame.setTranslator(pyT);
		ImportUtility.parseTableColumnsAndTypesToFlatTable(frame.getMetaData(), colNames, colTypes, varName);
		logger.info("Done.");
		return frame;
	}

	@Override
	public void setColMapping(Map<String, String> colMappings) {
		this.colMappings = colMappings;
	}

	private ITableDataFrame[] matchColMetadata(Insight insight, ITableDataFrame a, ITableDataFrame b,
			List<String> aCols, List<String> bCols) throws Exception {

		//ITableDataFrame aTemp = CopyFrameUtil.copyFrame(insight, a, -1);
		//ITableDataFrame bTemp = CopyFrameUtil.copyFrame(insight, b, -1);
		StringBuilder script = new StringBuilder();
		for (String col : aCols) {
			if (!colMappings.containsKey(col)) {
				//deleteFrameCols(a, col);
				//df.drop('column_name', axis=1, inplace=True)
				script.append(a.getName()).append(".drop('").append(col).append("', axis=1, inplace=True");
				pyT.runScript(script.toString());
				script.setLength(0);
			}
		}

		for (String col : bCols) {
			if (!colMappings.containsKey(col)) {
				script.append(b.getName()).append(".drop('").append(col).append("', axis=1, inplace=True");
				pyT.runScript(script.toString());
				script.setLength(0);
			}
		}

		realignCols(a, b, aCols, bCols);
		return new ITableDataFrame[] { a, b };

	}

	private void realignCols(ITableDataFrame a, ITableDataFrame b, List<String> aCols, List<String> bCols) {
		String dfName = a.getName();
		String script = new StringBuilder().append(dfName).append("=").append(dfName).append("[").append(aCols)
				.append("]").toString();
		logger.info(script);
		pyT.runScript(script);
		dfName = b.getName();
		script = new StringBuilder().append(dfName).append("=").append(dfName).append("[").append(aCols).append("]")
				.toString();
		logger.info(script);
		pyT.runScript(script);
	}

}
