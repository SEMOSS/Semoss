package prerna.reactor.imports.union;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.poi.main.HeadersException;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * Concrete R union class.
 *
 */
public class RUnion extends AbstractUnion{
	
	private AbstractRJavaTranslator rJavaTranslator;
	private Logger logger;
	private Map<String, String> colMappings;
	
	public RUnion() {
		
	}

	@Override
	public ITableDataFrame performUnion(ITableDataFrame a, ITableDataFrame b, String unionType, Insight insight, Logger logger) {
		List<String> aCols = getSemossCols(a.getQsHeaders());
		List<String> bCols = getSemossCols(b.getQsHeaders());
		checkRBaseCases(a, b, aCols, bCols);
		this.logger = logger;
		this.rJavaTranslator = insight.getRJavaTranslator(logger);
		ITableDataFrame[] frameArr;
		try {
			frameArr = matchColMetadata(insight, a, b, aCols, bCols);
		} catch (Exception e) {
			throw new SemossPixelException("Union frame array does not contain the frames for union.");
		}
		unionR(unionType, frameArr[0].getName(), frameArr[1].getName(), insight);
		return a;
	}
	
	/**
	 * This method takes both the frames and runs the union script
	 * in the R interpretor.
	 * 
	 * @param unionType
	 * @param frameA
	 * @param frameB
	 * @return
	 */
	
	private void unionR(String unionType, String frameA, String frameB, Insight insight) {
		StringBuilder script = new StringBuilder();
		//String retVar = "Union_Frame_" + Utility.getRandomString(5);
		String uType = null;
		if(unionType.equals("union")) {
			uType = "union";
		}else {
			uType = "union_all";
		}
		script.append(frameA).append("<-").append(uType).append("(").append(frameA).append(",").append(frameB).append(")");
		rJavaTranslator.runR(script.toString());
		//return frameA;
		//return createNewFrameFromVariable(retVar, insight);
	}
	
	/********************************************
	 * 
	 * Helper methods to flush R data into new
	 * variables.
	 * 
	 * ******************************************
	 */
	
	protected RDataTable createNewFrameFromVariable(String frameName, Insight insight) {
		// create new frame
		RDataTable newTable = new RDataTable(insight.getRJavaTranslator(logger), frameName);
		OwlTemporalEngineMeta meta = genNewMetaFromVariable(frameName);
		newTable.setMetaData(meta);
		return newTable;
	}
	
	protected OwlTemporalEngineMeta genNewMetaFromVariable(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = getColumns(frameName);
		String[] colTypes = getColumnTypes(frameName);
		//clean headers
		HeadersException headerChecker = HeadersException.getInstance();
		colNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		String rColNames = "";
		for (int i = 0; i < colNames.length; i++) {
			rColNames += "\"" + colNames[i] + "\"";
			if (i < colNames.length - 1) {
				rColNames += ", ";
			}
		}
		String script = "colnames(" + frameName + ") <- c(" + rColNames + ")";
		this.rJavaTranslator.executeEmptyR(script);
		
		OwlTemporalEngineMeta meta = new OwlTemporalEngineMeta();
		ImportUtility.parseTableColumnsAndTypesToFlatTable(meta, colNames, colTypes, frameName);
		return meta;
	}
	
	public String[] getColumns(String frameName) {
		return this.rJavaTranslator.getColumns(frameName);
	}

	public String[] getColumnTypes(String frameName) {
		return this.rJavaTranslator.getColumnTypes(frameName);
	}

	private ITableDataFrame[] matchColMetadata(Insight insight, ITableDataFrame a, ITableDataFrame b, List<String> aCols, List<String> bCols) throws Exception {
		
		//ITableDataFrame aTemp = CopyFrameUtil.copyFrame(insight, a, -1);
		//ITableDataFrame bTemp = CopyFrameUtil.copyFrame(insight, b, -1);
		
		//if(colMappings != null) {
			for(String col : aCols) {
				if(!colMappings.containsKey(col)) {
					deleteFrameCols(a, col);
				}
			}
			
			Set<String> colBSet = new HashSet<>(colMappings.values().stream().map(String :: trim).collect(Collectors.toList()));
			for(String col : bCols) {
				if(!colBSet.contains(col)) {
					deleteFrameCols(b, col);
				}
			}
		//}
//		for(Entry<String, String> entry : colMappings.entrySet()) {
//			String leftColNm = entry.getKey();
//			String rightColNm = entry.getValue();
//			renameCol(b, leftColNm, rightColNm);
//		}
		realignCols(a, b, aCols, bCols); 
		
		return new ITableDataFrame[] {a, b};
		
	}
	
	private void realignCols(ITableDataFrame a, ITableDataFrame b, List<String> aCols, List<String> bCols) {
		String dfName = a.getName();
		String script = new StringBuilder().append(dfName).append("<-").append(dfName).append("[, c(").append(aCols).append(")]").toString();
		logger.info(script);
		rJavaTranslator.runR(script);
		dfName = b.getName();
		script = new StringBuilder().append(dfName).append("<-").append(dfName).append("[, c(").append(bCols).append(")]").toString();
		logger.info(script);
		rJavaTranslator.runR(script);
	}
	
	public void setColMapping(Map<String, String> colMappings) {
		this.colMappings = colMappings;
	}
	
	private void renameCol(ITableDataFrame frame, String leftCol, String rightCol) {
		String script = new StringBuilder().append(frame.getName()).append("<-").append(frame.getName()).append(" %>% ")
				.append("rename(").append(leftCol).append("=").append(rightCol).append(")").toString();
		rJavaTranslator.runR(script);
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		frame.syncHeaders();
	}

	
}
