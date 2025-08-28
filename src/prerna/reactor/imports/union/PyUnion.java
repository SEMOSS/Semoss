/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

/** Concrete Py union class. */
public class PyUnion extends AbstractUnion {

  private Logger logger;
  private Map<String, String> colMappings;
  private PyTranslator pyT;

  @Override
  public ITableDataFrame performUnion(
      ITableDataFrame a, ITableDataFrame b, String unionType, Insight insight, Logger logger) {
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

    // String varName = "Union_Frame_" + Utility.getRandomString(5);
    String varName = frameArr[0].getName();
    String dropDups = ".drop_duplicates()";
    StringBuilder script = new StringBuilder();
    script
        .append(varName)
        .append(" = pd.concat([")
        .append(frameArr[0].getName())
        .append(",")
        .append(frameArr[1].getName())
        .append("]")
        .append(", ignore_index=True")
        .append(")");
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
   * @param pyTranslator
   * @return
   */
  private ITableDataFrame createFrameFromPyOutput(String varName, PyTranslator pyTranslator) {
    logger.info("Generating result.");
    String[] colNames = pyTranslator.getStringArray(PandasSyntaxHelper.getColumns(varName));
    pyTranslator.runScript(PandasSyntaxHelper.cleanFrameHeaders(varName, colNames));
    colNames = pyTranslator.getStringArray(PandasSyntaxHelper.getColumns(varName));
    String[] colTypes = pyTranslator.getStringArray(PandasSyntaxHelper.getTypes(varName));
    if (colNames == null || colTypes == null) {
      throw new IllegalArgumentException(
          "Please make sure the variable "
              + varName
              + " exists and can be a valid data.table object");
    }
    PandasFrame frame = new PandasFrame(varName, pyTranslator);
    pyTranslator.runEmptyPy(PandasSyntaxHelper.makeWrapper(frame.getWrapperName(), varName));
    ImportUtility.parseTableColumnsAndTypesToFlatTable(
        frame.getMetaData(), colNames, colTypes, varName);
    logger.info("Done.");
    return frame;
  }

  @Override
  public void setColMapping(Map<String, String> colMappings) {
    this.colMappings = colMappings;
  }

  private ITableDataFrame[] matchColMetadata(
      Insight insight, ITableDataFrame a, ITableDataFrame b, List<String> aCols, List<String> bCols)
      throws Exception {

    // ITableDataFrame aTemp = CopyFrameUtil.copyFrame(insight, a, -1);
    // ITableDataFrame bTemp = CopyFrameUtil.copyFrame(insight, b, -1);
    StringBuilder script = new StringBuilder();
    for (String col : aCols) {
      if (!colMappings.containsKey(col)) {
        // deleteFrameCols(a, col);
        // df.drop('column_name', axis=1, inplace=True)
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
    return new ITableDataFrame[] {a, b};
  }

  private void realignCols(
      ITableDataFrame a, ITableDataFrame b, List<String> aCols, List<String> bCols) {
    String dfName = a.getName();
    String script =
        new StringBuilder()
            .append(dfName)
            .append("=")
            .append(dfName)
            .append("[")
            .append(aCols)
            .append("]")
            .toString();
    logger.info(script);
    pyT.runScript(script);
    dfName = b.getName();
    script =
        new StringBuilder()
            .append(dfName)
            .append("=")
            .append(dfName)
            .append("[")
            .append(aCols)
            .append("]")
            .toString();
    logger.info(script);
    pyT.runScript(script);
  }
}
