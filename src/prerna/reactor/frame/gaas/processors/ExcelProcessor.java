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
package prerna.reactor.frame.gaas.processors;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class ExcelProcessor extends AbstractFileProcessor {

    private static final Logger classLogger = LogManager.getLogger(ExcelProcessor.class);

    public ExcelProcessor(String filePath, VectorDatabaseCSVWriter writer) {
        super(filePath, writer);
    }

    @Override
    public void process() throws IOException {
        FileInputStream is = null;
        Workbook workbook = null;
        try {
            is = new FileInputStream(this.filePath);
            // WorkbookFactory handles both .xls (HSSF) and .xlsx (XSSF)
            workbook = WorkbookFactory.create(is);
            processWorkbook(workbook);
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw e;
        } finally {
            if (workbook != null) {
                try {
                    workbook.close();
                } catch (IOException e) {
                    classLogger.error("Error closing workbook", e);
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    classLogger.error("Error closing FileInputStream", e);
                }
            }
        }
    }

    private void processWorkbook(Workbook workbook) {
        String source = getSource(this.filePath);
        DataFormatter dataFormatter = new DataFormatter();
        FormulaEvaluator formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();
        formulaEvaluator.setIgnoreMissingWorkbooks(true);

        int numberOfSheets = workbook.getNumberOfSheets();
        int pageCount = 1;

        for (int i = 0; i < numberOfSheets; i++) {
            Sheet sheet = workbook.getSheetAt(i);
            StringBuilder sheetText = new StringBuilder();
            String sheetName = sheet.getSheetName();
            sheetText.append("Sheet: ").append(sheetName).append("\n");

            for (Row row : sheet) {
                StringBuilder rowText = new StringBuilder();
                boolean firstCell = true;

                for (Cell cell : row) {
                    if (!firstCell) {
                        rowText.append("\t");
                    }
                    firstCell = false;
                    rowText.append(getCellValue(cell, dataFormatter, formulaEvaluator));
                }

                String rowStr = rowText.toString().trim();
                if (!rowStr.isEmpty()) {
                    sheetText.append(rowStr).append("\n");
                }
            }

            this.writer.writeRow(source, pageCount + "", sheetText.toString());
            pageCount++;
        }
    }

    private String getCellValue(Cell cell, DataFormatter dataFormatter, FormulaEvaluator formulaEvaluator) {
        if (cell == null) {
            return "";
        }

        try {
            // formatCellValue with the evaluator handles all cell types,
            // including evaluating formulas to their computed result
            return dataFormatter.formatCellValue(cell, formulaEvaluator);
        } catch (Exception e) {
            classLogger.debug("Formula evaluation failed for cell, using raw value", e);
            // fall back to formatting without evaluation (may return formula string for
            // formula cells)
            return dataFormatter.formatCellValue(cell);
        }
    }

}
