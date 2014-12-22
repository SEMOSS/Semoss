/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.swing.JComponent;
import javax.swing.JTable;
import javax.swing.table.TableModel;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class JTableExcelExportListener extends AbstractListener {
	private final String WORKING_DIR = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	private final String EXPORT_FOLDER = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Excel" + System.getProperty("file.separator");
	private JTable table;
	private String filename;

	public JTableExcelExportListener(JTable table, String questionTitle) {
		this.table = table;
		this.filename = this.WORKING_DIR + this.EXPORT_FOLDER + questionTitle.replace("?", "") + ".xlsx";
	}

	/**
	 * Runs Excel sheet export of current values displayed in Grid Table format.
	 * 
	 * @return	boolean successfulExport - whether or not the export was completed successfully, used to display the correct alert message
	 */
	public boolean runExport() {
		boolean successfulExport = true;
		FileOutputStream stream = null;
		try{
			TableModel model = this.table.getModel();
			XSSFWorkbook wb = new XSSFWorkbook();
			XSSFSheet sheet = wb.createSheet("Export");

			int columnCount = model.getColumnCount();

			//Write out column headers in first row
			XSSFRow headerRow = sheet.createRow(0);
			for(int i = 0; i < columnCount; i++){
				XSSFCell cell = headerRow.createCell(i);
				cell.setCellValue(model.getColumnName(i));
			}

			//Write out instance data in following rows
			for(int i=0; i < model.getRowCount(); i++) {
				XSSFRow dataRow = sheet.createRow(i+1);
				for(int j=0; j < columnCount; j++) {
					XSSFCell cell = dataRow.createCell(j);
					cell.setCellValue(model.getValueAt(i,j).toString());
				}
			}

			//Write all data to file
			stream = new FileOutputStream(this.filename);
			wb.write(stream);
			stream.close();
		} catch(IOException e) {
			successfulExport = false;
			System.out.println(e);
		} finally {
			try {
				if(stream!=null)
					stream.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}

		return successfulExport;
	}

	@Override
	public void actionPerformed(ActionEvent arg0) {
		boolean success = runExport();

		if(success) {
			Utility.showMessage("Data export complete:\n\n" + this.filename);
		} else {
			Utility.showError("Export failed. Please check export folder structure (" + this.WORKING_DIR + this.EXPORT_FOLDER + ").");
		}
	}

	@Override
	public void setView(JComponent view) { }
}
