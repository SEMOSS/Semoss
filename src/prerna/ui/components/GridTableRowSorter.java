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
package prerna.ui.components;

import java.util.Comparator;

import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.bigdata.rdf.model.BigdataURIImpl;
import com.itextpdf.text.pdf.PdfStructTreeController.returnType;


/**
 * This class is used to create a table model for grids.
 */
public class GridTableRowSorter <M extends TableModel> extends TableRowSorter<TableModel> {
	MyComparator comparator = new MyComparator();
	static final Logger logger = LogManager.getLogger(GridTableRowSorter.class.getName());
	
	public GridTableRowSorter(TableModel tm){
		super(tm);
	}
	
    @Override
    public boolean useToString(int column){
    	return false;
    }
	
    @Override
    public Comparator getComparator(int column){
    	return comparator;
    }
    
    private class MyComparator implements Comparator<Object>{

		@Override
		public int compare(Object o1, Object o2) {
			if(o1 == null){
				if(o2 == null){
					return 0;
				}
				else{
					return 1;
				}
			}
			else if(o2 == null){
				return -1;
			}
			else{
				if(o1 instanceof Integer) {
					o1 = new Double((Integer)o1);
				}
				if(o2 instanceof Integer) {
					o2 = new Double((Integer)o2);
				}
				
				if(o1 instanceof String){
					if(o2 instanceof String){
						return ((String)o1).compareTo((String) o2);
					}
					else if(o2 instanceof Double){
						return -1; //string comes before double
					}
				}
				else if(o2 instanceof String){
					if(o1 instanceof Double){
						return 1;//string comes before double
					}
				}
				else if(o1 instanceof Double){
					if(o2 instanceof Double){
						if(((Double)o1) > ((Double)o2)) {
							return 1;
						}
						else if(((Double)o1) < ((Double)o2)) {
							return -1;
						}
						else if(Double.doubleToLongBits(((Double)o1)) ==
				                  Double.doubleToLongBits(((Double)o2))) {
							return 0;
						}
						else if(Double.isNaN((Double)o1)){
							if(Double.isNaN((Double)o2)){
								return 0;
							}
							else{
								return 1;
							}
						}
						else if(Double.isNaN((Double)o2)){
							return -1;
						}
					}
				}
			}

			logger.debug("Need to add logic to compare classes: " + o1.getClass() + "     " +o1 + "  and     " + o2.getClass()+"     " +o2);
			return (o1.toString()).compareTo(o2.toString());
		}
    }
}
