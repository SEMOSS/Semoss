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

import java.awt.Component;
import java.util.EventObject;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This class is used to render edge filters.
 */
public class EdgeFilterRenderer extends JComboBox implements TableCellRenderer, TableCellEditor {
	
	static final Logger logger = LogManager.getLogger(EdgeFilterRenderer.class.getName());
	protected Vector listeners = new Vector();
	int originalValue;
	static Double [] data = {new Double(100),new Double(200)};
	
	/**
	 * Constructor for EdgeFilterRenderer.
	 */
	public EdgeFilterRenderer()
	{
		super(data);
		//super(JSlider.HORIZONTAL);//, 0, 100);
		//this.setMinimum(0);
		//this.setMaximum(100);
		
		//super()
	}
	
	/**
	 * Creates the component that is used for drawing the cell.
	 * @param table 		JTable that is asking the renderer to draw.
	 * @param value 		Value of the cell to be rendered.
	 * @param isSelected 	True if the cell is to be rendered with the selection highlighted.
	 * @param hasFocus 		If true, render cell appropriately.
	 * @param row 			Row index of the cell being drawn.
	 * @param column 		Column index of the cell being drawn.
	
	 * @return Component 	Used for drawing the cell. */
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) {
		// if the column number is 3
		// and the value is not null
		// I need to make a slider set the value and give that back. 
		//Component retComponent = super.getTableCellRendererComponent(table, value, isSelected, hasFocus,
		//		row, column);
		Component retComponent = null;
		
		Double val = ((Double)value);
		int intVal = val.intValue();
		logger.debug("Value is " + value);
		logger.debug("Row and Column are " + row + "<>" + column);
		if(intVal != 0)
		{
			logger.debug("Value being set is " + intVal);
			setSelectedItem(value);
			//this.setValue(intVal);
			retComponent = this;
			/*this.addChangeListener(new ChangeListener(){

				@Override
				public void stateChanged(ChangeEvent arg0) {
					logger.debug("State Changed >>>>>>>>>>>>> " );
					
				}
				
			});*/
		}
		else
		{
			retComponent = new JLabel("");
		}
		return retComponent;
	}

	/**
	 * Adds a listener to the list that is s notified when the editor stops or cancels editing.
	 * @param arg0 	Cell editor listener.
	 */
	@Override
	public void addCellEditorListener(CellEditorListener arg0) {
		listeners.addElement(arg0);
		
	}

	/**
	 * Tells the editor to cancel editing and not accept any partially edited value.
	 */
	@Override
	public void cancelCellEditing() {
		fireEditingCanceled();
	}

	/**
	 * Returns the value contained in the editor.
	
	 * @return Object	Value contained in the editor. */
	@Override
	public Object getCellEditorValue() {
		return getSelectedItem();//new Integer(this.getValue());
	}

	/**
	 * Checks if the cell is editable.
	 * @param arg0 			The event the editor should use to consider whether to begin editing or not.
	
	 * @return boolean		True if the cell is editable. */
	@Override
	public boolean isCellEditable(EventObject arg0) {
		return true;
	}

	/**
	 * Removes a listener from the list.
	 * @param arg0 	Cell editor listener.
	 */
	@Override
	public void removeCellEditorListener(CellEditorListener arg0) {
		listeners.removeElement(arg0);
		
	}

	/**
	 * Checks if the editing cell should be selected.
	 * @param arg0 		The event the editor should use to start editing.
	
	 * @return boolean 	True if the editing cell should be selected. */
	@Override
	public boolean shouldSelectCell(EventObject arg0) {
		return true;
	}

	/**
	 * Tells the editor to stop editing and accept any partially edited value as the value of the editor.
	
	 * @return boolean 	True if editing was stopped. */
	@Override
	public boolean stopCellEditing() {
		fireEditingStopped();
		return true;
	}

	/**
	 * Sets an intial value for the editor.
	 * @param arg0 				JTable that is asking the editor to edit.
	 * @param arg1 				Value of the cell to be edited.
	 * @param arg2 				True if the cell is to be rendered with highlighting.
	 * @param arg3 				Row of the cell being edited.
	 * @param arg4 				Column of the cell being edited.
	
	 * @return Component 		The component for editing. */
	@Override
	public Component getTableCellEditorComponent(JTable arg0, Object arg1,
			boolean arg2, int arg3, int arg4) {
		//setValue(((Double)arg1).intValue());
		logger.debug("Value coming through " + arg1);
		arg0.setRowSelectionInterval(arg3, arg3);
		arg0.setColumnSelectionInterval(arg4, arg4);
		//originalValue = getValue();
		return this;
	}

	/**
	 * Informs the listeners that editing has been canceled.
	 */
	protected void fireEditingCanceled() {
		//setValue(originalValue);
		ChangeEvent ce = new ChangeEvent(this);
		for (int i = listeners.size(); i >= 0; i--) {
			((CellEditorListener)listeners.elementAt(i)).editingCanceled(ce);
		}
	}
	/**
	 * Informs the listeners that editing has been stopped.
	 */
	protected void fireEditingStopped() {
		ChangeEvent ce = new ChangeEvent(this);
		for (int i = listeners.size() - 1; i >= 0; i--) {
			((CellEditorListener)listeners.elementAt(i)).editingStopped(ce);
		}
	}
}
