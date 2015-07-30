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
package prerna.ui.swing.custom;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.ListModel;

import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;

/**
 * This class extends the basic JButton to create a drop down menu for buttons.
 */
public class SelectScrollList  extends JButton {  

	private JList list;
	public JScrollPane pane = new JScrollPane();
	/** 
	
	 * @param title String		Name of the button.
	 */  
	public SelectScrollList (String title)
	{
		this.setText(title);
		list = new JList();
	}
	

	/**
	 * Adds button with specified views, size, and horizontal/vertical scrollbars to the popup menu. 
	 * @param listArray String[]		List of elements to add.
	 */
	public void setupButton(Vector<String> elementList)
	{
		list = new JList(elementList);
	
		pane.setViewportView(list);
		pane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		pane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
	
	}
	
	/**
	 * Adds button with specified views, size, and horizontal/vertical scrollbars to the popup menu. 
	 * @param listArray String[]		List of elements to add.
	 * @param width int					Preferred width for button dimensions.
	 * @param height int				Preferred height for button dimensions.
	 */
	public void setupButton(Vector<String> elementList, int width, int height)
	{
		setupButton(elementList);
		pane.setPreferredSize(new Dimension(width, height));
	}
	
		
	public void resetList(Vector<String> elementList) {
		list = new JList(elementList);

		pane.setViewportView(list);
	}
	
	/**
	 * Set's the table's selection mode.
	 * @param mode int		Specifies single selections, a single contiguous interval, or multiple intervals.
	 */
	public void setSelectionMode(int mode)
	{
		list.setSelectionMode(mode);
	}
	
	public void setVisible(boolean visible)
	{
		list.setVisible(visible);
		pane.setVisible(visible);
	}

	public void selectAll()
	{
		list.setSelectionInterval(0,list.getModel().getSize()-1);
	}
	
	/**
	 * Unselects everything and selects only the specified values
	 * @param listToSelect
	 */
	public void setSelectedValues(List<String> listToSelect) {
	    list.clearSelection();
	    
		//unselect all
		Iterator<String> itr = listToSelect.iterator();
		while(itr.hasNext()) {
		    String val = itr.next();
	        int index = getIndex(list.getModel(), val);
	        if (index >=0) {
	            list.addSelectionInterval(index, index);
	        }
		}
	    list.ensureIndexIsVisible(list.getSelectedIndex());
	}
	
	/**
	 * Removes the specified values from the selection
	 * @param listToDeselect
	 */
	public void deSelectValues(Vector<String> listToDeselect) {
		ListModel model = list.getModel();
		for (int i = 0; i < model.getSize(); i++)
		{
			String value = (String)model.getElementAt(i);
			//if the unselect list doesnt contain the value, then it should be selected
			if(listToDeselect.contains(value))
				list.removeSelectionInterval(i, i);
		}
	}

	private int getIndex(ListModel model, String value) {
	    if (value == null) return -1;
	    for (int i = 0; i < model.getSize(); i++) {
	        if (value.equals(model.getElementAt(i))) return i;
	    }
	    return -1;
	}
	
	public ArrayList<String> getSelectedValues()
	{
		if(list.getSelectedValuesList().isEmpty())
			return new ArrayList<String>();
		return (ArrayList<String>)list.getSelectedValuesList();
	}
	
	public ArrayList<String> getUnselectedValues()
	{
		ArrayList<String> unselectedList = new ArrayList<String>();
		List<String> selectedList = list.getSelectedValuesList();
		ListModel model = list.getModel();
		for (int i = 0; i < model.getSize(); i++)
		{
			String value = (String)model.getElementAt(i);
			if(!selectedList.contains(value))
			{
				unselectedList.add(value);
			}
		}
		return unselectedList;
	}
	
	public void clearSelection()
	{
		list.clearSelection();
	}
}  
