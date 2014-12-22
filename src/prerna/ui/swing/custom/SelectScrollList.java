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
package prerna.ui.swing.custom;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.ListModel;

import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;

/**
 * This class extends the basic JButton to create a drop down menu for buttons.
 */
public class SelectScrollList  extends JButton {  

	private JFrame frame = new JFrame("Test");
	public JDialog dialog = new JDialog();
	public JList list;
	public JScrollPane pane = new JScrollPane();
	/** 
	
	 * @param title String		Name of the button.
	 */  
	public SelectScrollList (String title)
	{
		this.setText(title);
		list = new JList();
	}
	
	public void setVisible(boolean visible)
	{
		list.setVisible(visible);
		pane.setVisible(visible);
	}
	/**
	 * Set's the table's selection mode.
	 * @param mode int		Specifies single selections, a single contiguous interval, or multiple intervals.
	 */
	public void setSelectionMode(int mode)
	{
		list.setSelectionMode(mode);
	}
	/**
	 * Adds button with specified views, size, and horizontal/vertical scrollbars to the popup menu.
	 * @param listArray String[]		List of elements to add.
	 */
	public void setupButton(String[] listArray)
	{
		list = new JList(listArray);

		pane.setViewportView(list);
		pane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		pane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		pane.setPreferredSize(new Dimension((this.getPreferredSize()).width, 300));
	}
	
	/**
	 * Adds button with specified views, size, and horizontal/vertical scrollbars to the popup menu. 
	 * @param listArray String[]		List of elements to add.
	 * @param width int					Preferred width for button dimensions.
	 * @param height int				Preferred height for button dimensions.
	 */
	public void setupButton(String[] listArray, int width, int height)
	{
		list = new JList(listArray);
	
		pane.setViewportView(list);
		pane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		pane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		pane.setPreferredSize(new Dimension(width, height));
	
	}
	/**
	 * Resets existing button.
	 */
	public void resetButton()
	{
		JScrollPane pane = new JScrollPane();
		list = new JList();
	}
	public void selectAll()
	{
		list.setSelectionInterval(0,list.getModel().getSize()-1);
	}
	public void setSelectedValues(Vector<String> listToSelect) {
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
	public void setUnselectedValues(Vector<String> listToUnselect) {
	    list.clearSelection();
		ListModel model = list.getModel();
		for (int i = 0; i < model.getSize(); i++)
		{
			String value = (String)model.getElementAt(i);
			//if the unselect list doesnt contain the value, then it should be selected
			if(!listToUnselect.contains(value))
				list.addSelectionInterval(i, i);
		}
	}
	public int getIndex(ListModel model, String value) {
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
	public void clearList()
	{
		list.clearSelection();
	}
	
}  
