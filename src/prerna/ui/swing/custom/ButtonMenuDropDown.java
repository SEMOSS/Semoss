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
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.event.PopupMenuEvent;
import javax.swing.event.PopupMenuListener;

import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;

/**
 * This class extends the basic JButton to create a drop down menu for buttons.
 */
public class ButtonMenuDropDown  extends JButton {  

	private JFrame frame = new JFrame("Test");  
	public JPopupMenu popupMenu = new JPopupMenu(); 
	public JDialog dialog = new JDialog();
	public JList list;
	public JScrollPane pane = new JScrollPane();
	/** 
	
	 * @param title String		Name of the button.
	 */  
	public ButtonMenuDropDown (String title)
	{
		this.setText(title);
		list = new JList();
		setListeners();
	}
	
	/**
	 *Sets initial values for the drop down.
	 * @param icon ImageIcon	Initial icon that is painted.
	 */
	public ButtonMenuDropDown (ImageIcon icon)
	{
		this.setIcon(icon);
		list = new JList();
		setListeners();
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
		popupMenu.add(pane);
		

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
		popupMenu.add(pane);  
	
	}
	/**
	 * Used to create the current playsheet list dropdown.
	 * Removes all the existing playsheets in the menu and appends the new list.
	 * @param listArray String[]		List of elements to add.
	 * @param width int					Preferred width for button dimensions.
	 * @param height int				Preferred height for button dimensions.
	 */
	public void setupPlaySheetList(String[] listArray, int width, int height)
	{
		popupMenu.removeAll();
		list = new JList(listArray);
				
		pane.setViewportView(list);
		pane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		pane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		pane.setPreferredSize(new Dimension(width, height));
		popupMenu.add(pane);
	
	}
	

	/**
	 * Resets existing button.
	 */
	public void resetButton()
	{
		JScrollPane pane = new JScrollPane();

		list = new JList();
		setListeners();
	}
	
	/**
	 * Sets the listeners.
	 */
	public void setListeners()
	{
		final JButton button = this;
		this.addActionListener(new ActionListener() {
			@Override  
			public void actionPerformed(ActionEvent actionEvent) {  
				popupMenu.show(button, 0, (button.getPreferredSize()).height);  
				button.setEnabled(false);  
			}  
		});
		popupMenu.addPopupMenuListener(new PopupMenuListener() {  

			@Override  
			public void popupMenuWillBecomeVisible(PopupMenuEvent e) {  
			}  

			@Override  
			public void popupMenuWillBecomeInvisible(PopupMenuEvent e) {  
				SwingUtilities.invokeLater(new Runnable() {  
					  
			          @Override  
			          public void run() {  
			            button.setEnabled(true);  
			          }  
			        }); 
			}  

			@Override  
			public void popupMenuCanceled(PopupMenuEvent e) {  
			}  
		});  
	}
}  
