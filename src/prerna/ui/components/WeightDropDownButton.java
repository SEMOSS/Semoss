/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.ui.components;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;

import javax.swing.DefaultListModel;
import javax.swing.DefaultListSelectionModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.event.PopupMenuEvent;
import javax.swing.event.PopupMenuListener;

import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.main.listener.impl.WeightConvertEdgeListListener;
import prerna.ui.main.listener.impl.WeightConvertNodeListListener;

/**
 * This class is used to create the button that allows the weight to be adjusted.
 */
public class WeightDropDownButton  extends JButton {  

	public JPopupMenu popupMenu = new JPopupMenu(); 
	public JDialog dialog = new JDialog();
	public JList edgePropList, nodePropList;
	public JScrollPane edgePane = new JScrollPane();
	public JScrollPane nodePane = new JScrollPane();
	DefaultListModel nodeListModel= new DefaultListModel();
	DefaultListModel edgeListModel= new DefaultListModel();
	GraphPlaySheet playSheet;
	/** 
	 * @param args 
	 */  
	public WeightDropDownButton (String title)
	{
		this.setText(title);
		edgePropList = new JList();
		nodePropList = new JList();
		setListeners();
	}

	public WeightDropDownButton (ImageIcon icon)
	{
		this.setIcon(icon);
		edgePropList = new JList();
		nodePropList = new JList();
		setListeners();
	}

	public void setSelectionMode(int mode)
	{
		edgePropList.setSelectionMode(mode);
		nodePropList.setSelectionMode(mode);
	}
	public void setPlaySheet(GraphPlaySheet gps)
	{
		this.playSheet = gps;
	}


	//method with sizing
	public void setupButton()
	{

		nodePropList = new JList(nodeListModel);
		edgePropList = new JList(edgeListModel);
		nodePropList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		edgePropList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		//made JList deselectable
		nodePropList.setSelectionModel(new DefaultListSelectionModel(){


			@Override
			public void setSelectionInterval(int index0, int index1) {
				if (index0==index1) {
					if (isSelectedIndex(index0)) {
						removeSelectionInterval(index0, index0);
						return;
					}
				}
				super.setSelectionInterval(index0, index1);
			}

			@Override
			public void addSelectionInterval(int index0, int index1) {
				if (index0==index1) {
					if (isSelectedIndex(index0)) {
						removeSelectionInterval(index0, index0);
						return;
					}
					super.addSelectionInterval(index0, index1);
				}
			}

		});
		edgePropList.setSelectionModel(new DefaultListSelectionModel(){


			@Override
			public void setSelectionInterval(int index0, int index1) {
				if (index0==index1) {
					if (isSelectedIndex(index0)) {
						removeSelectionInterval(index0, index0);
						return;
					}
				}
				super.setSelectionInterval(index0, index1);
			}

			@Override
			public void addSelectionInterval(int index0, int index1) {
				if (index0==index1) {
					if (isSelectedIndex(index0)) {
						removeSelectionInterval(index0, index0);
						return;
					}
					super.addSelectionInterval(index0, index1);
				}
			}

		});
		popupMenu=new JPopupMenu(); 
		setListeners();
		popupMenu.setLayout(new GridBagLayout());

		//add node property label and custom pane
		GridBagConstraints c = new GridBagConstraints();
		JLabel nodePropLblb = new JLabel("Node Properties");
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 0;
		c.gridy = 0;
		c.insets = new Insets(5,5,0,5);
		popupMenu.add(nodePropLblb, c);

		nodePane.setViewportView(nodePropList);
		nodePane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		nodePane.getVerticalScrollBar().setVisible(true);
		nodePane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		nodePane.setSize(new Dimension(this.getWidth(), 200));


		c = new GridBagConstraints();
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 0;
		c.gridy = 1;
		c.insets = new Insets(5,5,0,5);
		popupMenu.add(nodePane, c);

		//add edge property label and custom pane
		JLabel edgePropLblb = new JLabel("Edge Properties");
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 0;
		c.gridy = 2;
		c.insets = new Insets(5,5,0,5);
		popupMenu.add(edgePropLblb, c);


		edgePane.setViewportView(edgePropList);
		edgePane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		edgePane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		edgePane.setPreferredSize(new Dimension(this.getWidth(), 200));
		

		c = new GridBagConstraints();
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 0;
		c.gridy = 3;
		c.insets = new Insets(5,5,0,5);
		popupMenu.add(edgePane, c);
	}

	public void setupLists(ArrayList nodePropArrayList, ArrayList edgePropArrayList)
	{
		WeightConvertEdgeListListener edgeConvertListener = new WeightConvertEdgeListListener();
		edgeConvertListener.setPlaySheet(playSheet);
		edgeConvertListener.setButtonMenu(this);
		WeightConvertNodeListListener nodeConvertListener = new WeightConvertNodeListListener();
		nodeConvertListener.setPlaySheet(playSheet);
		nodeConvertListener.setButtonMenu(this);
		nodePropList.addListSelectionListener(nodeConvertListener);
		edgePropList.addListSelectionListener(edgeConvertListener);
		setListeners();

		//add new properties if they exist
		for (int i=0;i<nodePropArrayList.size();i++)
		{
			if(!nodeListModel.contains(nodePropArrayList.get(i)))
			{
				nodeListModel.addElement(nodePropArrayList.get(i));
			}
		}
		for (int i=0;i<edgePropArrayList.size();i++)
		{
			if(!edgeListModel.contains(edgePropArrayList.get(i)))
			{
				edgeListModel.addElement(edgePropArrayList.get(i));
			}
		}

		//check which ones need to be removed
		ArrayList nodeRemoveList = new ArrayList();
		ArrayList edgeRemoveList = new ArrayList();
		for (int i=0;i<nodeListModel.size();i++)
		{
			if(!nodePropArrayList.contains(nodeListModel.elementAt(i)))
			{
				nodeRemoveList.add(nodeListModel.elementAt(i));
			}
		}
		for (int i=0;i<edgeListModel.size();i++)
		{
			if(!edgePropArrayList.contains(edgeListModel.elementAt(i)+""))
			{
				edgeRemoveList.add(edgeListModel.elementAt(i));
			}
		}

		//finally remove them from listModel
		for (int i=0;i<nodeRemoveList.size();i++)
		{
			String prop = (String) nodePropList.getSelectedValue();
			if(prop!=null && prop.equals(nodeRemoveList.get(i)))
			{
				nodePropList.clearSelection();
			}
			nodeListModel.removeElement(nodeRemoveList.get(i));

		}
		for (int i=0;i<edgeRemoveList.size();i++)
		{
			String prop = (String) edgePropList.getSelectedValue();
			if(prop!=null && prop.equals(edgeRemoveList.get(i)))
			{
				edgePropList.clearSelection();
			}
			edgeListModel.removeElement(edgeRemoveList.get(i));
		}

		//sort elements in ListTable
		String[] modelArray = new String[nodeListModel.size()];
		for (int i=0;i<nodeListModel.size();i++){
			modelArray[i] = (String)nodeListModel.getElementAt(i);
		}
		String prop = (String) nodePropList.getSelectedValue();
		Arrays.sort(modelArray);   // sort the array (this step uses the compareTo method)
		nodeListModel.clear();     // empty the model
		for (String x : modelArray)
			nodeListModel.addElement(x);  
		//keep previously selected ones selected
		if (prop!=null)
		{
			nodePropList.setSelectedValue(prop, true);
		}
		
		
		String[] modelArray2 = new String[edgeListModel.size()];
		for (int i=0;i<edgeListModel.size();i++){
			modelArray2[i] = (String)edgeListModel.getElementAt(i);
		}
		prop = (String) edgePropList.getSelectedValue();
		Arrays.sort(modelArray2);   // sort the array (this step uses the compareTo method)
		edgeListModel.clear();     // empty the model
		for (String x : modelArray2)
			edgeListModel.addElement(x); 
		//keep previously selected ones selected
		if (prop!=null)
		{
			edgePropList.setSelectedValue(prop, true);
		}
		
		popupMenu.pack();
		popupMenu.revalidate();
		popupMenu.repaint();
	}

	public void setListeners()
	{
		final JButton button = this;
		this.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent actionEvent) {
				popupMenu.show(button, 0, (button.getPreferredSize()).height/2);
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
