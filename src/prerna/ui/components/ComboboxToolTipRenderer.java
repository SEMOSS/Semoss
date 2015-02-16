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

import java.awt.Component;
import java.util.ArrayList;

import javax.swing.DefaultListCellRenderer;
import javax.swing.JComponent;
import javax.swing.JList;
/**
 * This class renders the tooltips for the combo box.
 */
public class ComboboxToolTipRenderer extends DefaultListCellRenderer {
    ArrayList tooltips;

    /**
     * Returns a component that has been configured to display the specified value. 
     * In this case, this is the tooltip.
     * @param list JList to be painted.
     * @param value The value returned by list.getModel().getElementAt(index).
     * @param index The cell's index.
     * @param isSelected True if the specified cell is selected.
     * @param cellHasFocus True if cell has focus.
    
     * @return Component	The set tooltip.  */
    @Override
    public Component getListCellRendererComponent(JList list, Object value,
                        int index, boolean isSelected, boolean cellHasFocus) {

        JComponent comp = (JComponent) super.getListCellRendererComponent(list,
                value, index, isSelected, cellHasFocus);

        if (-1 < index && null != value && null != tooltips) {
        			String ttString = "<html><body style=\"border:0px solid white; box-shadow:1px 1px 1px #000; padding:2px; background-color:white;\">" +
        					"<font size=\"3\" color=\"black\"><i>"+ tooltips.get(index)+ "</i></font></body></html>";
                    list.setToolTipText(ttString);
                }
        return comp;
    }

    /**
     * Sets tooltips.
     * @param tooltips ArrayList
     */
    public void setTooltips(ArrayList tooltips) {
        this.tooltips = tooltips;
    }
}
