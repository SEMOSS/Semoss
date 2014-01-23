/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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
