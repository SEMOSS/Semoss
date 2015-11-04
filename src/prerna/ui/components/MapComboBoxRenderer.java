package prerna.ui.components;

import java.awt.Component;
import java.util.ArrayList;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JList;

public class MapComboBoxRenderer extends ComboboxToolTipRenderer{
	
	public final static String VALUE = "value";
	public final static String KEY = "id";
	
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
	public Component getListCellRendererComponent(JList list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
		if(value != null) {
			JComponent comp = (JComponent) super.getListCellRendererComponent(list, ((Map<String, String>) value).get(VALUE), index, isSelected, cellHasFocus);
			if (-1 < index && null != value && null != tooltips) {
				String ttString = "<html><body style=\"border:0px solid white; box-shadow:1px 1px 1px #000; padding:2px; background-color:white;\">" +
						"<font size=\"3\" color=\"black\"><i>"+ tooltips.get(index)+ "</i></font></body></html>";
				list.setToolTipText(ttString);
			}
			return comp;
		}
		else {
			JComponent comp = (JComponent) super.getListCellRendererComponent(list, "Invalid question in perspective", 0, true, true);
			return comp;
		}
	}

	/**
	 * Sets tooltips.
	 * @param tooltips ArrayList
	 */
	public void setTooltips(ArrayList tooltips) {
		this.tooltips = tooltips;
	}
}
