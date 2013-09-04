package prerna.ui.components;

import java.awt.BorderLayout;  
import java.awt.Dimension;  
import java.awt.event.ActionEvent;  
import java.awt.event.ActionListener;  
import java.util.Vector;

import javax.swing.DefaultListModel;
import javax.swing.JButton;  
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;  
import javax.swing.JLabel;  
import javax.swing.JList;
import javax.swing.JPopupMenu;  
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;  
import javax.swing.event.PopupMenuEvent;  
import javax.swing.event.PopupMenuListener;  

public class MultiSelectDropDown  extends JButton {  

	private JFrame frame = new JFrame("Test");  
	public JPopupMenu popupMenu = new JPopupMenu(); 
	public JDialog dialog = new JDialog();
	public JList list;
	/** 
	 * @param args 
	 */  
	public MultiSelectDropDown (String title)
	{
		this.setText(title);
	}
	public void setupButton(String[] listArray)
	{
JScrollPane pane = new JScrollPane();
		
		list = new JList(listArray);
		list.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

		pane.setViewportView(list);
		pane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		pane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		pane.setPreferredSize(new Dimension((this.getPreferredSize()).width, 300));
		popupMenu.add(pane);  
		
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
	public MultiSelectDropDown() {  
		
	}  

}  