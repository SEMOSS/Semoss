package prerna.rdf.main;

import java.util.Vector;

import javax.swing.JFrame;

public class ACTF {
	
	public static void main(String [] args)
	{
		Vector list = new Vector();
		list.add("Cat");
		list.add("Cat2");
		list.add("Cat3");
		list.add("Cat4");
		list.add("Dog");
		
		JFrame frame = new JFrame();
		Java2sAutoTextField fl = new Java2sAutoTextField(list, new Java2sAutoComboBox(list));
		Java2sAutoComboBox bx = new Java2sAutoComboBox(list);
		frame.add(fl);
		frame.add(bx);
		frame.pack();
		frame.setVisible(true);
		
	}

}
