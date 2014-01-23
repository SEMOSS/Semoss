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
package prerna.rdf.main;

import java.util.Vector;

import javax.swing.JFrame;

/**
 */
public class ACTF {
	
	/**
	 * Method main.
	 * @param args String[]
	 */
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
