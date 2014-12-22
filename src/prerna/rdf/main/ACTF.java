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
