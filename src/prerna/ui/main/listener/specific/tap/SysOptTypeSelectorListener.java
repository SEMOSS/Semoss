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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import prerna.ui.main.listener.impl.AbstractListener;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class SysOptTypeSelectorListener extends AbstractListener {
	
	JTextField sysSelectQueryField;
	JRadioButton theaterSysButton, garrisonSysButton, allSysButton;
	JRadioButton lowProbButton, medProbButton, highProbButton, allProbButton;
	

	/**
	 * Determines if the user has selected HSD, HSS, FHP check box's in MHS TAP to include functional areas to include in RFP report
	 * Will populate sourceSelectPanel to show all capabilities for the functional area's selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		String query = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}@THEATERGARRISON@ @PROB@}";
		if(theaterSysButton.isSelected())
			query=query.replace("@THEATERGARRISON@","{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> 'Theater'}");
		else if(garrisonSysButton.isSelected())
			query=query.replace("@THEATERGARRISON@","{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> 'Garrison'}");
		else
			query=query.replace("@THEATERGARRISON@","");
		if(lowProbButton.isSelected())
			query=query.replace("@PROB@","{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'Low'}");
		else if(medProbButton.isSelected())
			query=query.replace("@PROB@","{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'Medium'}");
		else if(highProbButton.isSelected())
			query=query.replace("@PROB@","{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}");
		else
			query=query.replace("@PROB@","");
		sysSelectQueryField.setText(query);
	}

	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
	
	public void setTheaterGarrisonBtn(JRadioButton theaterSysButton,JRadioButton garrisonSysButton,JRadioButton allSysButton)
	{
		this.theaterSysButton = theaterSysButton;
		this.garrisonSysButton = garrisonSysButton;
		this.allSysButton = allSysButton;
	}
	public void setProbBtn(JRadioButton lowProbButton,JRadioButton medProbButton,JRadioButton highProbButton, JRadioButton allProbButton)
	{
		this.lowProbButton = lowProbButton;
		this.medProbButton = medProbButton;
		this.highProbButton = highProbButton;
		this.allProbButton = allProbButton;
	}
	public void setQueryTextField(JTextField sysSelectQueryField)
	{
		this.sysSelectQueryField = sysSelectQueryField;
	}
}
