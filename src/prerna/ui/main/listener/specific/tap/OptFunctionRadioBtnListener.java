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

import prerna.ui.components.api.IChakraListener;

/**
 */
public class OptFunctionRadioBtnListener implements IChakraListener {
	
	JRadioButton proRdBtn;
	JRadioButton roiRdBtn;
	JRadioButton bkeRdBtn;
	boolean bkeExists;
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		
		if (actionevent.getSource().equals(proRdBtn)&& proRdBtn.isSelected())
		{
			roiRdBtn.setSelected(!proRdBtn.isSelected());
			if(bkeExists)
				bkeRdBtn.setSelected(!proRdBtn.isSelected());
		}
		else if (actionevent.getSource().equals(proRdBtn)&& !proRdBtn.isSelected())
		{
			proRdBtn.setSelected(true);
		}
		else if (actionevent.getSource().equals(roiRdBtn)&& roiRdBtn.isSelected())
		{
			proRdBtn.setSelected(!roiRdBtn.isSelected());
			if(bkeExists)
				bkeRdBtn.setSelected(!roiRdBtn.isSelected());
		}
		else if (actionevent.getSource().equals(roiRdBtn)&& !roiRdBtn.isSelected())
		{
			roiRdBtn.setSelected(true);
		}
		else if (bkeExists&&actionevent.getSource().equals(bkeRdBtn)&& bkeRdBtn.isSelected())
		{
			proRdBtn.setSelected(!bkeRdBtn.isSelected());
			roiRdBtn.setSelected(!bkeRdBtn.isSelected());
		}
		else if (bkeExists&&actionevent.getSource().equals(bkeRdBtn)&& !bkeRdBtn.isSelected())
		{
			bkeRdBtn.setSelected(true);
		}
		
	}
	
	/**
	 * Method setRadioBtn.
	 * @param pf JRadioButton
	 * @param roi JRadioButton
	 * @param bke JRadioButton
	 */
	public void setRadioBtn(JRadioButton pf, JRadioButton roi, JRadioButton bke)
	{
		proRdBtn = pf;
		roiRdBtn = roi;
		bkeRdBtn = bke;
		bkeExists = true;
	}
	
	/**
	 * Method setRadioBtn.
	 * @param pf JRadioButton
	 * @param roi JRadioButton
	 * @param bke JRadioButton
	 */
	public void setRadioBtn(JRadioButton pf, JRadioButton roi)
	{
		proRdBtn = pf;
		roiRdBtn = roi;
		bkeExists = false;
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
