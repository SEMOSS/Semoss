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
	JRadioButton irrRdBtn;
	boolean isSerOpt;
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		
		if (actionevent.getSource().equals(proRdBtn)&& proRdBtn.isSelected())
		{
			roiRdBtn.setSelected(!proRdBtn.isSelected());
			if(isSerOpt)
				bkeRdBtn.setSelected(!proRdBtn.isSelected());
			else
				irrRdBtn.setSelected(!proRdBtn.isSelected());
		}
		else if (actionevent.getSource().equals(proRdBtn)&& !proRdBtn.isSelected())
		{
			proRdBtn.setSelected(true);
		}
		else if (actionevent.getSource().equals(roiRdBtn)&& roiRdBtn.isSelected())
		{
			proRdBtn.setSelected(!roiRdBtn.isSelected());
			if(isSerOpt)
				bkeRdBtn.setSelected(!roiRdBtn.isSelected());
			else
				irrRdBtn.setSelected(!roiRdBtn.isSelected());
		}
		else if (actionevent.getSource().equals(roiRdBtn)&& !roiRdBtn.isSelected())
		{
			roiRdBtn.setSelected(true);
		}
		else if (isSerOpt&&actionevent.getSource().equals(bkeRdBtn)&& bkeRdBtn.isSelected())
		{
			proRdBtn.setSelected(!bkeRdBtn.isSelected());
			roiRdBtn.setSelected(!bkeRdBtn.isSelected());
		}
		else if (isSerOpt&&actionevent.getSource().equals(bkeRdBtn)&& !bkeRdBtn.isSelected())
		{
			bkeRdBtn.setSelected(true);
		}
		else if (!isSerOpt&&actionevent.getSource().equals(irrRdBtn)&& irrRdBtn.isSelected())
		{
			proRdBtn.setSelected(!irrRdBtn.isSelected());
			roiRdBtn.setSelected(!irrRdBtn.isSelected());
		}
		else if (!isSerOpt&&actionevent.getSource().equals(irrRdBtn)&& !irrRdBtn.isSelected())
		{
			irrRdBtn.setSelected(true);
		}
		
	}
	
	/**
	 * Method setRadioBtn.
	 * @param pf JRadioButton
	 * @param roi JRadioButton
	 * @param bke JRadioButton
	 */
	public void setSerOptRadioBtn(JRadioButton pf, JRadioButton roi, JRadioButton bke)
	{
		proRdBtn = pf;
		roiRdBtn = roi;
		bkeRdBtn = bke;
		isSerOpt = true;
	}
	
	/**
	 * Method setRadioBtn.
	 * @param pf JRadioButton
	 * @param roi JRadioButton
	 * @param bke JRadioButton
	 */
	public void setSysOptRadioBtn(JRadioButton pf, JRadioButton roi,JRadioButton irr)
	{
		proRdBtn = pf;
		roiRdBtn = roi;
		irrRdBtn = irr;
		isSerOpt = false;
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
