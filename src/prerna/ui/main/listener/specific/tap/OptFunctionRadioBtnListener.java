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

		
		if (((JRadioButton)actionevent.getSource()).getName().equals(proRdBtn.getName()) && proRdBtn.isSelected())
		{
			roiRdBtn.setSelected(!proRdBtn.isSelected());
			if(isSerOpt)
				bkeRdBtn.setSelected(!proRdBtn.isSelected());
			else
				irrRdBtn.setSelected(!proRdBtn.isSelected());
		}
		else if (((JRadioButton)actionevent.getSource()).getName().equals(proRdBtn.getName()) && !proRdBtn.isSelected())
		{
			proRdBtn.setSelected(true);
		}
		else if (((JRadioButton)actionevent.getSource()).getName().equals(roiRdBtn.getName()) && roiRdBtn.isSelected())
		{
			proRdBtn.setSelected(!roiRdBtn.isSelected());
			if(isSerOpt)
				bkeRdBtn.setSelected(!roiRdBtn.isSelected());
			else
				irrRdBtn.setSelected(!roiRdBtn.isSelected());
		}
		else if (((JRadioButton)actionevent.getSource()).getName().equals(roiRdBtn.getName()) && !roiRdBtn.isSelected())
		{
			roiRdBtn.setSelected(true);
		}
		else if (isSerOpt && ((JRadioButton)actionevent.getSource()).getName().equals(bkeRdBtn.getName()) && bkeRdBtn.isSelected())
		{
			proRdBtn.setSelected(!bkeRdBtn.isSelected());
			roiRdBtn.setSelected(!bkeRdBtn.isSelected());
		}
		else if (isSerOpt && ((JRadioButton)actionevent.getSource()).getName().equals(bkeRdBtn.getName()) && !bkeRdBtn.isSelected())
		{
			bkeRdBtn.setSelected(true);
		}
		else if (!isSerOpt && ((JRadioButton)actionevent.getSource()).getName().equals(irrRdBtn.getName()) && irrRdBtn.isSelected())
		{
			proRdBtn.setSelected(!irrRdBtn.isSelected());
			roiRdBtn.setSelected(!irrRdBtn.isSelected());
		}
		else if (!isSerOpt && ((JRadioButton)actionevent.getSource()).getName().equals(irrRdBtn.getName()) && !irrRdBtn.isSelected())
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
