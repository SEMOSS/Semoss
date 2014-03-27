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

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.CentralSysBPActInsertProcessor;
import prerna.ui.components.specific.tap.ServicesAggregationProcessor;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CentralSysBPActInsertListener extends AbstractListener {

	Logger logger = Logger.getLogger(getClass());
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		//get selected threshold values and parse as a double
		JTextField dataObjectThresholdTextField = (JTextField) DIHelper.getInstance().getLocalProp(ConstantsTAP.DATA_OBJECT_THRESHOLD_VALUE_TEXT_BOX);
		String dataObjectThresholdTextValue = dataObjectThresholdTextField.getText();
		Double dataObjectThresholdValue = 0.0;
		JTextField bluThresholdTextField = (JTextField) DIHelper.getInstance().getLocalProp(ConstantsTAP.BLU_THRESHOLD_VALUE_TEXT_BOX);
		String bluThresholdTextValue = bluThresholdTextField.getText();
		Double bluThresholdValue = 0.0;		
		try{
			dataObjectThresholdValue = Double.parseDouble(dataObjectThresholdTextValue);
			bluThresholdValue = Double.parseDouble(bluThresholdTextValue);
			if ((dataObjectThresholdValue > 1.0) || (bluThresholdValue > 1.0) || (dataObjectThresholdValue < 0) || (bluThresholdValue < 0))  {
				throw new IllegalArgumentException("Threshold value must be between 1 and 0.");
			}
		}catch(Exception e){
			if (e instanceof NumberFormatException) {Utility.showError("All text values must be numbers."); }
			else Utility.showError(e.getMessage());
			return;
		}
				
		//send to processor
		logger.info("Inserting System-BP and System-Activity for Central Systems into TAP_Core...");			
		CentralSysBPActInsertProcessor insertProcessor = new CentralSysBPActInsertProcessor(dataObjectThresholdValue, bluThresholdValue);		
		boolean success = insertProcessor.runAggregation();
		if(success)	{
			Utility.showMessage("Finished Aggregation!");
		}
		else {
			String errorMessage = insertProcessor.getErrorMessage();
			Utility.showError(errorMessage);
		}
	}
	
	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
