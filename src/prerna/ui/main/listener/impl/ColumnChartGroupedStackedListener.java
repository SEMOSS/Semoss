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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;

import com.teamdev.jxbrowser.chromium.Browser;

public class ColumnChartGroupedStackedListener implements ActionListener{

	Browser browser;
	
	public void setBrowser(Browser b){
		this.browser = b;
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		JButton button = (JButton) e.getSource();
		if(button.getText().equals("Transition Grouped")){
			System.out.println("transition to grouped");
			browser.executeJavaScript("transitionGrouped();");
			button.setText("Transition Stacked");
		}
		else {
			System.out.println("transition to stacked");
			browser.executeJavaScript("transitionStacked();");
			button.setText("Transition Grouped");
		}
		
	}

}
