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
