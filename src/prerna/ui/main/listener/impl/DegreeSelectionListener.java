package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.MachineLearningModulePlaySheet;

public class DegreeSelectionListener extends AbstractListener{
	static final Logger logger = LogManager.getLogger(ClassificationSelectionListener.class.getName());
	
	//shows or hides the JTextField for inputting a number of clusters depending on if the JComboBox is set to manually select
	private MachineLearningModulePlaySheet playSheet;
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> bx = (JComboBox<String>)e.getSource();
		String selection = bx.getSelectedItem() + "";
		//if manually select need to show text field, otherwise hide it
		if(selection.equals("Polynomial")) {
			//playSheet.setKernelLabel("Degree");
			
			playSheet.showPolynomialDegreeTextField(true);
			//playSheet.setPolynomialDegreeTextField("2");
			
			//playSheet.showKappaTextField(false);
			//playSheet.setKappaTextField("1");
			
			//playSheet.showConstantTextField(true);
			//playSheet.setConstantTextField("1");
			//playSheet.showConstantLabel(true);
			
		} else if(selection.equals("Sigmoid")) {
			//playSheet.setKernelLabel("Kappa");
			
			playSheet.showPolynomialDegreeTextField(false);
			//playSheet.setPolynomialDegreeTextField("2");
			
			//playSheet.showKappaTextField(true);
			//playSheet.setKappaTextField("1");
			
			//playSheet.showConstantTextField(true);
			//playSheet.setConstantTextField("1");
			//playSheet.showConstantLabel(true);
		} else {
			playSheet.showPolynomialDegreeTextField(false);
			//playSheet.setPolynomialDegreeTextField("2");
			
			//playSheet.showKappaTextField(false);
			//playSheet.setKappaTextField("1");
			
			//playSheet.showConstantTextField(false);
			//playSheet.setConstantTextField("1");
			//playSheet.showConstantLabel(false);
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.playSheet = (MachineLearningModulePlaySheet) view;
	}
}
