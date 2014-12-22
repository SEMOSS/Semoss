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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyVetoException;
import java.util.ArrayList;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.ui.components.WrapLayout;
import prerna.ui.swing.custom.CustomButton;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.hp.hpl.jena.query.ResultSet;

/**
 */
public class RegExplorerPlaySheet extends AbstractRDFPlaySheet{


	private static final Logger logger = LogManager.getLogger(RegExplorerPlaySheet.class.getName());
	String depVarText;
	ArrayList<String> indepVarTextList;
	ArrayList<Double> indepVarMedianValues;
	ArrayList<Double> indepVarSlopeValues;
	ArrayList<JPanel> indepPanelList = new ArrayList<JPanel>();
	JPanel depPanel;
	
	JPanel regPanel;
	
	protected ResultSet rs = null;

	/**
	 * Method createView.
	 */
	@Override
	public void createView() {
		addPanel();
	}
	
	
	/**
	 * Method setRs.
	 * @param rs ResultSet
	 */
	public void setRs(ResultSet rs) {
		this.rs = rs;
	}
	
	/**
	 * Method getVariable.
	 * @param varName String
	 * @param sjss SesameJenaSelectStatement
	
	 * @return Object */
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}

	/**
	 * Method refineView.
	 */
	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		// this is easy
		// just use the filter to not show stuff I dont need to show
		// but this also means I need to create the vertex filter data etc. 
		//
		
	}
	/**
	 * Method overlayView.
	 */
	@Override
	public void overlayView() {
		//Fill
	}
	

	/**
	 * Method addPanel.
	 */
	public void addPanel()
	{
		try{
			
			regPanel = new JPanel();
			this.setContentPane(regPanel);
			regPanel.setLayout(new GridBagLayout());

			JLabel regressorLabel = new JLabel("Regressors:");
			regressorLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
			GridBagConstraints gbc_regressorLabel = new GridBagConstraints();
			gbc_regressorLabel.insets= new Insets(0,0,50,0);
			gbc_regressorLabel.anchor= GridBagConstraints.WEST;
			//gbc_regressorLabel.fill= GridBagConstraints.BOTH;
			gbc_regressorLabel.gridx = 0;
			gbc_regressorLabel.gridy = 0;
			regPanel.add(regressorLabel,gbc_regressorLabel);
			
			JPanel indepPanel = new JPanel();
			indepPanel.setLayout(new WrapLayout());
			for(int i=0;i<indepVarTextList.size();i++)
			{
				JPanel panel = new JPanel();
				panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
				panel.setBorder(new EmptyBorder(10, 10, 10, 10) );
				
				JLabel label = new JLabel(indepVarTextList.get(i));
				label.setAlignmentX(CENTER_ALIGNMENT);
				panel.add(label);

				String toPut = indepVarSlopeValues.get(i).toString();
				int indexOfDec = toPut.indexOf(".");
				if(indexOfDec>0&&toPut.length()>(indexOfDec+4))
					toPut=toPut.substring(0,indexOfDec+4);				
				JLabel labelRegCoeff = new JLabel("Reg Coeff: "+toPut);
				labelRegCoeff.setAlignmentX(CENTER_ALIGNMENT);
				panel.add(labelRegCoeff);
				
				toPut = indepVarMedianValues.get(i).toString();
				indexOfDec = toPut.indexOf(".");
				if(indexOfDec>0&&toPut.length()>(indexOfDec+4))
					toPut=toPut.substring(0,indexOfDec+4);
				JTextField field = new JTextField(toPut);
				field.setAlignmentX(CENTER_ALIGNMENT);
				panel.add(field);
				indepPanelList.add(panel);
				indepPanel.add(panel);			
			}
			GridBagConstraints gbc_indepPanel = new GridBagConstraints();
			gbc_indepPanel.insets= new Insets(0,0,50,0);
			gbc_indepPanel.anchor= GridBagConstraints.NORTH;
			gbc_indepPanel.fill= GridBagConstraints.BOTH;
			gbc_indepPanel.gridx = 0;
			gbc_indepPanel.gridy = 1;
			regPanel.add(indepPanel,gbc_indepPanel);
			indepPanel.repaint();
			regPanel.repaint();

			
			
			JLabel depVarLabel = new JLabel("Dependent Variable Prediction:");
			depVarLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
			GridBagConstraints gbc_depVarLabel = new GridBagConstraints();
			gbc_depVarLabel.insets= new Insets(0,0,50,0);
			gbc_depVarLabel.anchor= GridBagConstraints.NORTH;
			gbc_depVarLabel.fill= GridBagConstraints.BOTH;
			gbc_depVarLabel.gridx = 0;
			gbc_depVarLabel.gridy = 2;
			regPanel.add(depVarLabel,gbc_depVarLabel);
			
			depPanel = new JPanel();
			depPanel.setLayout(new BoxLayout(depPanel, BoxLayout.Y_AXIS));
			
			JLabel deplabel = new JLabel(depVarText);
			deplabel.setAlignmentX(CENTER_ALIGNMENT);
			depPanel.add(deplabel);
			
			Double depValue =0.0;//constVal;
			for(int i=0;i<indepVarSlopeValues.size();i++)
			{
				Double medValue = Double.parseDouble(((JTextField)indepPanelList.get(i).getComponent(2)).getText());
				depValue+=indepVarSlopeValues.get(i)*medValue;
			}
			JTextField depfield = new JTextField(depValue.toString().substring(0,8));
			depfield.setAlignmentX(CENTER_ALIGNMENT);
			depPanel.add(depfield);
			//depPanel.setPreferredSize(new Dimension(100,100));

			GridBagConstraints gbc_depPanel = new GridBagConstraints();
			gbc_depPanel.gridx = 0;
			gbc_depPanel.gridy = 3;
			regPanel.add(depPanel,gbc_depPanel);
			
			
			JButton recalculateBtn = new CustomButton("Recalculate");
			recalculateBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
			recalculateBtn.setAlignmentX(CENTER_ALIGNMENT);

			GridBagConstraints gbc_recalculateBtn = new GridBagConstraints();
			gbc_recalculateBtn.gridx = 0;
			gbc_recalculateBtn.gridy = 4;
			//gbc_recalculateBtn.fill = GridBagConstraints.HORIZONTAL;
			recalculateBtn.setMinimumSize(new Dimension(100,30));
			regPanel.add(recalculateBtn,gbc_recalculateBtn);
			
			recalculateBtn.addActionListener(new ActionListener() {
	            public void actionPerformed(ActionEvent e) {
	    			try{
		        		Double depValue = 0.0;
		        		for(int i=0;i<indepVarSlopeValues.size();i++)
		        		{
		        			Double medValue = Double.parseDouble(((JTextField)indepPanelList.get(i).getComponent(2)).getText());
		        			depValue+=indepVarSlopeValues.get(i)*medValue;
		        		}
		        		((JTextField)(depPanel.getComponent(1))).setText(depValue.toString());
	    			}
	    			catch(RuntimeException ex){
	    				displayCheckBoxError();
	    			}
	            }
	        });
			CSSApplication css = new CSSApplication(recalculateBtn,".standardButton");

			
			
			
			pane.add(this);
			this.setMaximum(true);
//			this.setExtendedState(Frame.MAXIMIZED_BOTH);
			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			this.setMaximum(true);
//			this.setExtendedState(Frame.MAXIMIZED_BOTH);
			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
	
			logger.debug("Added the main pane");
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Method setValues.
	 * @param dependentVar String
	 * @param independentVarList ArrayList<String>
	 * @param independentVarMedianValues ArrayList<Double>
	 * @param independentVarSlopeValues ArrayList<Double>
	 */
	public void setValues(String dependentVar,ArrayList<String> independentVarList,ArrayList<Double> independentVarMedianValues,ArrayList<Double> independentVarSlopeValues)
	{
		depVarText = dependentVar;
		indepVarTextList = independentVarList;
		indepVarMedianValues =independentVarMedianValues;
		indepVarSlopeValues = independentVarSlopeValues;
	}
	
	/**
	 * Method displayCheckBoxError.
	 * 
	 */
	
	public void displayCheckBoxError(){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, "One of the elements is not a number.", "Error", JOptionPane.ERROR_MESSAGE);
		
	}


	@Override
	public void createData() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public Object getData() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}
}
