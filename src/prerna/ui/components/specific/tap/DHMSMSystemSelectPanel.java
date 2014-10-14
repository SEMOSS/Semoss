package prerna.ui.components.specific.tap;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.ListSelectionModel;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.specific.tap.CheckBoxSelectorListener;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class DHMSMSystemSelectPanel extends JPanel {
	public IEngine engine;
	String header = "Select Systems:";
	public JCheckBox allSysButton, recdSysButton, intDHMSMSysButton, notIntDHMSMSysButton;
	public JCheckBox lowProbButton, highProbButton, theaterSysButton, garrisonSysButton;
	public SelectScrollList sysSelectDropDown;
	
	public DHMSMSystemSelectPanel()
	{
	}
	public void setHeader(String header) {
		this.header = header;
	}
	public void addElements()
	{
		this.removeAll();
		GridBagLayout gbl_systemSelectPanel = new GridBagLayout();
		gbl_systemSelectPanel.columnWidths = new int[]{0, 0, 0, 0,};
		gbl_systemSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gbl_systemSelectPanel);

		JLabel lblSystemSelectHeader = new JLabel(header);
		lblSystemSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblSystemSelectHeader = new GridBagConstraints();
		gbc_lblSystemSelectHeader.gridwidth = 4;
		gbc_lblSystemSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblSystemSelectHeader.insets = new Insets(10, 0, 5, 5);
		gbc_lblSystemSelectHeader.gridx = 0;
		gbc_lblSystemSelectHeader.gridy = 0;
		this.add(lblSystemSelectHeader, gbc_lblSystemSelectHeader);
		
		allSysButton = new JCheckBox("All");
		allSysButton.setName("allSysButton");
		GridBagConstraints gbc_allSysButton = new GridBagConstraints();
		gbc_allSysButton.anchor = GridBagConstraints.WEST;
		gbc_allSysButton.gridx = 0;
		gbc_allSysButton.gridy = 1;
		this.add(allSysButton, gbc_allSysButton);
		
		recdSysButton = new JCheckBox("Rec'd");
		recdSysButton.setName("recdSysButton");
		GridBagConstraints gbc_recdSysButton = new GridBagConstraints();
		gbc_recdSysButton.anchor = GridBagConstraints.WEST;
		gbc_recdSysButton.gridx = 1;
		gbc_recdSysButton.gridy = 1;
		this.add(recdSysButton, gbc_recdSysButton);

		intDHMSMSysButton = new JCheckBox("Interface");
		intDHMSMSysButton.setName("intDHMSMSysButton");
		GridBagConstraints gbc_intDHMSMSysButton = new GridBagConstraints();
		gbc_intDHMSMSysButton.anchor = GridBagConstraints.WEST;
		//gbc_intDHMSMSysButton.gridwidth = 2;
		gbc_intDHMSMSysButton.gridx = 2;
		gbc_intDHMSMSysButton.gridy = 1;
		this.add(intDHMSMSysButton, gbc_intDHMSMSysButton);
		
		notIntDHMSMSysButton = new JCheckBox("No Interface");
		notIntDHMSMSysButton.setName("notIntDHMSMSysButton");
		GridBagConstraints gbc_notIntDHMSMSysButton = new GridBagConstraints();
		gbc_notIntDHMSMSysButton.anchor = GridBagConstraints.WEST;
		gbc_notIntDHMSMSysButton.gridx = 3;
		gbc_notIntDHMSMSysButton.gridy = 1;
		this.add(notIntDHMSMSysButton, gbc_notIntDHMSMSysButton);
		
		lowProbButton = new JCheckBox("Low");
		lowProbButton.setName("lowProbButton");
		GridBagConstraints gbc_lowProbButton = new GridBagConstraints();
		gbc_lowProbButton.anchor = GridBagConstraints.WEST;
		gbc_lowProbButton.gridx = 0;
		gbc_lowProbButton.gridy = 2;
		this.add(lowProbButton, gbc_lowProbButton);
		
		highProbButton = new JCheckBox("High");
		highProbButton.setName("highProbButton");
		GridBagConstraints gbc_highProbButton = new GridBagConstraints();
		gbc_highProbButton.anchor = GridBagConstraints.WEST;
		gbc_highProbButton.gridx = 1;
		gbc_highProbButton.gridy = 2;
		this.add(highProbButton, gbc_highProbButton);		

		theaterSysButton = new JCheckBox("Theater");
		theaterSysButton.setName("theaterSysButton");
		GridBagConstraints gbc_theaterSysButton = new GridBagConstraints();
		gbc_theaterSysButton.anchor = GridBagConstraints.WEST;
		gbc_theaterSysButton.gridx = 2;
		gbc_theaterSysButton.gridy = 2;
		this.add(theaterSysButton, gbc_theaterSysButton);
		
		garrisonSysButton = new JCheckBox("Garrison");
		garrisonSysButton.setName("garrisonSysButton");
		GridBagConstraints gbc_garrisonSysButton = new GridBagConstraints();
		gbc_garrisonSysButton.anchor = GridBagConstraints.WEST;
		gbc_garrisonSysButton.gridx = 3;
		gbc_garrisonSysButton.gridy = 2;
		this.add(garrisonSysButton, gbc_garrisonSysButton);
		
		sysSelectDropDown = new SelectScrollList("Select Individual Systems");
		sysSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_sysSelectDropDown = new GridBagConstraints();
		gbc_sysSelectDropDown.gridwidth = 4;
		gbc_sysSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_sysSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysSelectDropDown.gridx = 0;
		gbc_sysSelectDropDown.gridy = 3;
		this.add(sysSelectDropDown.pane, gbc_sysSelectDropDown);
		
		String[] sysArray = makeListFromQuery("System","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}} ");
		sysSelectDropDown.setupButton(sysArray,40,120); //need to give list of all systems
		
		CheckBoxSelectorListener sysCheckBoxListener = new CheckBoxSelectorListener();
		sysCheckBoxListener.setEngine(engine);
		sysCheckBoxListener.setScrollList(sysSelectDropDown);
		sysCheckBoxListener.setCheckBox(allSysButton,recdSysButton, intDHMSMSysButton,notIntDHMSMSysButton,theaterSysButton,garrisonSysButton,lowProbButton, highProbButton);
		allSysButton.addActionListener(sysCheckBoxListener);
		recdSysButton.addActionListener(sysCheckBoxListener);
		intDHMSMSysButton.addActionListener(sysCheckBoxListener);
		notIntDHMSMSysButton.addActionListener(sysCheckBoxListener);
		theaterSysButton.addActionListener(sysCheckBoxListener);
		garrisonSysButton.addActionListener(sysCheckBoxListener);
		lowProbButton.addActionListener(sysCheckBoxListener);
		highProbButton.addActionListener(sysCheckBoxListener);
	}
	
	public String[] makeListFromQuery(String type, String query)
	{
		engine = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		EntityFiller filler = new EntityFiller();
		filler.engineName = engine.getEngineName();
		filler.type = "Capability";
		filler.setExternalQuery(query);
		filler.run();
		Vector<String> names = filler.nameVector;
		String[] listArray=new String[names.size()];
		for (int i = 0;i<names.size();i++)
		{
			listArray[i]=(String) names.get(i);
		}
		return listArray;
	}
	
	public void clearList() {
		recdSysButton.setSelected(false);
		intDHMSMSysButton.setSelected(false);
		notIntDHMSMSysButton.setSelected(false);
		allSysButton.setSelected(false);
		recdSysButton.setSelected(false);
		intDHMSMSysButton.setSelected(false);
		lowProbButton.setSelected(false);
		highProbButton.setSelected(false);
		theaterSysButton.setSelected(false);
		garrisonSysButton.setSelected(false);
		sysSelectDropDown.clearList();
	}
	
	public ArrayList<String> getSelectedSystems()
	{
		return sysSelectDropDown.getSelectedValues();
	}
}
