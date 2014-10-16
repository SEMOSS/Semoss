package prerna.ui.components.specific.tap;

import java.awt.GridBagConstraints;

import javax.swing.JCheckBox;

import prerna.ui.main.listener.specific.tap.SystemCheckBoxSelectorListener;

@SuppressWarnings("serial")
public class DHMSMManualSystemSelectPanel extends DHMSMSystemSelectPanel {
	public JCheckBox mhsSpecificCheckBox, ehrCoreCheckBox;
	
	protected void addCheckBoxes() {
		super.addCheckBoxes();
		
		mhsSpecificCheckBox = new JCheckBox("MHS Specific");
		mhsSpecificCheckBox.setName("mhsSpecificCheckBox");
		GridBagConstraints gbc_mhsSpecificCheckBox = new GridBagConstraints();
		gbc_mhsSpecificCheckBox.anchor = GridBagConstraints.WEST;
		gbc_mhsSpecificCheckBox.gridx = 4;
		gbc_mhsSpecificCheckBox.gridy = 1;
		this.add(mhsSpecificCheckBox, gbc_mhsSpecificCheckBox);
		
		ehrCoreCheckBox = new JCheckBox("EHR Core");
		ehrCoreCheckBox.setName("ehrCoreCheckBox");
		GridBagConstraints gbc_ehrCoreCheckBox = new GridBagConstraints();
		gbc_ehrCoreCheckBox.anchor = GridBagConstraints.WEST;
		gbc_ehrCoreCheckBox.gridx = 4;
		gbc_ehrCoreCheckBox.gridy = 2;
		this.add(ehrCoreCheckBox, gbc_ehrCoreCheckBox);
	}
	
	@Override
	protected void addListener() {
		SystemCheckBoxSelectorListener sysCheckBoxListener = new SystemCheckBoxSelectorListener();
		sysCheckBoxListener.setEngine(engine);
		sysCheckBoxListener.setScrollList(sysSelectDropDown);
		sysCheckBoxListener.setCheckBox(allSysCheckBox,recdSysCheckBox, intDHMSMSysCheckBox,notIntDHMSMSysCheckBox,theaterSysCheckBox,garrisonSysCheckBox,lowProbCheckBox, highProbCheckBox,mhsSpecificCheckBox,ehrCoreCheckBox);
		allSysCheckBox.addActionListener(sysCheckBoxListener);
		recdSysCheckBox.addActionListener(sysCheckBoxListener);
		intDHMSMSysCheckBox.addActionListener(sysCheckBoxListener);
		notIntDHMSMSysCheckBox.addActionListener(sysCheckBoxListener);
		theaterSysCheckBox.addActionListener(sysCheckBoxListener);
		garrisonSysCheckBox.addActionListener(sysCheckBoxListener);
		lowProbCheckBox.addActionListener(sysCheckBoxListener);
		highProbCheckBox.addActionListener(sysCheckBoxListener);
		mhsSpecificCheckBox.addActionListener(sysCheckBoxListener);
		ehrCoreCheckBox.addActionListener(sysCheckBoxListener);
	}
	
	@Override
	public void clearList() {
		super.clearList();
		mhsSpecificCheckBox.setSelected(false);
		ehrCoreCheckBox.setSelected(false);
	}
}
