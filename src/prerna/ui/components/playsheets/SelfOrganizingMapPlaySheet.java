package prerna.ui.components.playsheets;

import java.awt.GridBagConstraints;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.unsupervised.som.SOMRoutine;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;

public class SelfOrganizingMapPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(SelfOrganizingMapPlaySheet.class.getName());

	private SOMRoutine alg;
	private String[] columnHeaders;
	
//	//TODO: need to determine optimal parameters across multiple datasets
	private int instanceIndex;
	private int maxIt = 15;
	private double l0 = 0.07;
	private double r0 = 2.0;
	private double tau = 7.5;
	private int gridWidth;
	private int gridLength;
	private List<String> skipAttributes;

	protected JTabbedPane jTab;

	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		alg = new SOMRoutine();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), instanceIndex);
		selectedOptions.put(options.get(1).getName(), r0);
		selectedOptions.put(options.get(2).getName(), l0);
		selectedOptions.put(options.get(3).getName(), tau);
		selectedOptions.put(options.get(4).getName(), maxIt);
		if(this.gridWidth > 0) {
			selectedOptions.put(options.get(5).getName(), this.gridWidth);
		}
		if(this.gridLength > 0){
			selectedOptions.put(options.get(6).getName(), this.gridLength);
		}
		selectedOptions.put(options.get(8).getName(), skipAttributes);
		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAction(alg);
		this.columnHeaders = dataFrame.getColumnHeaders();
	}
	
	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public int getMaxIt() {
		return maxIt;
	}

	public void setMaxIt(int maxIt) {
		this.maxIt = maxIt;
	}

	public double getL0() {
		return l0;
	}

	public void setL0(double l0) {
		this.l0 = l0;
	}

	public double getR0() {
		return r0;
	}

	public void setR0(double r0) {
		this.r0 = r0;
	}

	public double getTau() {
		return tau;
	}

	public void setTau(double tau) {
		this.tau = tau;
	}

	public int getGridWidth() {
		return gridWidth;
	}

	public void setGridWidth(int gridWidth) {
		this.gridWidth = gridWidth;
	}

	public int getGridLength() {
		return this.gridLength;
	}

	public void setGridLength(int gridLength) {
		this.gridLength = gridLength;
	}
	
	public void setSkipAttributes(List<String> skipAttributes) {
		this.skipAttributes = skipAttributes;
	}
	
	@Override
	public String[] getNames() {
		String[] newNames;
		if(skipAttributes == null || (skipAttributes.size() == 0)) {
			newNames = columnHeaders;
		} else {
			newNames = new String[columnHeaders.length - skipAttributes.size()];
			int counter = 0;
			for(String name : columnHeaders) {
				if(!skipAttributes.contains(name)) {
					newNames[counter] = name;
					counter++;
				}
			}
		}
		
		return newNames;
	}
	
	@Override
	public List<Object[]> getTabularData() {
		List<Object[]> allData = new ArrayList<Object[]>();
		Iterator<Object[]> it = dataFrame.iterator(false);
		while(it.hasNext()) {
			allData.add(it.next());
		}
		
		return allData;
	}

	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
	@Override
	public void addPanel() {
		if (jTab == null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if (jTab.getTabCount() > 1)
				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
			addPanelAsTab(count + ". SOM Raw Data");
		}
	}

	public void addPanelAsTab(String tabName) {
		table = new JTable();
		GridScrollPane gsp = null;
		gsp = new GridScrollPane(dataFrame.getColumnHeaders(), dataFrame.getData());
		gsp.addHorizontalScroll();
		jTab.addTab(tabName, gsp);
	}

	public void addScrollPanel(JPanel panel, JComponent obj) {
		JScrollPane scrollPane = new JScrollPane(obj);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);

		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		panel.add(scrollPane, gbc_scrollPane);
	}

	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}

	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}
}
