package prerna.ui.components.playsheets;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.weka.impl.WekaAprioriAlgorithm;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.ui.main.listener.impl.JTableExcelExportListener;
import prerna.util.Utility;

public class WekaAprioriPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriPlaySheet.class.getName());

	private WekaAprioriAlgorithm alg;

	private int numRules = -1; // number of rules to output
	private double confPer = -1; // min confidence lvl (percentage)
	private double minSupport = -1; // min number of rows required for rule (percentage of total rows of data);
	
	@Override
	public void createData() {
		generateData();
		runAlgorithm();
	}

	private void generateData() {
		if(query!=null) {
			list = new ArrayList<Object[]>();

			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			names = sjsw.getVariables();
			int length = names.length;
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				Object[] row = new Object[length];
				int i = 0;
				for(; i < length; i++) {
					row[i] = sjss.getVar(names[i]);
				}
				list.add(row);
			}
		}
	}

	public void runAlgorithm() {
		LOGGER.info("Creating apriori algorithm for instance: " + names[0]);
		alg = new WekaAprioriAlgorithm(list, names);
		if(numRules != -1) {
			alg.setNumRules(numRules);
		}
		if(confPer != -1) {
			alg.setConfPer(confPer);
		}
		if(minSupport != -1) {
			alg.setMinSupport(minSupport);
		}
		try {
			alg.execute();
			alg.generateDecisionRuleTable();
			
			list = alg.getRetList();
			names = alg.getRetNames();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}

	@Override
	public void addPanel()
	{
		if(jTab==null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Apriori");
		}
	}

	public void addPanelAsTab(String tabName) {
		//	setWindow();
		try {
			table = new JTable();

			//Add Excel export popup menu and menuitem
			JPopupMenu popupMenu = new JPopupMenu();
			JMenuItem menuItemAdd = new JMenuItem("Export to Excel");
			String questionTitle = this.getTitle();
			menuItemAdd.addActionListener(new JTableExcelExportListener(table, questionTitle));
			popupMenu.add(menuItemAdd);
			table.setComponentPopupMenu(popupMenu);

			GridPlaySheetListener gridPSListener = new GridPlaySheetListener();
			LOGGER.debug("Created the table");
			this.addInternalFrameListener(gridPSListener);
			LOGGER.debug("Added the internal frame listener ");
			//table.setAutoCreateRowSorter(true);

			JPanel panel = new JPanel();
			panel.add(table);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[]{0, 0};
			gbl_mainPanel.rowHeights = new int[]{0, 0};
			gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			panel.setLayout(gbl_mainPanel);

			addScrollPanel(panel, table);

			jTab.addTab(tabName, panel);

			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			LOGGER.debug("Added new Outlier Sheet");

		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
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
	
	public int getNumRules() {
		return numRules;
	}

	public void setNumRules(int numRules) {
		this.numRules = numRules;
	}

	public double getConfPer() {
		return confPer;
	}

	public void setConfPer(double confPer) {
		this.confPer = confPer;
	}

	public double getMinSupport() {
		return minSupport;
	}

	public void setMinSupport(double minSupport) {
		this.minSupport = minSupport;
	}

	protected JTabbedPane jTab;

	public WekaAprioriPlaySheet() {
		super();
	}

}
