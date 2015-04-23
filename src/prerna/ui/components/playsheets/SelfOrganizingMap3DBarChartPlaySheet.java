package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.unsupervised.som.SelfOrganizingMap;
import prerna.algorithm.learning.unsupervised.som.SelfOrganizingMapGridViewer;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.ui.main.listener.impl.JTableExcelExportListener;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SelfOrganizingMap3DBarChartPlaySheet extends BrowserPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(SelfOrganizingMap3DBarChartPlaySheet.class.getName());

	private SelfOrganizingMap alg;
	
	private Double l0;
	private Double r0;
	private Double tau;
	private Integer maxIt;
	
	private double[][] zAxisGrid;
	
	public SelfOrganizingMap3DBarChartPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/surfaceplot.html";
	}
	
	@Override
	public void createData() {
		generateData();
		runAlgorithm();
		dataHash = processQueryData();
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
		
		long start = System.currentTimeMillis();
		
		LOGGER.info("Creating apriori algorithm for instance: " + names[0]);
		alg = new SelfOrganizingMap(list, names);
		if(l0 != null) {
			alg.setL0(l0);
		}
		if(tau != null) {
			alg.setTau(tau);
		}
		if(r0 != null) {
			alg.setR0(r0);
		}
		if(maxIt != null) {
			alg.setMaxIt(maxIt);
		}
		boolean success = alg.execute();
		if(success == false) {
			Utility.showError("Error occured running SOM Algorithm!");
		} else {
			zAxisGrid = SelfOrganizingMapGridViewer.generateZAxisGridValues(alg.getLength(), alg.getHeight(), alg.getNumInstancesInGrid());
		}
		
		long end = System.currentTimeMillis();

		System.out.println("Time in (s): " + (end-start)/1000);
	}
	
	@Override 
	public Hashtable processQueryData() {
		SelfOrganizingMapPlaySheet tablePS = new SelfOrganizingMapPlaySheet();
		tablePS.setList(list);
		tablePS.setNames(names);
		tablePS.setAlg(alg);
		tablePS.processAlgorithm();
		list = tablePS.getList();
		names = tablePS.getNames();
		
		dataHash = new Hashtable();
		dataHash.put("specificData", zAxisGrid);
		return dataHash;
	}
	
	public SelfOrganizingMap getAlg() {
		return alg;
	}
	public void setAlg(SelfOrganizingMap alg) {
		this.alg = alg;
	}
	public Integer getMaxIt() {
		return maxIt;
	}
	public void setMaxIt(Integer maxIt) {
		this.maxIt = maxIt;
	}

	@Override
	public void addPanel()
	{
		if(jTab==null) {
			super.addPanel();
			addGridTab(1);
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Self Organizing Map");
			addGridTab(count);
		}
		
		new CSSApplication(getContentPane());
	}
	
	public void addGridTab(int count) {
		JTable table = new JTable();
		// Add Excel export popup menu and menuitem
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
		// table.setAutoCreateRowSorter(true);
		
		GridFilterData gfd = new GridFilterData();
		gfd.setColumnNames(names);
		// append cluster information to list data
		gfd.setDataList(list);
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setRowSorter(new GridTableRowSorter(model));

		JPanel panel = new JPanel();
		panel.add(table);
		GridBagLayout gbl_mainPanel = new GridBagLayout();
		gbl_mainPanel.columnWidths = new int[] { 0, 0 };
		gbl_mainPanel.rowHeights = new int[] { 0, 0 };
		gbl_mainPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_mainPanel.rowWeights = new double[] { 1.0, Double.MIN_VALUE };
		panel.setLayout(gbl_mainPanel);
		
		addScrollPanel(panel, table);
		jTab.addTab(count + ". SOM Raw Data", panel);
	}

	public Double getL0() {
		return l0;
	}
	public void setL0(Double l0) {
		this.l0 = l0;
	}
	public Double getR0() {
		return r0;
	}
	public void setR0(Double r0) {
		this.r0 = r0;
	}
	public Double getTau() {
		return tau;
	}
	public void setTau(Double tau) {
		this.tau = tau;
	}

	@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getVar(varName);
	}
	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}
	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
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
	
}