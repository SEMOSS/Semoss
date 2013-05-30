package prerna.ui.components;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.ArrayList;

import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IPlaySheet;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class GridRAWPlaySheet extends JInternalFrame implements IPlaySheet{

	String query = null;
	String title = null;
	String questionID = null;
	boolean extend = false;
	boolean append = false;
	JDesktopPane pane = null;
	ParamPanel panel = null;
	IEngine engine = null;
	ResultSet rs = null;
	Model jenaModel = ModelFactory.createDefaultModel();
	GridFilterData gfd = new GridFilterData();
	JTable table = null;
	public JProgressBar jBar = new JProgressBar();
	
	Logger logger = Logger.getLogger(getClass());
	
	public GridRAWPlaySheet() {
		super("_", true, true, true, true);
		//addPanel();
	}

	@Override
	public void run() {
		logger.info("Running Grid Playsheet ");
		createView();
	}

	@Override
	public void setQuery(String query) {
		this.query = query;
	}

	@Override
	public String getQuery() {
		return this.query;
	}

	@Override
	public void setJDesktopPane(JDesktopPane pane) {
		this.pane = pane;
	}

	@Override
	public void setQuestionID(String id) {
		this.questionID = id;
	}

	@Override
	public String getQuestionID() {
		return this.questionID;
	}

	public void setGFD(GridFilterData gfd) {
		this.gfd = gfd;
	}
	
	@Override
	public void createView() {
		// uses the engine to create the sparql result
		// once created find the binding names
		// compose a array
		// and then create filter data and a table
		
		ArrayList <Object []> list = new ArrayList();
		addPanel();
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){
			wrapper.setQuery(query);
			progressBarUpdate("10%...Querying RDF Repository", 10);
			wrapper.setEngine(engine);
			progressBarUpdate("30%...Querying RDF Repository", 30);
			wrapper.executeQuery();
			progressBarUpdate("60%...Processing RDF Statements	", 60);
		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}
		
		// get the bindings from it
		String [] names = wrapper.getVariables();
		
		gfd.setColumnNames(names);
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				
				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = sjss.getRawVar(names[colIndex]);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
			gfd.setDataList(list);
			GridTableModel model = new GridTableModel(gfd);
			table.setModel(model);
		} catch (Exception e) {
			logger.fatal(e);
		}
		progressBarUpdate("80%...Creating Visualization", 80);

		progressBarUpdate("100%...Table Generation Complete", 100);
	}
	
	public void addPanel()
	{
		// create a panel and add the table to it
		try {
			table = new JTable();
			JPanel mainPanel = new JPanel();

			logger.debug("Created the table");
			//this.addInternalFrameListener(GridPlaySheetListener.getInstance());
			logger.debug("Added the internal frame listener ");
			table.setAutoCreateRowSorter(true);
			//this.add(new JButton("Yo"));
			this.setContentPane(mainPanel);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[]{0, 0};
			gbl_mainPanel.rowHeights = new int[]{0, 0};
			gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			mainPanel.setLayout(gbl_mainPanel);
			
			JScrollPane scrollPane = new JScrollPane(table);
			scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
			scrollPane.setAutoscrolls(true);
			
			GridBagConstraints gbc_scrollPane = new GridBagConstraints();
			gbc_scrollPane.fill = GridBagConstraints.BOTH;
			gbc_scrollPane.gridx = 0;
			gbc_scrollPane.gridy = 0;
			mainPanel.add(scrollPane, gbc_scrollPane);

			jBar.setStringPainted(true);
			jBar.setString("0%...Preprocessing");
			jBar.setValue(0);
			JPanel barPanel = new JPanel();
			
			GridBagConstraints gbc_barPanel = new GridBagConstraints();
			gbc_barPanel.fill = GridBagConstraints.BOTH;
			gbc_barPanel.gridx = 0;
			gbc_barPanel.gridy = 1;
			mainPanel.add(barPanel, gbc_barPanel);
			barPanel.setLayout(new BorderLayout(0, 0));
			barPanel.add(jBar, BorderLayout.CENTER);
			
			pane.add(this);
			
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

	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		// this is easy
		// just use the filter to not show stuff I dont need to show
		// but this also means I need to create the vertex filter data etc. 
		//
		
	}

	@Override
	public void setParamPanel(ParamPanel panel) {
		this.panel = panel;
	}

	@Override
	public void setRDFEngine(IEngine engine) {
		this.engine = engine;
	}

	@Override
	public void setTitle(String title) {
		super.setTitle(title);
		this.title = title;
	}

	@Override
	public VertexFilterData getFilterData() {
		return null;
	}
	
	public void setResultSet(ResultSet rs) {
		this.rs = rs;
	}

	public void setEngineType(ResultSet rs) {
		this.rs = rs;
	}

	@Override
	public void extendView() {
		// uses the engine to create the sparql result
		// once created find the binding names
		// compose a array
		// and then create filter data and a table
		
		ArrayList <Object []> list = gfd.dataList;
		
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		progressBarUpdate("10%...Querying RDF Repository", 10);
		wrapper.setEngine(engine);
		progressBarUpdate("30%...Querying RDF Repository", 30);
		wrapper.executeQuery();
		wrapper.getVariables();
		progressBarUpdate("60%...Processing RDF Statements	", 60);
		// get the bindings from it
		String [] names = gfd.columnNames;
		
		int count = gfd.getNumRows();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				
				Object [] values = new Object[names.length];
				boolean filledData = true;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(sjss.getVar(names[colIndex]) != null)
					{
						values[colIndex] = sjss.getVar(names[colIndex]);
						logger.debug("Binding Name " + names[colIndex]);
						logger.debug("Binding Value " + values[colIndex]);
					}
					else {
						filledData = false;
						break;
					}
				}
				logger.debug("Creating new Value " + values);
				if(filledData)
					list.add(count, values);
				count++;
			}
			gfd.setDataList(list);
			((GridTableModel)table.getModel()).fireTableDataChanged();
		} catch (Exception e) {
			logger.fatal(e);
		}
		progressBarUpdate("80%...Creating Visualization", 80);
		progressBarUpdate("100%...Table Generation Complete", 100);
		// Create the table and let it go
		// fire the table refresh event or may be the listener would do it
	}

	private void progressBarUpdate(String status, int x)
	{
		jBar.setString(status);
		jBar.setValue(x);
	}	
	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void undoView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void redoView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setJenaModel(Model jenaModel) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Model getJenaModel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void recreateView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setActiveSheet() {
		// TODO Auto-generated method stub
		
	}

}
