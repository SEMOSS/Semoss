package prerna.ui.components;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.util.Hashtable;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import org.apache.jena.larq.IndexLARQ;
import org.apache.jena.larq.IndexWriterFactory;
import org.apache.jena.larq.LARQ;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;

import prerna.search.SubjectIndexer;
import prerna.ui.main.listener.impl.RingsButtonListener;
import prerna.ui.main.listener.impl.TreeConverterListener;
import prerna.ui.main.listener.impl.VertexTextSizeListener;
import prerna.ui.swing.custom.CustomAruiStyle;
import prerna.ui.swing.custom.ToggleButton;
import prerna.ui.transformer.ArrowFillPaintTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.SearchArrowFillPaintTransformer;
import prerna.ui.transformer.SearchEdgeStrokeTransformer;
import prerna.ui.transformer.SearchVertexLabelFontTransformer;
import prerna.ui.transformer.SearchVertexPaintTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.util.CSSApplication;
import prerna.util.Constants;

import aurelienribon.ui.components.Button;
import aurelienribon.ui.css.Style;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;

import edu.uci.ics.jung.algorithms.layout.BalloonLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.algorithms.layout.RadialTreeLayout;
import edu.uci.ics.jung.visualization.VisualizationViewer;

public class SearchPanel extends JPanel implements KeyListener, FocusListener, ActionListener, Runnable {
	private JTextField searchText;
	private Model jenaModel = null;
	SubjectIndexer larqBuilder = null;
	public Hashtable <String, String> resHash = new Hashtable<String, String>();
	//String data = "";
	JPopupMenu menu = new JPopupMenu();
	VisualizationViewer target = null;
	SearchVertexPaintTransformer tx = new SearchVertexPaintTransformer(resHash);
	VertexPaintTransformer oldTx = null;
	SearchEdgeStrokeTransformer etx = new SearchEdgeStrokeTransformer();
	EdgeStrokeTransformer oldeTx = null;
	SearchArrowFillPaintTransformer afp = new SearchArrowFillPaintTransformer();
	ArrowFillPaintTransformer oldafpTx = null;
	SearchVertexLabelFontTransformer vlf = new SearchVertexLabelFontTransformer(resHash);
	VertexLabelFontTransformer oldVLF = null;
	public JToggleButton btnHighlight = null;
	long lastTime = 0;
	Thread thread = null;
	boolean typed = false;
	boolean searchContinue = true;
	Logger logger = Logger.getLogger(getClass());
	VertexTextSizeListener sizeListener;
	boolean showRingsButton = false;
	JToggleButton btnRingsButton;
	public JToggleButton treeButton;
	RingsButtonListener rings;
	TreeConverterListener treeListener;
	GraphPlaySheet gps;
	
	/**
	 * Create the panel.
	 */
	public SearchPanel() {
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[] {410, 20, 5, 0, 10, 5, 0, 0};
		gridBagLayout.rowHeights = new int[]{0, 0};
		gridBagLayout.columnWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gridBagLayout.rowWeights = new double[]{0.0, Double.MIN_VALUE};
		setLayout(gridBagLayout);
		
		searchText = new JTextField();
		searchText.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		searchText.setText(Constants.ENTER_TEXT);
		GridBagConstraints gbc_txtAbracadabra = new GridBagConstraints();
		gbc_txtAbracadabra.insets = new Insets(0, 0, 0, 5);
		gbc_txtAbracadabra.fill = GridBagConstraints.HORIZONTAL;
		gbc_txtAbracadabra.gridx = 0;
		gbc_txtAbracadabra.gridy = 0;
		add(searchText, gbc_txtAbracadabra);
		searchText.setColumns(9);
		searchText.addFocusListener(this);
		searchText.addKeyListener(this);
		
		// sets it up in terms of highlight
		btnHighlight = new JToggleButton("Q");
		btnHighlight.setToolTipText("Depress to see your results on the graph, keep it depressed to see results as you type (slow)");
		GridBagConstraints gbc_btnHighlight = new GridBagConstraints();
		gbc_btnHighlight.fill = GridBagConstraints.VERTICAL;
		gbc_btnHighlight.insets = new Insets(0, 5, 0, 5);
		gbc_btnHighlight.gridx = 1;
		gbc_btnHighlight.gridy = 0;
		add(btnHighlight, gbc_btnHighlight);
		btnHighlight.addActionListener(this);

		treeListener = new TreeConverterListener();
		treeButton = new JToggleButton("T");
		treeButton.setToolTipText("Convert current graph to tree by duplicating nodes with multiple in-edges");
		treeButton.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_treeButton = new GridBagConstraints();
		gbc_treeButton.fill = GridBagConstraints.VERTICAL;
		gbc_treeButton.insets = new Insets(0, 0, 0, 5);
		gbc_treeButton.gridx = 3;
		gbc_treeButton.gridy = 0;
		add(treeButton, gbc_treeButton);
		treeButton.addActionListener(treeListener);
		
		rings = new RingsButtonListener();
		btnRingsButton = new JToggleButton("O");
		btnRingsButton.setToolTipText("Only availble with Balloon and Radial Tree layouts. Show/hide layout rings");
		btnRingsButton.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_btnNewButton = new GridBagConstraints();
		gbc_btnNewButton.fill = GridBagConstraints.VERTICAL;
		gbc_btnNewButton.insets = new Insets(0, 0, 0, 5);
		gbc_btnNewButton.gridx = 4;
		gbc_btnNewButton.gridy = 0;
		add(btnRingsButton, gbc_btnNewButton);
		btnRingsButton.addActionListener(rings);
		
		JButton btnDecreaseFontSize = new JButton();
		btnDecreaseFontSize.setText("A");
		btnDecreaseFontSize.setToolTipText("Decrease the label font size of selected item or all items");
		btnDecreaseFontSize.setName("Decrease");
		btnDecreaseFontSize.setFont(new Font("Tahoma", Font.PLAIN, 8));
		GridBagConstraints gbc_btnA_1 = new GridBagConstraints();
		gbc_btnA_1.fill = GridBagConstraints.VERTICAL;
		gbc_btnA_1.insets = new Insets(0, 0, 0, 5);
		gbc_btnA_1.gridx = 6;
		gbc_btnA_1.gridy = 0;
		add(btnDecreaseFontSize, gbc_btnA_1);
		sizeListener = new VertexTextSizeListener();
		btnDecreaseFontSize.addActionListener(sizeListener);
		
		JButton btnIncreaseFontSize = new JButton();
		btnIncreaseFontSize.setText("A");
		btnIncreaseFontSize.setToolTipText("Increase the label font size of selected item or all items");
		btnIncreaseFontSize.setName("Increase");
		btnIncreaseFontSize.setFont(new Font("Tahoma", Font.PLAIN, 14));
		GridBagConstraints gbc_btnA = new GridBagConstraints();
		gbc_btnA.fill = GridBagConstraints.VERTICAL;
		gbc_btnA.gridx = 7;
		gbc_btnA.gridy = 0;
		add(btnIncreaseFontSize, gbc_btnA);
		btnIncreaseFontSize.addActionListener(sizeListener);

	}
	
	public void indexStatement(Statement st)
	{
		// indexes one statement at a time
	}
	
	public void indexStatements(Model jenaModel)
	{
		try
		{
			IndexWriter iw = IndexWriterFactory.create(new RAMDirectory());
			//larqBuilder = new IndexBuilderSubject(iw);
			larqBuilder = new SubjectIndexer(iw);
			larqBuilder.indexStatements(jenaModel.listStatements());
			larqBuilder.closeWriter();
			IndexLARQ larq = larqBuilder.getIndex();
			LARQ.setDefaultIndex(larq);
			this.jenaModel = jenaModel;
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void searchStatement(String searchString)
	{
		System.out.println("Jena Model is " + jenaModel);
		String searchQuery = "PREFIX pf: <http://jena.hpl.hp.com/ARQ/property#>"+
		"SELECT ?doc" +
				"{" +
				 " ?doc pf:textMatch " ;
		String remainder =  " 0.25 80). ?doc ?p ?lit }";
		String remainder2 =  " 0.25 80). ?lit ?p ?doc }";
		String remainder3 =  " 0.25 80). ?lit ?doc ?obj }";

		String searchQuery2 = "PREFIX pf: <http://jena.hpl.hp.com/ARQ/property#>" +
			"SELECT distinct ?doc  { ?doc pf:textMatch ("; 
	
		String cSearchQuery = searchQuery2 + "'" + searchString + "'" + remainder;
		//String cSearchQuery = searchQuery + "'" + data + "'" + remainder;
		System.out.println("Query  " + cSearchQuery);
		com.hp.hpl.jena.query.Query lQuery = QueryFactory.create(cSearchQuery);
		QueryExecution qexec2 = QueryExecutionFactory.create(lQuery, jenaModel);
		
		ResultSet rs = qexec2.execSelect();
		resHash.clear();
		menu.setSize(new Dimension(410, 60));
		menu.add("Results to be highlighted......");
		synchronized(menu)
		{
			while(rs.hasNext())
			{
				QuerySolution qs = rs.next();
				String doc = qs.get("doc") + "";
				System.out.println("Document is " + doc);
				resHash.put(doc, doc);	
				menu.add(doc);
			}
		}
		qexec2.close();
		// execute query for objects
		
		cSearchQuery = searchQuery2 + "'" + searchString + "'" + remainder2;
		//String cSearchQuery = searchQuery + "'" + data + "'" + remainder;
		System.out.println("Query  " + cSearchQuery);
		lQuery = QueryFactory.create(cSearchQuery);
		qexec2 = QueryExecutionFactory.create(lQuery, jenaModel);
		
		rs = qexec2.execSelect();
		synchronized(menu)
		{
			while(rs.hasNext())
			{
				QuerySolution qs = rs.next();
				String doc = qs.get("doc") + "";
				System.out.println("Document is " + doc);
				resHash.put(doc, doc);	
				menu.add(doc);
			}
		}
		qexec2.close();
		
		/*// execute query for predicates
		cSearchQuery = searchQuery2 + "'" + searchString + "'" + remainder3;
		//String cSearchQuery = searchQuery + "'" + data + "'" + remainder;
		System.out.println("Query  " + cSearchQuery);
		lQuery = QueryFactory.create(cSearchQuery);
		qexec2 = QueryExecutionFactory.create(lQuery, jenaModel);
		rs = qexec2.execSelect();
		synchronized(menu)
		{
			while(rs.hasNext())
			{
				QuerySolution qs = rs.next();
				String doc = qs.get("doc") + "";
				System.out.println("Document is " + doc);
				resHash.put(doc, doc);	
				menu.add(doc);
			}
		}*/

		tx.setVertHash(resHash);
		//menu.show(this, 0,20);
		//menu.setVisible(true);
		this.repaint();
		target.repaint();
		searchText.requestFocus(true);
	}
	
	public void setViewer(VisualizationViewer view)
	{
		this.target = view;
		sizeListener.setTransformer((VertexLabelFontTransformer) target.getRenderContext().getVertexFontTransformer());
		sizeListener.setViewer(view);
		rings.setViewer(view);
	}
	
	public void setGraphLayout(Layout lay){
		if(lay instanceof BalloonLayout || lay instanceof RadialTreeLayout){
			rings.setGraph(gps.forest);
			showRingsButton = true;
			btnRingsButton.setEnabled(true);
			//btnRingsButton.doClick();
			//btnRingsButton.doClick();
		}
		else{
			showRingsButton = false;
			btnRingsButton.setEnabled(false);
			btnRingsButton.setSelected(false);
		}
		rings.setLayout(lay);
		
	}
	
	public void setPlaySheet(GraphPlaySheet gps){

		this.gps = gps;
		treeListener.setPlaySheet(gps);
	}

	// key listener
	
	@Override
	public void keyPressed(KeyEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void keyReleased(KeyEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void keyTyped(KeyEvent e) {
		
		lastTime = System.currentTimeMillis();
		menu.setVisible(false);
		typed = true;
		//e.getKeyChar() >= 'a' && e.getKeyChar() <= 'b' && 
		// e.getKeyChar() != KeyEvent.VK_BACK_SPACE && e.getKeyChar() != KeyEvent.VK_DELETE)
		/*if(e.getID() == KeyEvent.KEY_TYPED && e.getKeyChar() != KeyEvent.VK_BACK_SPACE && e.getKeyChar() != KeyEvent.VK_DELETE)
		{
			data = data + e.getKeyChar();
			//searchText.setText(data);
		// create a thread here which will update the vector by searching
		}
		else if(e.getKeyChar() == KeyEvent.VK_BACK_SPACE)
		{
			//remove the last character
			//System.out.println("Invoked " + data.length());
			if(data.length() > 0)
			{
				data = data.substring(0,data.length() - 1);
				//searchText.setText(data);
			}
		}
		/*else if(e.getKeyChar() == KeyEvent.VK_DELETE)
		{
			// delete from this point on
			// see if there is anything is in the clipboard
			String selText = searchText.getSelectedText();
			if(selText.length() >= 0)
			{
				data = data.replace(selText, "");
				searchText.setText(data);
			}
			// else delete from this point on
			else
			{
				int curPosition = searchText.getCaretPosition();
				// find the substring until this position
				if(curPosition < searchText.getText().length())
				{
					String data1 = data.substring(0, curPosition);
					String data2 = data.substring(curPosition + 1);
					data = data1 + data2;
					searchText.setText(data);
				}
			}
		}*/
		/*data = searchText.getText(); */
		synchronized(this)
		{
			this.notify();
		}
		/*if(data.length() > 0)
			searchStatement(data);*/
	}

	
	// focus listener
	@Override
	public void focusGained(FocusEvent e) {
		if(searchText.getText().equalsIgnoreCase(Constants.ENTER_TEXT))
			searchText.setText("");
		if(thread == null || thread.getState() == Thread.State.TERMINATED)
		{
			thread = new Thread(this);
			searchContinue = true;
			thread.start();
			logger.info("Starting thread again");
		}
	}

	@Override
	public void focusLost(FocusEvent e) {
		// TODO Auto-generated method stub
		if(searchText.getText().equalsIgnoreCase(""))
		{
			searchText.setText(Constants.ENTER_TEXT);
			searchContinue = false;
			logger.info("Ended the thread");
		}
	}
	
	// toggle button listener
	// this will swap the view based on what is being presented
	public void actionPerformed(ActionEvent e)
	{
		// see if the key is depressed
		// if yes swap the transformer
		if(btnHighlight.isSelected())
		{
			oldTx = (VertexPaintTransformer)target.getRenderContext().getVertexFillPaintTransformer();
			target.getRenderContext().setVertexFillPaintTransformer(tx);
			oldeTx = (EdgeStrokeTransformer)target.getRenderContext().getEdgeStrokeTransformer();
			target.getRenderContext().setEdgeStrokeTransformer(etx);
			oldafpTx = (ArrowFillPaintTransformer)target.getRenderContext().getArrowFillPaintTransformer();
			target.getRenderContext().setArrowFillPaintTransformer(afp);
			oldVLF = (VertexLabelFontTransformer)target.getRenderContext().getVertexFontTransformer();
			vlf.setFontSize(oldVLF.getCurrentFontSize());
			target.getRenderContext().setVertexFontTransformer(vlf);
			target.repaint();
		}
		else
		{
			target.getRenderContext().setVertexFillPaintTransformer(oldTx);
			target.getRenderContext().setEdgeStrokeTransformer(oldeTx);
			target.getRenderContext().setArrowFillPaintTransformer(oldafpTx);
			target.getRenderContext().setVertexFontTransformer(oldVLF);
			target.repaint();
		}
	}
	
	public void run()
	{
		try {
			while(searchContinue)
			{
				long thisTime = System.currentTimeMillis();
				if(thisTime - lastTime > 300 && typed)
				{
					synchronized(menu)
					{
						menu.setVisible(false);
						menu.removeAll();
					}
					if(searchText.getText().length() > 0 && lastTime != 0)
						searchStatement(searchText.getText());
					lastTime = System.currentTimeMillis();
					typed = false;
				}
				else
				{
					//menu.setVisible(false);
					Thread.sleep(100);
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
