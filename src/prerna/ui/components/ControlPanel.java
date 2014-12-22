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
package prerna.ui.components;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Insets;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;
import javax.swing.border.BevelBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.search.SearchController;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.main.listener.impl.GraphImageExportListener;
import prerna.ui.main.listener.impl.GraphTextSizeListener;
import prerna.ui.main.listener.impl.GraphTransformerResetListener;
import prerna.ui.main.listener.impl.GraphVertexSizeListener;
import prerna.ui.main.listener.impl.RedoListener;
import prerna.ui.main.listener.impl.RingsButtonListener;
import prerna.ui.main.listener.impl.TreeConverterListener;
import prerna.ui.main.listener.impl.UndoListener;
import prerna.ui.main.listener.impl.WeightConvertButtonListener;
import prerna.ui.transformer.EdgeLabelFontTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexShapeTransformer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import edu.uci.ics.jung.algorithms.layout.BalloonLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.algorithms.layout.RadialTreeLayout;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.picking.PickedState;


/**
 Icons used in this search panel contributed from gentleface.com.
 * @author karverma
 * @version $Revision: 1.0 $
 */
public class ControlPanel extends JPanel {

	private JTextField searchText;
	static final Logger logger = LogManager.getLogger(ControlPanel.class.getName());
	GraphTextSizeListener textSizeListener;
	GraphVertexSizeListener vertSizeListener;
	boolean showRingsButton = false;
	JToggleButton btnRingsButton;
	public JToggleButton treeButton;
	RingsButtonListener rings;
	TreeConverterListener treeListener;
	GraphPlaySheet gps;
	public JButton btnGraphImageExport;
	public WeightDropDownButton weightButton;
	GraphImageExportListener imageExportListener;
	WeightConvertButtonListener edgeWeightListener;
	GraphTransformerResetListener resetTransListener;
	public JToggleButton btnHighlight = null;
	boolean search =true;
	PickedState state;
	VisualizationViewer target = null;
	public SearchController searchCon;
	RedoListener redoListener = new RedoListener();
	UndoListener undoListener = new UndoListener();
	public JButton undoBtn, redoBtn, btnDecreaseFontSize, btnIncreaseFontSize;
	private JButton btnIncreaseVertSize, btnDecreaseVertSize;
	/**
	 * Create the panel.
	 * @param search Boolean
	 */
	public ControlPanel(Boolean search) {
		this.search=search;
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[] {0, 20, 0, 0, 0, 0, 10, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0};
		gridBagLayout.rowHeights = new int[]{0, 0};
		gridBagLayout.columnWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gridBagLayout.rowWeights = new double[]{0.0, Double.MIN_VALUE};
		setLayout(gridBagLayout);
		
		searchText = new JTextField();
		searchText.setMaximumSize(new Dimension(300,20));
		searchText.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		searchText.setText(Constants.ENTER_TEXT);
		if(!this.search)
		{
			searchText.setText(Constants.ENTER_SEARCH_DISABLED_TEXT);
			searchText.setEnabled(false);
		}
		
		GridBagConstraints gbc_txtAbracadabra = new GridBagConstraints();
		gbc_txtAbracadabra.fill = GridBagConstraints.HORIZONTAL;
		gbc_txtAbracadabra.insets = new Insets(0, 0, 0, 5);
		gbc_txtAbracadabra.gridx = 0;
		gbc_txtAbracadabra.gridy = 0;
		add(searchText, gbc_txtAbracadabra);
		searchText.setColumns(9);
		searchCon = new SearchController();
		searchCon.setText(searchText);
		searchText.addFocusListener(searchCon);
		searchText.addKeyListener(searchCon);
		
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		
		// initialize keystroke and keys for buttons on control panel
		String keyStroke ="";
		KeyStroke key = KeyStroke.getKeyStroke(keyStroke);

		// sets it up in terms of highlight
		btnHighlight = new JToggleButton();
		String searchIconLocation = "/pictures/search.png";
		try {
		    Image img = ImageIO.read(new File(workingDir+searchIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    btnHighlight.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }

		
		btnHighlight.setToolTipText("<html><b>Search</b><br>Depress to see your results on the graph,<br>keep it depressed to see results as you type (slow)</html>");
		GridBagConstraints gbc_btnHighlight = new GridBagConstraints();
		gbc_btnHighlight.fill = GridBagConstraints.VERTICAL;
		gbc_btnHighlight.insets = new Insets(0, 5, 0, 5);
		gbc_btnHighlight.gridx = 1;
		gbc_btnHighlight.gridy = 0;
		add(btnHighlight, gbc_btnHighlight);
		btnHighlight.addActionListener(searchCon);
		
		
		JSeparator separator = new JSeparator();
		separator.setOrientation(SwingConstants.VERTICAL);
		GridBagConstraints gbc_separator = new GridBagConstraints();
		gbc_separator.fill = GridBagConstraints.VERTICAL;
		gbc_separator.insets = new Insets(0, 0, 0, 5);
		gbc_separator.gridx = 2;
		gbc_separator.gridy = 0;
		add(separator, gbc_separator);
		
		
		String restIconLocation = "/pictures/refresh.png";
		JButton resetBtn = new JButton();
		try {
		    Image img = ImageIO.read(new File(workingDir+restIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    resetBtn.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		resetTransListener = new GraphTransformerResetListener();
		resetBtn.addActionListener(resetTransListener);
		resetBtn.setToolTipText("<html><b>Reset (F5)</b><br>Reset Graph Transformers</html>");
		// add key bindings for button
		keyStroke = "F5";
		key = KeyStroke.getKeyStroke("F5");
		// map keystroke with action id 
		resetBtn.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// maps action id to listener
		resetBtn.getActionMap().put(keyStroke, resetTransListener);
		GridBagConstraints gbc_resetBtn = new GridBagConstraints();
		gbc_resetBtn.fill = GridBagConstraints.VERTICAL;
		gbc_resetBtn.insets = new Insets(0, 0, 0, 5);
		gbc_resetBtn.gridx = 3;
		gbc_resetBtn.gridy = 0;
		add(resetBtn, gbc_resetBtn);
		
		String undoIconLocation = "/pictures/undo.png";
		undoBtn = new JButton("");
		try {
		    Image img = ImageIO.read(new File(workingDir+undoIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    undoBtn.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		undoBtn.setToolTipText("<html><b>Undo (CRTL+Z)</b><br>Undo the last graph action</html>");
		undoBtn.addActionListener(undoListener);
		// add key bindings for button
		keyStroke = "control Z";
		key = KeyStroke.getKeyStroke(KeyEvent.VK_Z,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id 
		undoBtn.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// maps action id to listener
		undoBtn.getActionMap().put(keyStroke, undoListener);
		GridBagConstraints gbc_undoBtn = new GridBagConstraints();
		gbc_undoBtn.fill = GridBagConstraints.VERTICAL;
		gbc_undoBtn.insets = new Insets(0, 0, 0, 5);
		gbc_undoBtn.gridx = 4;
		gbc_undoBtn.gridy = 0;
		add(undoBtn, gbc_undoBtn);
		undoBtn.setEnabled(false);
		
		String redoIconLocation = "/pictures/redo.png";
		redoBtn = new JButton("");
		try {
		    Image img = ImageIO.read(new File(workingDir+redoIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    redoBtn.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		redoBtn.setToolTipText("<html><b>Redo (CRTL+Y)</b><br>Redo the previous action</html>");
		redoBtn.addActionListener(redoListener);
		// add key bindings for button
		keyStroke = "control Y";
		key = KeyStroke.getKeyStroke(KeyEvent.VK_Y,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id 
		redoBtn.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// maps action id to listener
		redoBtn.getActionMap().put(keyStroke, redoListener);
		GridBagConstraints gbc_redoBtn = new GridBagConstraints();
		gbc_redoBtn.fill = GridBagConstraints.VERTICAL;
		gbc_redoBtn.insets = new Insets(0, 0, 0, 5);
		gbc_redoBtn.gridx = 5;
		gbc_redoBtn.gridy = 0;
		add(redoBtn, gbc_redoBtn);
		redoBtn.setEnabled(false);
		
		String exportIconLocation = "/pictures/export.png";
		btnGraphImageExport = new JButton();
		try {
		    Image img = ImageIO.read(new File(workingDir+exportIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    btnGraphImageExport.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		imageExportListener = new GraphImageExportListener();
		btnGraphImageExport.addActionListener(imageExportListener);
		btnGraphImageExport.setToolTipText("<html><b>Export (CTRL+S)</b><br>Export vector image of graph</html>");
		// add key bindings for button
		keyStroke = "control S";
		key = KeyStroke.getKeyStroke(KeyEvent.VK_S,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id 
		btnGraphImageExport.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// maps action id to listener
		btnGraphImageExport.getActionMap().put(keyStroke, imageExportListener);
		GridBagConstraints gbc_btnGraphImageExport = new GridBagConstraints();
		gbc_btnGraphImageExport.fill = GridBagConstraints.VERTICAL;
		gbc_btnGraphImageExport.insets = new Insets(0, 0, 0, 5);
		gbc_btnGraphImageExport.gridx = 6;
		gbc_btnGraphImageExport.gridy = 0;
		add(btnGraphImageExport, gbc_btnGraphImageExport);
		
		
		JSeparator separator_1 = new JSeparator();
		separator_1.setOrientation(SwingConstants.VERTICAL);
		GridBagConstraints gbc_separator_1 = new GridBagConstraints();
		gbc_separator_1.fill = GridBagConstraints.VERTICAL;
		gbc_separator_1.insets = new Insets(0, 0, 0, 5);
		gbc_separator_1.gridx = 7;
		gbc_separator_1.gridy = 0;
		add(separator_1, gbc_separator_1);
		
		String treeIconLocation = "/pictures/tree.png";
		treeButton = new JToggleButton();
		try {
		    Image img = ImageIO.read(new File(workingDir+treeIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    treeButton.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		treeListener = new TreeConverterListener();
		treeButton.setToolTipText("<html><b>Convert to Tree</b><br>Convert current graph to tree by duplicating nodes with multiple in-edges</html>");
		treeButton.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_treeButton = new GridBagConstraints();
		gbc_treeButton.fill = GridBagConstraints.VERTICAL;
		gbc_treeButton.insets = new Insets(0, 0, 0, 5);
		gbc_treeButton.gridx = 8;
		gbc_treeButton.gridy = 0;
		add(treeButton, gbc_treeButton);
		treeButton.addActionListener(treeListener);
		
		String ringIconLocation = "/pictures/ring.png";
		btnRingsButton = new JToggleButton();
		try {
		    Image img = ImageIO.read(new File(workingDir+ringIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    btnRingsButton.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
		  }
		rings = new RingsButtonListener();
		btnRingsButton.setToolTipText("<html><b>Show Radial Rings</b><br>Only available with Balloon and Radial Tree layouts</html>");
		btnRingsButton.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_btnNewButton = new GridBagConstraints();
		gbc_btnNewButton.fill = GridBagConstraints.VERTICAL;
		gbc_btnNewButton.insets = new Insets(0, 0, 0, 5);
		gbc_btnNewButton.gridx = 9;
		gbc_btnNewButton.gridy = 0;
		add(btnRingsButton, gbc_btnNewButton);
		btnRingsButton.addActionListener(rings);
		
		
		edgeWeightListener = new WeightConvertButtonListener();
		String widthIconLocation = "/pictures/width.png";
		try {
		    Image img = ImageIO.read(new File(workingDir+widthIconLocation));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    weightButton = new WeightDropDownButton(new ImageIcon(newimg));
		   // edgeWeightButton.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		weightButton.setupButton();
		weightButton.setToolTipText("<html><b>Edge Weight</b><br>Convert edge thickness corresponding to properties that exist on the edges</html>");
		weightButton.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_edgeWeightButton = new GridBagConstraints();
		gbc_edgeWeightButton.fill = GridBagConstraints.VERTICAL;
		gbc_edgeWeightButton.insets = new Insets(0, 0, 0, 5);
		gbc_edgeWeightButton.gridx = 10;
		gbc_edgeWeightButton.gridy = 0;
		add(weightButton, gbc_edgeWeightButton);
		weightButton.addActionListener(edgeWeightListener);
		
		JSeparator separator_2 = new JSeparator();
		separator_2.setOrientation(SwingConstants.VERTICAL);
		GridBagConstraints gbc_separator_2 = new GridBagConstraints();
		gbc_separator_2.fill = GridBagConstraints.VERTICAL;
		gbc_separator_2.insets = new Insets(0, 0, 0, 5);
		gbc_separator_2.gridx = 11;
		gbc_separator_2.gridy = 0;
		add(separator_2, gbc_separator_2);

		vertSizeListener = new GraphVertexSizeListener();
		btnDecreaseVertSize = new JButton();
		String decreaseVertSizeIcon = "/pictures/decreaseNodeSize.png";
		try {
		    Image img = ImageIO.read(new File(workingDir+decreaseVertSizeIcon));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    btnDecreaseVertSize.setIcon(new ImageIcon(newimg));
		   // edgeWeightButton.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		btnDecreaseVertSize.setName("Decrease");
		btnDecreaseVertSize.addActionListener(vertSizeListener);		
		btnDecreaseVertSize.setToolTipText("<html><b>Decrease Node Size (CTRL+[)</b><br>Decrease the node size of selected nodes or all nodes</html>");
		// add key bindings for button
		keyStroke = "control [";
		key = KeyStroke.getKeyStroke(KeyEvent.VK_OPEN_BRACKET,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id 
		btnDecreaseVertSize.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// maps action id to listener
		btnDecreaseVertSize.getActionMap().put(keyStroke, vertSizeListener);
		GridBagConstraints gbc_btnDecreaseVertSize = new GridBagConstraints();
		gbc_btnDecreaseVertSize.fill = GridBagConstraints.BOTH;
		gbc_btnDecreaseVertSize.insets = new Insets(0, 0, 0, 5);
		gbc_btnDecreaseVertSize.gridx = 12;
		gbc_btnDecreaseVertSize.gridy = 0;
		add(btnDecreaseVertSize, gbc_btnDecreaseVertSize);

		
		
		btnIncreaseVertSize = new JButton();
		String increaseVertSizeIcon = "/pictures/increaseNodeSize.png";
		try {
		    Image img = ImageIO.read(new File(workingDir+increaseVertSizeIcon));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    btnIncreaseVertSize.setIcon(new ImageIcon(newimg));
		   // edgeWeightButton.setIcon(new ImageIcon(newimg));
		  } catch (IOException ex) {
			  logger.debug(ex);
		  }
		btnIncreaseVertSize.setName("Increase");
		btnIncreaseVertSize.addActionListener(vertSizeListener);
		btnIncreaseVertSize.setToolTipText("<html><b>Increase Node Size (CTRL+])</b><br>Increase the node size of selected nodes or all nodes</html>");
		// add key bindings for button
		keyStroke = "control ]";
		key = KeyStroke.getKeyStroke(KeyEvent.VK_CLOSE_BRACKET,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id 
		btnIncreaseVertSize.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// maps action id to listener
		btnIncreaseVertSize.getActionMap().put(keyStroke, vertSizeListener);
		GridBagConstraints gbc_btnIncreaseVertSize = new GridBagConstraints();
		gbc_btnIncreaseVertSize.fill = GridBagConstraints.BOTH;
		gbc_btnIncreaseVertSize.insets = new Insets(0, 0, 0, 5);
		gbc_btnIncreaseVertSize.gridx = 13;
		gbc_btnIncreaseVertSize.gridy = 0;
		add(btnIncreaseVertSize, gbc_btnIncreaseVertSize);
		

		btnDecreaseFontSize = new JButton();
		btnDecreaseFontSize.setName("Decrease");
		btnDecreaseFontSize.setText("A");
		btnDecreaseFontSize.setFont(new Font("Tahoma", Font.PLAIN, 8));
		textSizeListener = new GraphTextSizeListener();
		btnDecreaseFontSize.addActionListener(textSizeListener);
		btnDecreaseFontSize.setToolTipText("<html><b>Shrink Font (CTRL+-)</b><br>Decrease font size of selected labels or all labels");
		// add key bindings for button
		keyStroke = "contorl -";
		key = KeyStroke.getKeyStroke(KeyEvent.VK_MINUS,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id
		btnDecreaseFontSize.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// map action id to listener
		btnDecreaseFontSize.getActionMap().put(keyStroke, textSizeListener);
		GridBagConstraints gbc_btnA_1 = new GridBagConstraints();
		gbc_btnA_1.fill = GridBagConstraints.VERTICAL;
		gbc_btnA_1.insets = new Insets(0, 0, 0, 5);
		gbc_btnA_1.gridx = 14;
		gbc_btnA_1.gridy = 0;
		add(btnDecreaseFontSize, gbc_btnA_1);
		
		btnIncreaseFontSize = new JButton();
		btnIncreaseFontSize.setName("Increase");
		btnIncreaseFontSize.setText("A");
		btnIncreaseFontSize.setFont(new Font("Tahoma", Font.PLAIN, 14));
		btnIncreaseFontSize.addActionListener(textSizeListener);
		btnIncreaseFontSize.setToolTipText("<html><b>Grow Font (CTRL+=)</b><br>Increase font size of selected labels or all labels");
		// add key bindings for button
		keyStroke = "contorl +";
		key = KeyStroke.getKeyStroke(KeyEvent.VK_EQUALS,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id
		btnIncreaseFontSize.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// map action id to listener
		btnIncreaseFontSize.getActionMap().put(keyStroke, textSizeListener);
		GridBagConstraints gbc_btnA = new GridBagConstraints();
		gbc_btnA.insets = new Insets(0, 0, 0, 5);
		gbc_btnA.fill = GridBagConstraints.VERTICAL;
		gbc_btnA.gridx = 15;
		gbc_btnA.gridy = 0;
		add(btnIncreaseFontSize, gbc_btnA);

	}
	
	/**
	 * Sets the viewer.
	 * @param view VisualizationViewer
	 */
	public void setViewer(VisualizationViewer view)
	{
		this.target = view;
		this.state=view.getPickedVertexState();
		textSizeListener.setTransformers((VertexLabelFontTransformer) target.getRenderContext().getVertexFontTransformer(),(EdgeLabelFontTransformer) target.getRenderContext().getEdgeFontTransformer());
		textSizeListener.setViewer(view);
		vertSizeListener.setTransformers((VertexShapeTransformer) target.getRenderContext().getVertexShapeTransformer());
		vertSizeListener.setViewer(view);
		rings.setViewer(view);
		searchCon.setState(this.state);
		searchCon.setTarget(view);
	}
	
	/**
	 * Sets the graph layout.
	 * @param lay Layout
	 */
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
	
	/**
	 * Sets the playsheet for all necessary listeners.
	 * @param gps GraphPlaySheet
	 */
	public void setPlaySheet(GraphPlaySheet gps){

		this.gps = gps;
		treeListener.setPlaySheet(gps);
		edgeWeightListener.setPlaySheet(gps);
		resetTransListener.setPlaySheet(gps);
		redoListener.setPlaySheet(gps);
		undoListener.setPlaySheet(gps);
		searchCon.setGPS(gps);
		weightButton.setPlaySheet(gps);
	}


}
