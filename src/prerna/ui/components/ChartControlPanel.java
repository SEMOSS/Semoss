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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Insets;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JPanel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.impl.ChartImageExportListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 Icons used in this search panel contributed from gentleface.com.
 * @author karverma
 * @version $Revision: 1.0 $
 */
public class ChartControlPanel extends JPanel {
	
	static final Logger logger = LogManager.getLogger(ChartControlPanel.class.getName());
	public JButton btnGraphImageExport;
	ChartImageExportListener imageExportListener;
	BrowserPlaySheet bps;
	
	/**
	 * Create the panel.
	 */
	public ChartControlPanel() {
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[] {0};
		gridBagLayout.rowHeights = new int[]{0};
		gridBagLayout.columnWeights = new double[]{1.0};
		gridBagLayout.rowWeights = new double[]{0.0};
		setLayout(gridBagLayout);
	}
	
	public void addExportButton(int gridXidx){

		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		String exportIconLocation = "/pictures/export.png";
		btnGraphImageExport = new JButton();
		try {
			Image img = ImageIO.read(new File(workingDir+exportIconLocation));
			Image newimg = img.getScaledInstance(15, 15,  java.awt.Image.SCALE_SMOOTH );
			btnGraphImageExport.setIcon(new ImageIcon(newimg));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		imageExportListener = new ChartImageExportListener();
		btnGraphImageExport.addActionListener(imageExportListener);
		btnGraphImageExport.setToolTipText("Export vector image of chart.");

		GridBagConstraints gbc_btnGraphImageExport = new GridBagConstraints();
		gbc_btnGraphImageExport.insets = new Insets(10, 0, 0, 5);
		gbc_btnGraphImageExport.anchor = GridBagConstraints.EAST;
		gbc_btnGraphImageExport.gridx = gridXidx;
		gbc_btnGraphImageExport.gridy = 0;
		add(btnGraphImageExport, gbc_btnGraphImageExport);
	}
	
	/**
	 * Gets the image export button.
	
	 * @return JButton */
	public JButton getImageExportButton()
	{
		return btnGraphImageExport;
	}
	
	/**
	 * Sets the playsheet.
	 * @param bps BrowserPlaySheet
	 */
	public void setPlaySheet(BrowserPlaySheet bps) {
		this.bps = bps;
	}
}
