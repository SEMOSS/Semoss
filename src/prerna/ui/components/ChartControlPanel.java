/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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
	
	Logger logger = Logger.getLogger(getClass());
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
