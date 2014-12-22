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
package prerna.ui.main;

import java.awt.Color;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.event.ActionListener;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.JWindow;
import javax.swing.plaf.basic.BasicProgressBarUI;

import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This generates a Splash Screen and progress bar when the SEMOSS application is initially opened.
 */
public class SplashScreen extends JWindow {

	private static JProgressBar progressBar = new JProgressBar();

	/**
	 * Constructor for SplashScreen.
	 */
	public SplashScreen() {
		Container container = getContentPane();
		container.setLayout(null);

		BufferedImage image;
		JLabel picLabel = new JLabel();
		try {
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String picFileURL = workingDir +System.getProperty("file.separator")+"pictures"+System.getProperty("file.separator")+"semosslogo.jpg";
			image = ImageIO.read(new File(picFileURL));
			picLabel = new JLabel(new ImageIcon(image));
			picLabel.setSize(661, 335);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		progressBar.setUI(new MyProgressUI());
		progressBar.setForeground(Color.blue);
		progressBar.setMaximum(60);
		progressBar.setBounds(270, 335, 120, 15);
		progressBar.setIndeterminate(true);
		
		JLabel lblLicense = new JLabel("\u00A9 Distributed under the GNU General Public License");
		lblLicense.setBounds(210, 360, 350, 12);
		
		container.add(progressBar);
		container.add(picLabel);
		container.add(lblLicense);
		
		setSize(660, 385);
		setLocationRelativeTo(null);
		container.requestFocus();
		setVisible(true);
//		loadProgressBar();
	}

	/**
	 */
	private class MyProgressUI extends BasicProgressBarUI {

		private Rectangle r = new Rectangle();

		/**
		 * Method paintIndeterminate.
		 * @param g Graphics
		 * @param c JComponent
		 */
		@Override
		protected void paintIndeterminate(Graphics g, JComponent c) {
			Graphics2D g2d = (Graphics2D) g;
			g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
			r = getBox(r);
			g.setColor(progressBar.getForeground());
			g.fillRect(r.x, r.y, r.width, r.height);
		}
	}
}
