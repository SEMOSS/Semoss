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


/**
 * This generates a Splash Screen and progress bar when the SEMOSS application is initially opened.
 */
public class SplashScreen extends JWindow {

	private static JProgressBar progressBar = new JProgressBar();
	private static int count;

	/**
	 * Constructor for SplashScreen.
	 */
	public SplashScreen() {
		Container container = getContentPane();
		container.setLayout(null);

		BufferedImage image;
		JLabel picLabel = new JLabel();
		try {
			String workingDir = System.getProperty("user.dir");
			String picFileURL = workingDir + "\\pictures\\semosslogo.jpg";
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
	 * Method loadProgressBar.  Updates the value of the progress bar.
	 */
	private void loadProgressBar() {
		ActionListener progressListener = new ActionListener() {

			public void actionPerformed(java.awt.event.ActionEvent evt) {
				count++;
				progressBar.setValue(count);
			}
		};
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
