/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.comparison.specific.tap;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.SystemColor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

public class DBRenamer {
	private static final Logger logger = LogManager.getLogger(DBRenamer.class);
	private static final String DIR_SEPERATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static final int WIDTH = 600;
	private static final int HEIGHT = 250;

	JFrame window = new JFrame();
	JPanel dbImportPanel = new JPanel();
	JLabel doneLabel = new JLabel();
	private JTextField importFolderNameField = new JTextField();
	private JTextField importSMSSNameField = new JTextField();

	public DBRenamer() {
		window.setSize(WIDTH, HEIGHT);
		window.setTitle("DB Name Changer");

		dbImportPanel.setBackground(SystemColor.control);
		dbImportPanel.setSize(WIDTH, 100);
		window.add(dbImportPanel);

		JLabel selectionFolderLabel = new JLabel("Select DB Folder:");
		GridBagConstraints gbc_selectionFolderLabel = new GridBagConstraints();
		gbc_selectionFolderLabel.anchor = GridBagConstraints.WEST;
		gbc_selectionFolderLabel.insets = new Insets(0, 0, 5, 5);
		gbc_selectionFolderLabel.gridx = 1;
		gbc_selectionFolderLabel.gridy = 1;
		dbImportPanel.add(selectionFolderLabel, gbc_selectionFolderLabel);
		selectionFolderLabel.setFont(new Font("Tahoma", Font.PLAIN, 12));

		JButton folderBrowseButton = new JButton("Browse");
		folderBrowseButton.setName(Constants.IMPORT_BUTTON_BROWSE);
		folderBrowseButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_folderBrowseButton = new GridBagConstraints();
		gbc_folderBrowseButton.anchor = GridBagConstraints.WEST;
		gbc_folderBrowseButton.insets = new Insets(0, 0, 5, 5);
		gbc_folderBrowseButton.gridx = 2;
		gbc_folderBrowseButton.gridy = 1;
		dbImportPanel.add(folderBrowseButton, gbc_folderBrowseButton);
		folderBrowseButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				doneLabel.setText("");
				JFileChooser jfc = new JFileChooser();
				jfc.setCurrentDirectory(new java.io.File("."));
				jfc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				int retVal = jfc.showOpenDialog((JComponent) e.getSource());
				// Handle open button action.
				if (retVal == JFileChooser.APPROVE_OPTION) {
					File file = jfc.getSelectedFile();
					// This is where a real application would open the file.
					importFolderNameField.setText(file.getAbsolutePath());
				}
			}
		});

		importFolderNameField.setColumns(40);
		importFolderNameField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_importFolderNameField = new GridBagConstraints();
		gbc_importFolderNameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_importFolderNameField.gridx = 3;
		gbc_importFolderNameField.gridy = 1;
		dbImportPanel.add(importFolderNameField, gbc_importFolderNameField);

		JLabel selectionSMSSLabel = new JLabel("Select SMSS file:");
		GridBagConstraints gbc_selectionSMSSLabel = new GridBagConstraints();
		gbc_selectionSMSSLabel.anchor = GridBagConstraints.WEST;
		gbc_selectionSMSSLabel.insets = new Insets(0, 0, 0, 5);
		gbc_selectionSMSSLabel.gridx = 1;
		gbc_selectionSMSSLabel.gridy = 3;
		dbImportPanel.add(selectionSMSSLabel, gbc_selectionSMSSLabel);
		selectionSMSSLabel.setFont(new Font("Tahoma", Font.PLAIN, 12));

		JButton smssBrowseButton = new JButton("Browse");
		smssBrowseButton.setName(Constants.IMPORT_BUTTON_BROWSE);
		smssBrowseButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_smssBrowseButton = new GridBagConstraints();
		gbc_smssBrowseButton.anchor = GridBagConstraints.WEST;
		gbc_smssBrowseButton.insets = new Insets(0, 0, 0, 5);
		gbc_smssBrowseButton.gridx = 2;
		gbc_smssBrowseButton.gridy = 3;
		dbImportPanel.add(smssBrowseButton, gbc_smssBrowseButton);
		smssBrowseButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				doneLabel.setText("");
				JFileChooser jfc = new JFileChooser();
				jfc.setCurrentDirectory(new java.io.File("."));
				int retVal = jfc.showOpenDialog((JComponent) e.getSource());
				// Handle open button action.
				if (retVal == JFileChooser.APPROVE_OPTION) {
					File file = jfc.getSelectedFile();
					// This is where a real application would open the file.
					importSMSSNameField.setText(file.getAbsolutePath());
				}
			}
		});

		importSMSSNameField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_importSMSSNameField = new GridBagConstraints();
		gbc_importSMSSNameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_importSMSSNameField.gridwidth = 10;
		gbc_importSMSSNameField.insets = new Insets(0, 0, 0, 5);
		gbc_importSMSSNameField.gridx = 3;
		gbc_importSMSSNameField.gridy = 3;
		dbImportPanel.add(importSMSSNameField, gbc_importSMSSNameField);
		importSMSSNameField.setColumns(40);

		JButton changeNameButton = new JButton("Change DB Name");
		changeNameButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_changeNameButton = new GridBagConstraints();
		gbc_changeNameButton.anchor = GridBagConstraints.WEST;
		gbc_changeNameButton.insets = new Insets(0, 0, 0, 5);
		gbc_changeNameButton.gridx = 2;
		gbc_changeNameButton.gridy = 3;
		dbImportPanel.add(changeNameButton, gbc_changeNameButton);
		dbImportPanel.add(doneLabel);
		changeNameButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String folderDirectory = importFolderNameField.getText();
				String dbName = folderDirectory.substring(folderDirectory.indexOf("db\\") + 3);
				File folder = new File(Utility.normalizePath(folderDirectory));
				File[] listOfFiles = folder.listFiles();
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile() && listOfFiles[i].getName().contains(dbName)) {
						File f = new File(Utility.normalizePath(folderDirectory + DIR_SEPERATOR + listOfFiles[i].getName()));
						f.renameTo(new File(Utility.normalizePath(folderDirectory + DIR_SEPERATOR + "old" + listOfFiles[i].getName())));
					}
				}
				folder.renameTo(new File(Utility.normalizePath(folderDirectory.substring(0, folderDirectory.indexOf("db\\") + 3)
								+ "old" + folderDirectory.substring(folderDirectory.indexOf("db\\") + 3))));

				String smssDirectory = importSMSSNameField.getText();
				String smssName = smssDirectory.substring(smssDirectory.indexOf("db\\") + 3);
				String normalizedSmssDirectory = Utility.normalizePath(smssDirectory);
				Path path = Paths.get(normalizedSmssDirectory);
				Charset charset = StandardCharsets.UTF_8;
				try {
					String content = new String(Files.readAllBytes(path), charset);
					content = content.replaceAll(smssName.substring(0, smssName.indexOf(".")),
							"old" + smssName.substring(0, smssName.indexOf(".")));
					Files.write(path, content.getBytes(charset));
				} catch (IOException exception) {
					logger.error(Arrays.toString(exception.getStackTrace()));
				}
				File smssFile = new File(normalizedSmssDirectory);
				smssFile.renameTo(new File(Utility.normalizePath(
						smssDirectory.substring(0, smssDirectory.indexOf("db\\") + 3) + "old" + smssName)));

				doneLabel.setText("Name change is done.");
				logger.debug("Name change is done.");
			}
		});
		window.setVisible(true);
	}

	public static void main(String[] args) {
		DBRenamer ui = new DBRenamer();
	}

}