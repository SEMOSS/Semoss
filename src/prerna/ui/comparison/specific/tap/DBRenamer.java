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

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import prerna.util.Constants;

public class DBRenamer
{
	private static final int WIDTH = 600;
	private static final int HEIGHT = 250;
	
	JFrame window = new JFrame();
	JPanel dbImportPanel = new JPanel();
	JLabel doneLabel = new JLabel();
	private JTextField importFolderNameField = new JTextField();
	private JTextField importSMSSNameField = new JTextField();
	
	public DBRenamer()
	{
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
		folderBrowseButton.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				doneLabel.setText("");
				JFileChooser jfc = new JFileChooser();
				jfc.setCurrentDirectory(new java.io.File("."));
				jfc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				int retVal = jfc.showOpenDialog((JComponent) e.getSource());
				// Handle open button action.
				if (retVal == JFileChooser.APPROVE_OPTION)
				{
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
		smssBrowseButton.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				doneLabel.setText("");
				JFileChooser jfc = new JFileChooser();
				jfc.setCurrentDirectory(new java.io.File("."));
				int retVal = jfc.showOpenDialog((JComponent) e.getSource());
				// Handle open button action.
				if (retVal == JFileChooser.APPROVE_OPTION)
				{
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
		changeNameButton.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				String folderDirectory = importFolderNameField.getText();
				File folder = new File(folderDirectory);
				File[] listOfFiles = folder.listFiles();
				for (int i = 0; i < listOfFiles.length; i++)
				{
					if (listOfFiles[i].isFile())
					{
						File f = new File(folderDirectory + "\\" + listOfFiles[i].getName());
						f.renameTo(new File(folderDirectory + "\\old" + listOfFiles[i].getName()));
					}
				}
				folder.renameTo(new File(folderDirectory.substring(0, folderDirectory.indexOf("db\\") + 3) + "old"
						+ folderDirectory.substring(folderDirectory.indexOf("db\\") + 3)));

				String smssDirectory = importSMSSNameField.getText();
				String smssName = smssDirectory.substring(smssDirectory.indexOf("db\\") + 3);
				Path path = Paths.get(smssDirectory);
				Charset charset = StandardCharsets.UTF_8;
				try{
				String content = new String(Files.readAllBytes(path), charset);
					content = content.replaceAll(smssName.substring(0, smssName.indexOf(".")), "old" + smssName.substring(0, smssName.indexOf(".")));
				Files.write(path, content.getBytes(charset));
				} catch(IOException exception) {
					exception.printStackTrace();
				}
				File smssFile = new File(smssDirectory);
				smssFile.renameTo(new File(smssDirectory.substring(0, smssDirectory.indexOf("db\\") + 3) + "old" + smssName));
				
				doneLabel.setText("Name change is done.");
				System.out.println("Name change is done.");
			}
		});
		window.setVisible(true);
	}
	
	public static void main(String[] args)
	{
		DBRenamer ui = new DBRenamer();
	}
	
}