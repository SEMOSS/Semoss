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
package prerna.ui.swing.custom;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Paint;
import java.awt.RenderingHints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.Icon;
import javax.swing.JLabel;
import javax.swing.JToggleButton;
import javax.swing.SwingUtilities;

import aurelienribon.ui.components.AruiRules;
import aurelienribon.ui.css.StyleProcessor;
import aurelienribon.ui.css.StyleRuleSet;
import aurelienribon.ui.css.swing.PaintUtils;
import aurelienribon.ui.css.swing.SwingUtils;

/**
 * This class has additional functionalities that extend those of the JToggleButton.
 */
public class ToggleButton extends JToggleButton {
	public static final StyleProcessor PROCESSOR = new StyleProcessor() {
		@Override public void process(Object target, StyleRuleSet rs) {
			if (target instanceof ToggleButton) {
				ToggleButton t = (ToggleButton) target;
				if (rs.contains(AruiRules.FOREGROUND_MOUSEDOWN)) t.foregroundMouseDown = SwingUtils.asColor(rs.getParams(AruiRules.FOREGROUND_MOUSEDOWN), 0);
				if (rs.contains(AruiRules.FOREGROUND_MOUSEOVER)) t.foregroundMouseOver = SwingUtils.asColor(rs.getParams(AruiRules.FOREGROUND_MOUSEOVER), 0);
				if (rs.contains(AruiRules.STROKE)) t.stroke = SwingUtils.asColor(rs.getParams(AruiRules.STROKE), 0);
				if (rs.contains(AruiRules.STROKE_MOUSEDOWN)) t.strokeMouseDown = SwingUtils.asColor(rs.getParams(AruiRules.STROKE_MOUSEDOWN), 0);
				if (rs.contains(AruiRules.STROKE_MOUSEOVER)) t.strokeMouseOver = SwingUtils.asColor(rs.getParams(AruiRules.STROKE_MOUSEOVER), 0);
				if (rs.contains(AruiRules.FILL)) t.fill = SwingUtils.asPaint(rs.getParams(AruiRules.FILL), 0);
				if (rs.contains(AruiRules.FILL_MOUSEDOWN)) t.fillMouseDown = SwingUtils.asPaint(rs.getParams(AruiRules.FILL_MOUSEDOWN), 0);
				if (rs.contains(AruiRules.FILL_MOUSEOVER)) t.fillMouseOver = SwingUtils.asPaint(rs.getParams(AruiRules.FILL_MOUSEOVER), 0);
				if (rs.contains(AruiRules.CORNERRADIUS)) t.cornerRadius = rs.asInteger(AruiRules.CORNERRADIUS, 0);
				t.reload();
			}
		}
	};

	private Color foregroundMouseDown = Color.RED;
	private Color foregroundMouseOver = Color.RED;
	private Color stroke = Color.RED;
	private Color strokeMouseDown = Color.RED;
	private Color strokeMouseOver = Color.RED;
	private Paint fill = Color.RED;
	private Paint fillMouseDown = Color.RED;
	private Paint fillMouseOver = Color.RED;
	private int cornerRadius = 0;

	private final JLabel label = new JLabel();
	private boolean isMouseDown = false;
	private boolean isMouseOver = false;

	/**
	 * Creates a toggle button.
	 * @param text String		Text on the toggle button.
	 */
	public ToggleButton(String text) {
		setOpaque(false);
		setLayout(new BorderLayout());
		add(label, BorderLayout.CENTER);
		addMouseListener(mouseAdapter);
		addActionListener(changeAdapter);
		setText(text);
		reload();
	}

	/**
	 * Reloads the button.
	 */
	private void reload() {
		label.setHorizontalAlignment(getHorizontalAlignment());
		label.setVerticalAlignment(getVerticalAlignment());
		label.setFont(getFont());

		if (isMouseDown && isMouseOver) {
			label.setForeground(foregroundMouseDown);
		} else if (isMouseOver || (isMouseDown && !isMouseOver)) {
			label.setForeground(foregroundMouseOver);
		} else {
			label.setForeground(getForeground());
		}

		revalidate();
		repaint();
	}

	/**
	 * Sets text on button.
	 * @param text String		Text for button.
	 */
	@Override
	public void setText(String text) {
		super.setText(text);
		if (label != null) label.setText(text);
	}

	/**
	 * Sets the icon for a button.
	 * @param defaultIcon Icon		Icon to be set.
	 */
	@Override
	public void setIcon(Icon defaultIcon) {
		super.setIcon(defaultIcon);
		if (label != null) label.setIcon(defaultIcon);
	}

	/**
	 * Sets the foreground.
	 * @param fg Color		Color to be painted on button.
	 */
	@Override
	public void setForeground(Color fg) {
		super.setForeground(fg);
		if (label != null) label.setForeground(fg);
	}

	/**
	 * Sets horizontal alignment of the text.
	 * @param alignment int		Value of alignment.
	 */
	@Override
	public void setHorizontalAlignment(int alignment) {
		super.setHorizontalAlignment(alignment);
		if (label != null) label.setHorizontalAlignment(alignment);
	}

	/**
	 * Sets vertical alignment of the text.
	 * @param alignment int		Value of alignment.
	 */
	@Override
	public void setVerticalAlignment(int alignment) {
		super.setVerticalAlignment(alignment);
		if (label != null) label.setVerticalAlignment(alignment);
	}

	/**
	 * Paints the components of the button.
	 * @param g Graphics		Graphic to be displayed.
	 */
	@Override
	protected void paintComponent(Graphics g) {
		Graphics2D gg = (Graphics2D) g;
		gg.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

		int w = getWidth();
		int h = getHeight();

		if (isMouseDown && isMouseOver) {
			gg.setPaint(PaintUtils.buildPaint(fillMouseDown, w, h));
			gg.fillRoundRect(0, 0, w, h, cornerRadius, cornerRadius);
			gg.setColor(strokeMouseDown);
			gg.drawRoundRect(0, 0, w-1, h-1, cornerRadius, cornerRadius);

		} else if (isMouseOver || (isMouseDown && !isMouseOver)) {
			gg.setPaint(PaintUtils.buildPaint(fillMouseOver, w, h));
			gg.fillRoundRect(0, 0, w, h, cornerRadius, cornerRadius);
			gg.setColor(strokeMouseOver);
			gg.drawRoundRect(0, 0, w-1, h-1, cornerRadius, cornerRadius);

		} else {
			gg.setPaint(PaintUtils.buildPaint(fill, w, h));
			gg.fillRoundRect(0, 0, w, h, cornerRadius, cornerRadius);
			gg.setColor(stroke);
			gg.drawRoundRect(0, 0, w-1, h-1, cornerRadius, cornerRadius);
		}
	}

	private final MouseAdapter mouseAdapter = new MouseAdapter() {
		@Override
		public void mouseEntered(MouseEvent e) {
			isMouseOver = true;
			reload();
		}
		@Override
		public void mouseExited(MouseEvent e) {
			if (!isSelected())
			{
				isMouseOver = false;
				reload();
			}
		}
		@Override
		public void mousePressed(MouseEvent e) {
			if (SwingUtilities.isLeftMouseButton(e)) {
				isMouseDown = true;
				reload();
			}
		}
		@Override
		public void mouseReleased(MouseEvent e) {

		}
	};
	
	/**
	 * Set selected.
	 * @param boo boolean		Reloads button if clicked.
	 */
	@Override
	public void setSelected(boolean boo)
	{
		super.setSelected(boo);
		if (boo==false)
		{
			isMouseDown=false;
			reload();
		}
	}
	
	/**
	 * Programatically perform a click. Reloads the button.
	 */
	@Override
	public void doClick(){
		super.doClick();
		isMouseOver=true;
		isMouseDown=true;
		reload();
	}
	
	
	private final ActionListener changeAdapter = new ActionListener() {

		@Override
		public void actionPerformed(ActionEvent arg0) {
			if (isSelected())
			{
				isMouseDown=true;
				reload();
			}
			else
			{
				isMouseDown=false;
				reload();
			}
		}

	};
}
