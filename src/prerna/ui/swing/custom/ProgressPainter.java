/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.ui.swing.custom;
import java.awt.Color;
import java.awt.GradientPaint;
import java.awt.Graphics2D;
import java.awt.RenderingHints;

import javax.swing.Painter;

/**
 * This class uses Painter in order to create a progress bar.
 */
public class ProgressPainter implements Painter {

private Color light, dark;
private GradientPaint gradPaint;

/**
 * Constructor used to create progress bars.
 * @param light Color		Light color appearing on the bar.
 * @param dark Color		Dark color appearing on the bar.
 */
public ProgressPainter(Color light, Color dark) {
    this.light = light;
    this.dark = dark;
}

/**
 * Paints the progress bar.
 * @param g Graphics2D		Allows for more sophisticated graphics to be drawn.
 * @param c Object			Color of bar.
 * @param w int				Width of bar.
 * @param h int				Height of bar.
 */
@Override
public void paint(Graphics2D g, Object c, int w, int h) {
    g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
    gradPaint = new GradientPaint((w / 2.0f), 0, light, (w / 2.0f), (h /2.0f), dark, true);
    g.setPaint(gradPaint);
    g.fillRect(2, 2, (w - 5), (h - 5));

    Color outline = new Color(0, 85, 0);
    g.setColor(outline);
    g.drawRect(2, 2, (w - 5), (h - 5));
    Color trans = new Color(outline.getRed(), outline.getGreen(), outline.getBlue(), 100);
    g.setColor(trans);
    g.drawRect(1, 1, (w - 3), (h - 3)); 
}
}
