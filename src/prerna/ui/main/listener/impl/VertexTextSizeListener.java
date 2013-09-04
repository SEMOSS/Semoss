package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Iterator;

import javax.swing.JButton;

import prerna.om.DBCMVertex;
import prerna.ui.transformer.VertexLabelFontTransformer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.picking.PickedState;

public class VertexTextSizeListener implements ActionListener{

	VertexLabelFontTransformer transformer;
	VisualizationViewer viewer;
	
	public VertexTextSizeListener(){
		
	}
	
	public void setTransformer(VertexLabelFontTransformer trans){
		transformer = trans;
	}
	
	public void setViewer(VisualizationViewer v){
		viewer = v;
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		//get selected vertices
		PickedState <DBCMVertex> ps = viewer.getPickedVertexState();
		
		JButton button = (JButton) e.getSource();
		
		//if no vertices are selected, perform action on all vertices
		if(ps.getPicked().size()==0){
			// TODO Auto-generated method stub
			
			//just have to check if the button is to increase font size or decrease.
			//need to check bounds on how high/low the font size can get
			if(button.getName().contains("Increase"))
				transformer.increaseFontSize();
			else if(button.getName().contains("Decrease"))
				transformer.decreaseFontSize();
		}
		//else if vertices have been selected, apply action only to those vertices
		else{
			Iterator <DBCMVertex> it = ps.getPicked().iterator();
			while(it.hasNext()){
				DBCMVertex vert = it.next();
				String URI = vert.getURI();
				if(button.getName().contains("Increase"))
					transformer.increaseFontSize(URI);
				else if(button.getName().contains("Decrease"))
					transformer.decreaseFontSize(URI);
			}
			
		}
		
		viewer.repaint();
	}

}
