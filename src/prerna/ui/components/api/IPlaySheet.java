package prerna.ui.components.api;

import javax.swing.JDesktopPane;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ParamPanel;
import prerna.ui.components.VertexFilterData;

import com.hp.hpl.jena.rdf.model.Model;

// primary interface for all of the playsheets
// graph play sheet is only one type of a playsheet
/**
 * This interface class is used to define the basic functionality for all play sheet classes.  A play sheet is loosely defined
 * as any class that displays data on the the main PlayPane Desktop.  The data for a play sheet can be pulled directly from
 * an engine or it can be set in the form of a Jena Model.
 * <p>
 * The functions associated with this interface revolve around specifying the data to display, creating the visualization, 
 * and defining the play sheet.
 */
public interface IPlaySheet extends Runnable{

	// sets the query
	/**
	 * Sets the String version of the SPARQL query on the play sheet. <p> The query must be set before creating the model for
	 * visualization.  Thus, this function is called before createView(), extendView(), overlayView()--everything that 
	 * requires the play sheet to pull data through a SPARQL query.
	 * @param query the full SPARQL query to be set on the play sheet
	 * @see	#createView()
	 * @see #extendView()
	 * @see #overlayView()
	 */
	public void setQuery(String query);
	
	// gets the query
	/**
	 * Gets the latest query set to the play sheet.  <p> If multiple queries have been set to the specific play sheet through
	 * Extend or Overlay, the function will return the last query set to the play sheet.
	 * @return the SPARQL query previously set to this play sheet
	 * @see #extendView()
	 * @see #overlayView()
	 */
	public String getQuery();
	
	// sets the model
	/**
	 * Sets the Jena Model on the play sheet. <p> This should be used only if the model to be visualized has already been created
	 * and not if the play sheet must still create the model from a SPARQL query. The function {@link #recreateView()} uses 
	 * a preset Jena Model to create the visualization and display it.
	 * @param jenaModel the Jena Model to be used as the data behind the play sheet visualization
	 * @see #recreateView()
	 */
	public void setJenaModel(Model jenaModel);
	
	// gets the model
	/**
	 * Returns the Jena Model associated with the play sheet.  This Jena Model could have been created through 
	 * functions like {@link #createView()} or it could have been set manually through functions like {@link #setJenaModel(Model)}
	 * @return the Jena Model associated with the play sheet
	 */
	public Model getJenaModel();
	
	// sets the desktop pane for paining
	/**
	 * Sets the JDesktopPane to display the play sheet on. <p> This must be set before calling functions like 
	 * {@link #createView()} or {@link #extendView()}, as functions like these add the panel to the desktop pane set in this
	 * function.
	 * @param pane the desktop pane that the play sheet is to be displayed on
	 */
	public void setJDesktopPane(JDesktopPane pane);
	
	// sets a unique question number
	/**
	 * Sets the identification code for the question associated with the play sheet.  The question ID is what can be used in 
	 * conjunction with the specified engine's properties file to get the SPARQL query associated with the question as well 
	 * as the question String and play sheet class that the question is to be run on.
	 * @param id representation of the question as laid out in the specified engine's question file
	 */
	public void setQuestionID(String id);
	
	// gets the question ID
	/**
	 * Returns the question identification code that has been set to the play sheet. The question identification code can be used
	 * in conjunction with the specified engine's properties file to get the SPARQL query associated with the question as well
	 * as the question String and the play sheet class that the question is to be run on.  Only one question ID can be set to 
	 * each play sheet, so after using {@link #extendView()} or {@link #overlayView()} the play sheet will have the most 
	 * recent question ID associated with it.
	 * @return questionID the identification code of the question associated with the play sheet
	 */
	public String getQuestionID();
	
	// creates the view
	/**
	 * This is the function that is used to create the first view 
	 * of any play sheet.  It often uses a lot of the variables previously set on the play sheet, such as {@link #setQuery(String)},
	 * {@link #setJDesktopPane(JDesktopPane)}, {@link #setRDFEngine(IEngine)}, and {@link #setTitle(String)} so that the play 
	 * sheet is displayed correctly when the view is first created.  It generally creates the model for visualization from 
	 * the specified engine, then creates the visualization, and finally displays it on the specified desktop pane
	 * 
	 * <p>This is the function called by the PlaysheetCreateRunner.  PlaysheetCreateRunner is the runner used whenever a play 
	 * sheet is to first be created, most notably in ProcessQueryListener.
	 */
	public void createView();
	
	// refines the existing view
	/**
	 * Recreates the visualizer and the repaints the play sheet without recreating the model or pulling anything 
	 * from the specified engine.
	 * This function is used when the model to be displayed has not been changed, but rather the visualization itself 
	 * must be redone.
	 * <p>This function takes into account the nodes that have been filtered either through FilterData or Hide Nodes so that these 
	 * do not get included in the recreation of the visualization.
	 */
	public void refineView();
	
	// extends the view
	/**
	 * Adds additional data to the model that is already associated with the play sheet.  This function often uses 
	 * the query and the engine set to the play sheet to pull additional data for its model before recreating its visualization 
	 * and repainting the play sheet.
	 * <p> This is the function used by the PlaysheetExtendRunner which is called rather than the PlaysheetCreateRunner when 
	 * the extend button is selected.  Additionally, PlaysheetExtendRunner is the runner called with Traverse Freely.
	 */
	public void extendView();
	
	// overlays the view
	/**
	 * This function is very similar to {@link #extendView()}.  Its adds additional data to the model already associated with 
	 * the play sheet.  The main difference between these two functions, though is {@link #overlayView()} is used to overlay 
	 * a query that could be unrelated to the data that currently exists in the play sheet's model whereas {@link #extendView()} 
	 * uses FilterNames to limit the results of the additional data so that it only add data relevant to the play sheet's current
	 *  model.
	 *  <p>This function is used by PlaysheetOverlayRunner which is called when the Overlay button is selected.
	 */
	public void overlayView();
	
	//recreates the view in another internal frame
	/**
	 * Uses the model already set to the play sheet to recreate the visualization and repaint the play sheet.  
	 * This function does not add any data to the model, but if a new Jena Model has been set to the play sheet, this function 
	 * can be used to display it.
	 * <p>This is the function called in PlaysheetRecreateRunner.
	 */
	public void recreateView();

	// need one more to remove view
	/**
	 * Similar to {@link #overlayView()} but rather than adding data to the play sheet's current model it 
	 * takes it away.  This function uses the query and the engine set to the play sheet to determine what data should be 
	 * removed from the play sheet's model.
	 * <p>This function is called in PlaysheetRemoveRunner
	 */
	public void removeView();
	
	// and one more to undo
	/**
	 * Removes from the play sheet's current model everything that was returned from the query the last time the 
	 * model was augmented.
	 */
	public void undoView();
	
	// redo the view - this is useful when the user selects custom data properties and object properties
	/**
	 * Uses the query and the engine associated with the play sheet to create the model, the visualization, and repaint the graph.  
	 * This is very similar to {@link #createView()} with the main difference being that this function does not add the panel or 
	 * do any of the visibility functionality that is necessary when first creating the play sheet.
	 */
	public void redoView();
	
	// sets the param panel
	/**
	 * Sets the param panel associated with the play sheet.  This param panel is used in ProcessQueryListner to fill the 
	 * query with the parameters selected by the user.
	 * @param panel the param panel from the PlayPane that is associated with the play sheet.
	 */
	public void setParamPanel(ParamPanel panel);
	
	// sets the RDF engine
	/**
	 * Sets the RDF engine for the play sheet to run its query against.  Can be any of the active engines, all of which are 
	 * stored in DIHelper
	 * @param engine the active engine for the play sheet to run its query against.
	 */
	public void setRDFEngine(IEngine engine);
	
	// sets the title
	/**
	 * Sets the title of the play sheet.  The title is displayed as the text on top of the internal frame that is the play sheet.
	 * @param title representative name for the play sheet.  Often a concatenation of the question ID and question text
	 */
	public void setTitle(String title);
	
	// gets the filter data
	/**
	 * Returns the VertexFilterData associated with the play sheet.  VertexFilterData is often created through functions like 
	 * {@link #createView()}, {@link #recreateView()} and {@link #overlayView()}.  It is representative of all data shown in the
	 *  visualization stored in a usable manner.
	 * @return VertexFilterData the filter data associated with the play sheet
	 */
	public VertexFilterData getFilterData();
	
	// sets sheet as active sheet in internal frame
	/**
	 * Sets the play sheet as selected within the desktop that it is displayed on
	 */
	public void setActiveSheet();
	
	
		
}
