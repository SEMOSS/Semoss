package prerna.ui.helpers;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Vector;

import org.openrdf.repository.RepositoryConnection;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;

/**
 * This is the abstract helper class for managing the data frames associated with graph play sheet
 * Because there can be multiple data frame types (graph data model or tinker frame) the methods touching these have been abstracted out
 *
 */
public abstract class NetworkGraphHelper {

	abstract public boolean getSudowl();
	
	/**
	 * Method createView.
	 */
	abstract public void createView();
	
	/**
	 * Method undoView.
	 */
	abstract public void undoView();

	
    /**
     * Method redoView.
     */
	abstract public void redoView();

	abstract public void overlayView();
	
	/**
	 * Method removeView.
	 */
	abstract public void removeView();

	
	/**
	 * Method refineView.
	 */
	abstract public void refineView();
	
	/**
	 * Method createForest.
	 */
	abstract public void createForest();
	
	/**
	 * Method exportDB.
	 */
	abstract public void exportDB() ;
	
	/**
	 * Method setUndoRedoBtn.
	 */
	abstract public void setUndoRedoBtn();
	
	/**
	 * Method setRC.
	 * @param rc RepositoryConnection
	 */
	abstract public void setRC(RepositoryConnection rc);
	
	/**
	 * Method getRC.
	 * @param rc RepositoryConnection
	 */
	abstract public RepositoryConnection getRC();
	
	// removes existing concepts 
	/**
	 * Method removeExistingConcepts.
	 * @param subVector Vector<String>
	 */
	abstract public void removeExistingConcepts(Vector <String> subVector);
	
	// adds existing concepts 
	/**
	 * Method addNewConcepts.
	 * @param subjects String
	 * @param baseObject String
	 * @param predicate String
	 * @return String
	 */
	abstract public String addNewConcepts(String subjects, String baseObject, String predicate);

	abstract public void clearStores();

	abstract public Collection<SEMOSSVertex> getVerts();

	abstract public Collection<SEMOSSEdge> getEdges();

	abstract public Hashtable<String, SEMOSSEdge> getEdgeStore();

	abstract public Hashtable<String, SEMOSSVertex> getVertStore();

}
