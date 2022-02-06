package prerna.om;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.gson.GsonUtility;

public class PixelList implements Iterable<Pixel> {

	private static final Logger logger = LogManager.getLogger(PixelList.class);

//	private AtomicInteger counter = new AtomicInteger(0);
	private List<Pixel> pixelList = null;
	private transient Map<String, Integer> idToIndexHash = null;
	// frameName -> list of pixels
	private transient Map<String, LinkedList<Pixel>> frameDependency = null;
	// panel -> layer -> list of pixels
	private transient Map<String, Map<String, LinkedList<Pixel>>> taskDependency = null;

	public PixelList() {
		this(500);
	}
	
	public PixelList(int capacity) {
		// keeping the initial capacity initially at 500
		capacity = capacity > 500 ? capacity : 500;
		this.pixelList = new Vector<>(capacity);
		this.idToIndexHash = new ConcurrentHashMap<>(capacity);
		int subCapacity = capacity / 10;
		this.frameDependency = new ConcurrentHashMap<>(subCapacity);
		this.taskDependency = new ConcurrentHashMap<>(subCapacity);
	}
	
	/**
	 * Add a pixel directly to the list
	 * @param p
	 */
	public void addPixel(Pixel p) {
		pixelList.add(p);
		idToIndexHash.put(p.getId(), this.pixelList.size()-1);
		syncLastPixel();
		// increment the counter
//		counter.getAndIncrement();
	}
	
	public void syncLastPixel() {
		// now store the frame dependency
		Pixel p = pixelList.get(pixelList.size()-1);
		Pixel pixelToUtilize = p;
		
		// TODO: should keep track of which steps are code execution 
		// so we dont loop through all the pixels before trying to consolidate
		// can we consolidate the pixels?
		if(p.isCodeExecution() && pixelList.size()>1) {
			// will try to consolidate even if there are visualization pixels
			// that are not data based
			int counter = 2;
			LAST_DATA_LOOP : while(pixelList.size() - counter >= 0) {
				Pixel prevPixel = pixelList.get(pixelList.size()-counter);
				counter++;
				if(prevPixel.isCodeExecution() && prevPixel.isUserScript()
						&& p.getLanguage() == prevPixel.getLanguage()
						&& ( p.getLanguage() == Variable.LANGUAGE.R
							|| p.getLanguage() == Variable.LANGUAGE.PYTHON )
						) {
					// we can combine these!
					String prevCodeExecution = prevPixel.getCodeExecuted();
					String newCodeExecution = p.getCodeExecuted();
					
					String combined = prevCodeExecution + "\n" + newCodeExecution;
					StringBuilder newPixelString = new StringBuilder();
					if(p.getLanguage() == Variable.LANGUAGE.R) {
						newPixelString.append("R(\"<encode>");
					} else {
						newPixelString.append("Py(\"<encode>");
					}
					newPixelString.append(combined);
					newPixelString.append("</encode>\");");
					prevPixel.setPixelString(newPixelString.toString());
					prevPixel.setCodeDetails(true, combined, prevPixel.getLanguage(), true);
					
					// now remove p from the list
					pixelList.remove(pixelList.size()-1);
					// and change the pixelToUtilize reference
					pixelToUtilize = prevPixel;
					break LAST_DATA_LOOP;
				}
				
				// if this is a data operation
				// we need to break out of this loop
				if(prevPixel.isDataOperation()) {
					break LAST_DATA_LOOP;
				}
			}
		}
		
		// store frame output
		Set<String> frameOutputs = p.getFrameOutputs();
		for(String frameName : frameOutputs) {
			if(frameDependency.containsKey(frameName)) {
				frameDependency.get(frameName).addLast(pixelToUtilize);
			} else {
				LinkedList<Pixel> dll = new LinkedList<>();
				dll.addFirst(p);
				frameDependency.put(frameName, dll);
			}
		}
		
		// now we also want to keep track of querying / filtering
		Set<String> frameInputs = p.getFrameInputs();
		for(String frameName : frameInputs) {
			// do not double count..
			if(!frameOutputs.contains(frameName)) {
				if(frameDependency.containsKey(frameName)) {
					// make sure we are not adding the same reference twice
					if(frameDependency.get(frameName).getLast() != pixelToUtilize) {
						frameDependency.get(frameName).addLast(pixelToUtilize);
					}
				} else {
					logger.info("Super weird... this is a frame input for pixel but doesn't exist as a previous frame output");
					LinkedList<Pixel> dll = new LinkedList<>();
					dll.addFirst(pixelToUtilize);
					frameDependency.put(frameName, dll);
				}
			}
		}
		
		// we also want to keep track of the tasks on each panel
		List<TaskOptions> listTaskOptions = p.getTaskOptions();
		for(TaskOptions taskOption : listTaskOptions) {
			Set<String> panelIds = taskOption.getPanelIds();
			for(String panelId : panelIds) {
				String layerId = taskOption.getPanelLayerId(panelId);
				if(layerId == null) {
					layerId = "0";
				}
				
				Map<String, LinkedList<Pixel>> panelMap = null;
				if(taskDependency.containsKey(panelId)) {
					panelMap = taskDependency.get(panelId);
				} else {
					panelMap = new ConcurrentHashMap<>();
					taskDependency.put(panelId, panelMap);
				}
				
				if(panelMap.containsKey(layerId)) {
					// make sure we are not adding the same reference twice
					if(panelMap.get(layerId).getLast() != pixelToUtilize) {
						panelMap.get(layerId).addLast(pixelToUtilize);
					}
				} else {
					LinkedList<Pixel> dll = new LinkedList<>();
					dll.addFirst(pixelToUtilize);
					panelMap.put(layerId, dll);
				}
			}
		}
	}
	
	/**
	 * Add a pixel step to the recipe
	 * @param pixelString
	 * @return
	 */
	public Pixel addPixel(String pixelString) {
		List<String> pixelRecipe = new Vector<>();
		pixelRecipe.add(pixelString);
		List<Pixel> pList = addPixel(pixelRecipe);
		return pList.get(0);
	}
	
	/**
	 * Add the pixel at a specific location in the recipe
	 * @param index
	 * @param pixelString
	 * @return
	 */
	public Pixel addPixel(int index, String pixelString) {
		List<String> pixelRecipe = new Vector<>();
		pixelRecipe.add(pixelString);
		List<Pixel> pList = addPixel(pixelRecipe);
		return pList.get(0);
	}

	/**
	 * Add a list of pixel steps to the recipe
	 * @param pixelRecipe
	 * @return
	 */
	public List<Pixel> addPixel(List<String> pixelRecipe) {
		synchronized(this) {
			List<Pixel> subset = new Vector<>();
			for(int i = 0; i < pixelRecipe.size(); i++) {
				int intVal = this.pixelList.size();//counter.getAndIncrement();
				String uid = intVal + "";// + "__" + UUID.randomUUID().toString();
				Pixel pixel = new Pixel(uid, pixelRecipe.get(i));
				this.addPixel(pixel);
				// return the pixel object that was added
				subset.add(pixel);
			}
			return subset;
		}
	}
	
	public Pixel get(int index) {
		return pixelList.get(index);
	}
	
	public List<String> getPixelRecipe() {
		List<String> pixelRecipe = new Vector<>(pixelList.size());
		for(Pixel p : pixelList) {
			pixelRecipe.add(p.getPixelString());
		}
		return pixelRecipe;
	}
	
	public List<String> getPixelIds() {
		List<String> pixelIds = new Vector<>(pixelList.size());
		for(Pixel p : pixelList) {
			pixelIds.add(p.getId());
		}
		return pixelIds;
	}
	
	public List<String> getNonMetaPixelIds() {
		List<String> pixelIds = new Vector<>(pixelList.size());
		for(Pixel p : pixelList) {
			if(!p.isMeta()) {
				pixelIds.add(p.getId());
			}
		}
		return pixelIds;
	}
	
	public List<Map<String, Object>> getPixelPositions() {
		List<Map<String, Object>> pixelPositions = new Vector<>(pixelList.size());
		for(Pixel p : pixelList) {
			pixelPositions.add(p.getPositionMap());
		}
		return pixelPositions;
	}
	
	public List<Map<String, Object>> getNonMetaPixelPositions() {
		List<Map<String, Object>> pixelPositions = new Vector<>(pixelList.size());
		for(Pixel p : pixelList) {
			if(!p.isMeta()) {
				pixelPositions.add(p.getPositionMap());
			}
		}
		return pixelPositions;
	}
	
	/**
	 * Set the pixel id for each step in the recipe
	 * @param pixelIds
	 */
	public void updateAllPixelIds(List<String> pixelIds) {
		if(pixelIds.size() != this.pixelList.size()) {
			throw new IllegalArgumentException("Array size must match current recipe size");
		}
		for(int i = 0; i < pixelIds.size(); i++) {
			pixelList.get(i).setId(pixelIds.get(i));
		}
		// recalculate the id to index hash now
		recalculateIdToIndexHash();
	}
	
	/**
	 * Set the pixel id for each step in the recipe
	 * @param pixelIds
	 */
	public void updateAllPixelPositions(List<Map<String, Object>> pixelPositions) {
		if(pixelPositions.size() != this.pixelList.size()) {
			throw new IllegalArgumentException("Array size must match current recipe size");
		}
		for(int i = 0; i < pixelPositions.size(); i++) {
			pixelList.get(i).setPositionMap(pixelPositions.get(i));
		}
	}
	
	/**
	 * Set both the pixelIds and pixelPositions
	 * @param pixelIds
	 * @param pixelPositions
	 */
	public void updateAllIdsAndPositions(List<String> pixelIds, List<Map<String, Object>> pixelPositions) {
		if(pixelIds.size() != this.pixelList.size() && pixelPositions.size() != this.pixelList.size()) {
			throw new IllegalArgumentException("Array size must match current recipe size");
		}
		for(int i = 0; i < pixelPositions.size(); i++) {
			pixelList.get(i).setId(pixelIds.get(i));
			pixelList.get(i).setPositionMap(pixelPositions.get(i));
		}
		// recalculate the id to index hash now
		recalculateIdToIndexHash();
	}

	/**
	 * Get the Pixel object if found in the list
	 * @param pixelId
	 * @return
	 */
	public Pixel getPixel(String pixelId) {
		for(int i = 0; i < pixelList.size(); i++) {
			Pixel p = pixelList.get(i);
			if(p.getId().equals(pixelId)) {
				return p;
			}
		}
		return null;
	}
	
	/**
	 * Find the index for the pixel id
	 * @param pixelId
	 * @return
	 */
	public int findIndex(String pixelId) {
		for(int i = 0; i < pixelList.size(); i++) {
			if(pixelList.get(i).getId().equals(pixelId)) {
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * Delete the pixel ids and propagate the deletion of frame dependencies
	 * @param pixelIds
	 */
	public List<Integer> removeIds(List<String> pixelIds, boolean propogate) {
		synchronized(this.pixelList) {
			Map<String, Integer> idToIndex = getIdToIndexHash();
			List<Integer> indices = new Vector<Integer>(pixelIds.size());
			for(String pId : pixelIds) {
				Integer index = idToIndex.get(pId);
				if(index == null) {
					// error
					// recalculate the hash
					recalculateIdToIndexHash();
					throw new IllegalArgumentException("Cannot find pixel step with id = " + pId);
				}
				indices.add(index);
			}
			
			// sort the index list from largest to smallest
			Collections.sort(indices, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
				    return o2.intValue()-o1.intValue();
				}
			});
			
			// now find all the downstream indices
			// that are affected by this deletion
			// from a frame perspective
			
			Set<Integer> downstreamIndex = new HashSet<>();
			if(propogate) {
				for(int i = 0; i < indices.size(); i++) {
					Pixel p = this.pixelList.get(indices.get(i).intValue());
					for(String frameName : p.getFrameOutputs()) {
						LinkedList<Pixel> dll = frameDependency.get(frameName);
						Pixel lastP = dll.removeLast();
						while( lastP != null ) {
							// grab the index of last P
							// we will have to delete this guy
							downstreamIndex.add(idToIndex.get(lastP.getId()));
							
							// condition on when to stop
							// either we hit the pixel p
							// or p is the creator of this frame
							if(lastP.equals(p)
									|| dll.isEmpty()) {
								lastP = null;
							} else {
								lastP = dll.removeLast();
							}
						}
						
						// remove the frame index entirely
						if(dll.isEmpty()) {
							frameDependency.remove(frameName);
						}
					}
				}
			}
			
			// now merge the lists into the set
			// so it is all distinct values
			downstreamIndex.addAll(indices);
			// and now remove all 
			// but need to remove from largest index
			// to smallest index
			List<Integer> finalList = new Vector<>();
			finalList.addAll(downstreamIndex);
			Collections.sort(finalList, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
				    return o2.intValue()-o1.intValue();
				}
			});
			
			for(Integer index : finalList) {
				Pixel p = this.pixelList.remove(index.intValue());
				logger.info("Dropping from recipe " + p);
			}
			
			// recalculate the hash
			reorganizePixelIds();
			recalculateIdToIndexHash();
			return indices;
		}
	}
	
	/**
	 * Recalculate the index hash
	 */
	public void reorganizePixelIds() {
		for(int i = 0; i < this.pixelList.size(); i++) {
			pixelList.get(i).setId(i + "");
		}
	}
	
	/**
	 * Get the id to index hash
	 * @return
	 */
	public Map<String, Integer> getIdToIndexHash() {
		if(idToIndexHash == null || idToIndexHash.isEmpty()) {
			recalculateIdToIndexHash();
		}
		return idToIndexHash;
	}
	
	/**
	 * Recalculate the index hash
	 */
	public void recalculateIdToIndexHash() {
		if(idToIndexHash == null) {
			idToIndexHash = new ConcurrentHashMap<>();
		} else {
			idToIndexHash.clear();
		}
		for(int i = 0; i < pixelList.size(); i++) {
			idToIndexHash.put(pixelList.get(i).getId(), i);
		}
	}
	
	/**
	 * Find the last pixel used to paint this panel/layer combination
	 * That was not a refresh panel task
	 * @param panelId
	 * @param layerId
	 * @return
	 */
	public Pixel findLastPixelViewNotRefresh(String panelId, String layerId) {
		Map<String, LinkedList<Pixel>> panelMap = this.taskDependency.get(panelId);
		if(panelMap == null) {
			return null;
		}
		LinkedList<Pixel> listPixel = panelMap.get(layerId);
		if(listPixel == null) {
			return null;
		}
		
		// loop from end to beginning to find the last pixel that isn't a refresh
		Pixel foundPixel = null;
		Iterator<Pixel> iterator = listPixel.descendingIterator();
		while(iterator.hasNext()) {
			foundPixel = iterator.next();
			if(!foundPixel.isRefreshPanel()) {
				break;
			}
		}
		return foundPixel;
	}
	
	public PixelList copy() {
		Gson gson = GsonUtility.getDefaultGson();
		String strCopy = gson.toJson(this);
		PixelList copy = gson.fromJson(strCopy, PixelList.class);
		return copy;
	}
	
//	/**
//	 * Get the counter value
//	 * @return
//	 */
//	public int getCounter() {
//		return counter.intValue();
//	}
//	
//	/**
//	 * Set the counter value
//	 * @param value
//	 */
//	public void setCounter(int value) {
//		this.counter = new AtomicInteger(value);
//	}
	
	////////////////////////////////////////////////////////////
	
	/*
	 * General methods
	 */
	
	/**
	 * Wrapper method for the List<Pixel> contained in the object
	 * @return
	 */
	public boolean isEmpty() {
		return this.pixelList.isEmpty();
	}
	
	/**
	 * Wrapper method for the List<Pixel> contained in the object
	 * @return
	 */
	public void clear() {
		this.pixelList.clear();
		this.idToIndexHash.clear();
		this.frameDependency.clear();
		this.taskDependency.clear();
	}
	
	/**
	 * Wrapper method for the List<Pixel> contained in the object
	 * @return
	 */
	public int size() {
		return this.pixelList.size();
	}

	@Override
	public Iterator<Pixel> iterator() {
		return this.pixelList.iterator();
	}
	
}
