package prerna.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;

public class TimedEngineCleanup {

	private static final Logger classLogger = LogManager.getLogger(TimedEngineCleanup.class);

    private static TimedEngineCleanup singleton = null;

	private final Map<String, IEngine> internalMap = new HashMap<>();
    private final Map<String, Timer> timers = new HashMap<>();

    private TimedEngineCleanup() {
    
    }
    
    public static TimedEngineCleanup getInstance() {
    	if(singleton != null) {
    		return singleton;
    	}
    	
    	if(singleton == null) {
    		synchronized (TimedEngineCleanup.class) {
				if(singleton != null) {
					return singleton;
				}
				
				singleton = new TimedEngineCleanup();
			}
    	}
    	
    	return singleton;
    }
    
    /**
     * 
     * @param key
     * @param engine
     * @param timeoutMillis
     */
    public void put(IEngine engine, long timeoutMillis) {
        String engineId = engine.getEngineId();
    	internalMap.put(engineId, engine);

        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                onRemove(engineId);
                timer.cancel();
            }
        }, timeoutMillis);
        
        
        if(this.timers.containsKey(engineId)) {
        	this.timers.remove(engineId).cancel();
        }
        this.timers.put(engineId, timer);
    }

    /**
     * Get the timer set for this engine
     * @param engine
     * @return
     */
    public Timer getEngineTimer(IEngine engine) {
    	return this.timers.get(engine.getEngineId());
    }
    
    /**
     * 
     * @param key
     */
    protected synchronized void onRemove(String engineId) {
    	// check that the engine is still loaded
    	IEngine engine = (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
    	if(engine == null) {
    		classLogger.info("Engine " + engineId + " has already been removed");
    		return;
    	}

    	// now try to actually remove from disk
		try {
    		classLogger.info("Deleting Engine " + engineId + " from disk without removing any cloud backup or metadata");
			engine.delete();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
    }

}