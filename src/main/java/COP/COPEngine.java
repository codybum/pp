package COP;


import CP.CPEngine;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

public class COPEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;

	public COPEngine(Launcher plugin)
	{
		this.logger = new CLogger(COPEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;

	}
	 public void run() {
	        try 
	        {
	        	logger.debug("Start Data GEN");
	        	int regionCount = 100;
	        	int agentCount = 100;
	        	int pluginCount = 100;

	        	for(int i = 0; i < regionCount; i++) {
					//System.out.println("Start AddRegion1" + i);
					for(int ii = 0; ii < agentCount; ii++) {
						//System.out.println("Start AddRegion2" + i + " " + ii);

						for(int iii = 0; iii < pluginCount; iii++) {
							//System.out.println("Start AddRegion3" + i + " " + ii + " " + iii);
							//plugin.getGDB().gdb.addNode("region-" + i, "agent-" + ii, "plugin/" + iii);

						}

					}

				}

	        }
		   catch(Exception ex)
		   {
	            logger.error(plugin.getStringFromError(ex));
	       }
	    }  
}
