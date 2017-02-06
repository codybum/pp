package PP;


import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.UUID;

public class PPEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
	public String ppId = null;
	public boolean incomingActive = false;
	public PPoutgoing sendout;

	public PPEngine(Launcher plugin)
	{
		this.logger = new CLogger(PPEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		//ppId = UUID.randomUUID().toString();
		ppId = "cat";
		sendout = new PPoutgoing(plugin,this);
	}
	 public void run() {
	        try 
	        {
	        	new Thread(new PPIncoming(plugin,this)).start();

	        	while(plugin.isActive) {

					logger.debug("Start Data GEN");
					int regionCount = 10;
					int agentCount = 10;
					int pluginCount = 10;

					for(int i = 0; i < regionCount; i++) {
						//System.out.println("Start AddRegion1" + i);
						for(int ii = 0; ii < agentCount; ii++) {
							//System.out.println("Start AddRegion2" + i + " " + ii);

							for(int iii = 0; iii < pluginCount; iii++) {
								//System.out.println("Start AddRegion3" + i + " " + ii + " " + iii);
								//plugin.getGDB().gdb.addNode("region-" + i, "agent-" + ii, "plugin/" + iii);
								sendout.sendMessage(String.valueOf(i + ii + iii));
								//logger.info("Sending message");
							}

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
