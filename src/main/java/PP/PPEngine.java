package PP;


import com.rabbitmq.client.ConnectionFactory;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class PPEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
	public String ppId = null;
	public boolean incomingActive = false;
	public PPoutgoing sendout;
    public ConnectionFactory ppFactory;
    public String copId = null;

    public PPEngine(Launcher plugin)
	{
		this.logger = new CLogger(PPEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		ppId = "pp-" + UUID.randomUUID().toString();

		copId = plugin.getConfig().getStringParam("cop_id","cop-0");
        //copQueue = "pp-" + ppId;
        ppFactory = new ConnectionFactory();
        ppFactory.setHost(plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1"));
        ppFactory.setUsername(plugin.getConfig().getStringParam("pp_amqp_username","admin"));
        ppFactory.setPassword(plugin.getConfig().getStringParam("pp_amqp_password","cody01"));
        ppFactory.setConnectionTimeout(10000);

	}
	 public void run() {
	        try 
	        {
				PPIncoming incoming = new PPIncoming(plugin,this);
				incoming.start();

                sendout = new PPoutgoing(plugin,this);

                int hour = 0;

                while(plugin.isActive) {

                	StringBuilder sb = new StringBuilder();
                    for(int i = 0; i < 1000; i++) {

                        int sensorValue;
                        if((hour == 6) || (hour == 9)) {
                        	sensorValue = ThreadLocalRandom.current().nextInt(0, 25 + 1);
						}
						else if ((hour == 7) || (hour == 8)) {
							sensorValue = ThreadLocalRandom.current().nextInt(0, 65 + 1);
						}
						else {
							sensorValue = ThreadLocalRandom.current().nextInt(0, 10 + 1);
						}
                        sb.append("s" + i + ":" + sensorValue + ",");

                    }

                    if(hour == 24) {
                        hour = 0;
                    }
                    hour++;
                    logger.info("hour: " + hour);
                    MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, copId, ppId, "");
                    me.setParam("sensor_data",String.valueOf(sb.toString().substring(0,sb.length() -1)));

                    sendout.sendMessage(copId,me);
                    Thread.sleep(5000);


                }

                incoming.stop();


	        }
		   catch(Exception ex)
		   {
	            logger.error(plugin.getStringFromError(ex));
	       }
	    }  
}
