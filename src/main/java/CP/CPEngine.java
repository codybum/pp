package CP;


import com.rabbitmq.client.ConnectionFactory;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class CPEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
	public boolean incomingActive = false;
	public CPoutgoing sendout;
    public ConnectionFactory ppFactory;
    public String cpId = null;

    public CPEngine(Launcher plugin)
	{
		this.logger = new CLogger(CPEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;

        //cpId = "cp-" + "0";
		cpId = plugin.getConfig().getStringParam("cp_id","cp-0");


		//copQueue = "pp-" + ppId;
        ppFactory = new ConnectionFactory();
        ppFactory.setHost(plugin.getConfig().getStringParam("cp_amqp_host","127.0.0.1"));
        ppFactory.setUsername(plugin.getConfig().getStringParam("cp_amqp_username","admin"));
        ppFactory.setPassword(plugin.getConfig().getStringParam("cp_amqp_password","cody01"));
        ppFactory.setConnectionTimeout(10000);

	}
	 public void run() {
	        try
	        {
				CPIncoming incoming = new CPIncoming(plugin,this);
				incoming.start();

                sendout = new CPoutgoing(plugin,this);

                while(plugin.isActive) {

                	/*
                    StringBuilder sb = new StringBuilder();
                    for(int i = 0; i < 100; i++) {
                        int sensorValue = ThreadLocalRandom.current().nextInt(0, 10);
                        sb.append("s" + i + ":" + sensorValue + ",");
                    }
                    MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, copId, ppId, "");
                    me.setParam("sensor_data",String.valueOf(sb.toString().substring(0,sb.length() -1)));

                    sendout.sendMessage(copId,me);
                    */
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
