package COP;


import com.rabbitmq.client.ConnectionFactory;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.concurrent.ConcurrentLinkedQueue;

public class COPEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;

	public ConcurrentLinkedQueue<MsgEvent> cepQueue;

	public PPoutgoing sendout_pp;
	public CPoutgoing sendout_cp;

	public ConnectionFactory ppFactory;
	public ConnectionFactory cpFactory;

	public String copId = null;
	public String cpId = null;

    public COPEngine(Launcher plugin)
	{
		this.logger = new CLogger(COPEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;

		this.cepQueue = new ConcurrentLinkedQueue();

		//copId = UUID.randomUUID().toString();
		//copId = "cop-" + "0";
        copId = plugin.getConfig().getStringParam("cop_id","cop-0");
		cpId = plugin.getConfig().getStringParam("cp_id","cp-0");
        logger.info("cop_id: " + copId);
        logger.info("cp_id: " + cpId);

        //cpId = "cp-" + "0";

		//copQueue = "pp-" + ppId;
		ppFactory = new ConnectionFactory();
		//ppFactory.setHost(plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1"));
        String pphostName = plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1");
        ppFactory.setHost(pphostName);
        plugin.isReachable(pphostName,"5672");

        ppFactory.setUsername(plugin.getConfig().getStringParam("pp_amqp_username","admin"));
		ppFactory.setPassword(plugin.getConfig().getStringParam("pp_amqp_password","cody01"));
		ppFactory.setConnectionTimeout(10000);

		cpFactory = new ConnectionFactory();
		//cpFactory.setHost(plugin.getConfig().getStringParam("cp_amqp_host","127.0.0.1"));
        String cphostName = plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1");
        cpFactory.setHost(cphostName);
        plugin.isReachable(cphostName,"5672");

        cpFactory.setUsername(plugin.getConfig().getStringParam("cp_amqp_username","admin"));
		cpFactory.setPassword(plugin.getConfig().getStringParam("cp_amqp_password","cody01"));
		cpFactory.setConnectionTimeout(10000);



	}
	 public void run() {
	        try
	        {
				COPESPEREngine ee = new COPESPEREngine(plugin,this);
				Thread et = new Thread(ee);
				et.start();
				logger.info("COPESPER Engine Started.");

				PPIncoming incoming = new PPIncoming(plugin,this);
				incoming.start();
                logger.info("PP Incoming Started.");


				CPIncoming cIncoming = new CPIncoming(plugin,this);
				cIncoming.start();
                logger.info("CP Incoming Started.");


                sendout_pp = new PPoutgoing(plugin,this);
                logger.info("PP OutGoing Started.");

                sendout_cp = new CPoutgoing(plugin,this);
                logger.info("CP OutGoing Started.");


                while(plugin.isActive) {

                    /*
                    StringBuilder sb = new StringBuilder();
                    for(int i = 0; i < 100; i++) {
                        int sensorValue = ThreadLocalRandom.current().nextInt(0, 10);
                        sb.append("s" + i + ":" + sensorValue + ",");
                    }

                    sendout.sendMessage(copQueue,String.valueOf(sb.toString().substring(0,sb.length() -1)));
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
