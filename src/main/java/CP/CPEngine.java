package CP;


import app.AppTools;
import channels.ControllerChannel;
import com.rabbitmq.client.ConnectionFactory;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CPEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
	public boolean incomingActive = false;
	public CPoutgoing sendout;
    public ConnectionFactory ppFactory;
    public String cpId = null;

    public ControllerChannel cc;
    public AppTools at;

    public ConcurrentLinkedQueue<MsgEvent> cepQueue;

    public String queuePipeline = null;
    public String copPipeline = null;
    public String ppPipeline = null;


    public int state = 0;

	public CPEngine(Launcher plugin)
	{
        cc = new ControllerChannel(plugin); //methods to communicate with global controller
        at = new AppTools(plugin,this);

        this.logger = new CLogger(CPEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;

		this.cepQueue = new ConcurrentLinkedQueue();

		//cpId = "cp-" + "0";
		cpId = plugin.getConfig().getStringParam("cp_id","cp-0");


		//copQueue = "pp-" + ppId;
        ppFactory = new ConnectionFactory();
        //ppFactory.setHost(plugin.getConfig().getStringParam("cp_amqp_host","127.0.0.1"));
		String hostName = plugin.getConfig().getStringParam("cp_amqp_host","127.0.0.1");
		ppFactory.setHost(hostName);
		plugin.isReachable(hostName,"5672");

		ppFactory.setUsername(plugin.getConfig().getStringParam("cp_amqp_username","admin"));
        ppFactory.setPassword(plugin.getConfig().getStringParam("cp_amqp_password","cody01"));
        //ppFactory.setConnectionTimeout(10000);

	}

	private void launchQueue() {
        try {
            Map<String,String> ri = at.getControllerResourceInventory();
            logger.info("cpu core count: " + ri.get("cpu_core_count"));
            if(ri.get("cpu_core_count").equals("160")) {
                queuePipeline = at.addQueues();
                int count = 0;
                String status_code = at.getGpipelineStatus(queuePipeline);
                while((count < 300) && (!status_code.equals("10"))) {
                    logger.info("Waiting on queuePipeline " + queuePipeline + " status_code: " + status_code);
                    Thread.sleep(5000);
                    status_code = at.getGpipelineStatus(queuePipeline);
                }
                if(status_code.equals("10")) {
                    state = 1;
                } else {
                    state = 5;
                }
            }
        }
        catch(Exception ex) {
            logger.error("launchQueue: " + ex.getMessage());
        }
    }

    private void launchCOP() {
        try {
            copPipeline = at.addCOP();
            int count = 0;
            String status_code = at.getGpipelineStatus(copPipeline);
            while((count < 300) && (!status_code.equals("10"))) {
                logger.info("Waiting on COPPipeline " + copPipeline + " status_code: " + status_code);
                Thread.sleep(5000);
                status_code = at.getGpipelineStatus(copPipeline);
            }
            if(status_code.equals("10")) {
                state = 2;
            } else {
                state = 5;
            }

        }
        catch(Exception ex) {
            logger.error("launchCOP: " + ex.getMessage());
        }
    }

    private void launchPP() {
        try {
            ppPipeline = at.addPP();
            int count = 0;
            String status_code = at.getGpipelineStatus(ppPipeline);
            while((count < 300) && (!status_code.equals("10"))) {
                logger.info("Waiting on PPPipeline " + ppPipeline + " status_code: " + status_code);
                Thread.sleep(5000);
                status_code = at.getGpipelineStatus(ppPipeline);
            }
            if(status_code.equals("10")) {
                state = 10;
            } else {
                state = 5;
            }

        }
        catch(Exception ex) {
            logger.error("launchPP: " + ex.getMessage());
        }
    }

    private void noOP() {
        try {
            logger.info("Current State: " + state);
            logger.info("queuePipeline " + queuePipeline + " status_code: " + at.getGpipelineStatus(queuePipeline));
            logger.info("copPipeline " + copPipeline + " status_code: " + at.getGpipelineStatus(copPipeline));
            logger.info("ppPipeline " + ppPipeline + " status_code: " + at.getGpipelineStatus(ppPipeline));
        }
        catch(Exception ex) {
            logger.error("launchQueue: " + ex.getMessage());
        }
    }

    private void failureState() {
        try {

        }
        catch(Exception ex) {
            logger.error("launchQueue: " + ex.getMessage());
        }
    }

	 public void run() {
	        try
	        {
				CPESPEREngine ee = new CPESPEREngine(plugin,this);
				Thread et = new Thread(ee);
				et.start();
				logger.info("ESPER Engine Started.");

				CPIncoming incoming = new CPIncoming(plugin,this);
				incoming.start();

                sendout = new CPoutgoing(plugin,this);


                queuePipeline = "d63a1243-e02d-4b95-90a5-5504ec3a2c4c";
                copPipeline = "a431695e-10f6-4008-88ff-67371cf1439a";
                ppPipeline = "7baa9e50-3630-4ff3-b1c9-197c5fed9659";
                state = 10;


                while(plugin.isActive) {

                    switch (state) {
                        case 0:
                            launchQueue();
                            break;
                        case 1:
                            launchCOP();
                            break;
                        case 2:
                            launchPP();
                            break;
                        case 5:
                            failureState();
                            break;
                        case 10:
                            noOP();
                            break;
                        default:
                            noOP();
                    }

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
