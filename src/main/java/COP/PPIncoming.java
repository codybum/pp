package COP;


import PP.PPEngine;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.client.*;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.io.IOException;

public class PPIncoming {

	private Launcher plugin;
	private CLogger logger;
	private COPEngine pp;
	private Channel channel;
	private Gson gson;

	public PPIncoming(Launcher plugin, COPEngine pp)
	{
		this.logger = new CLogger(PPIncoming.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		this.pp = pp;
		gson = new GsonBuilder().create();
	}
	 public void start() {
	        try 
	        {
	            channel = pp.ppFactory.newConnection().createChannel();
				channel.basicQos(1);
				channel.queueDeclare(pp.copId , false, false, false, null);
                logger.info(" [*] Waiting for messages in " + pp.copId);

                //Consumer consumer = new DefaultConsumer(channel);




				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
							throws IOException {
						String message = new String(body, "UTF-8");
                        logger.debug(pp.copId + " [x] Received '" + message + "'");
						//logger.info(pp.copId + " [x] Received");
						MsgEvent me = gson.fromJson(message, MsgEvent.class);
                        if((me.getMsgRegion() == null)) {
                            me.setMsgAgent(pp.copId);
                            //pp.sendout_cp.sendMessage(pp.cpId,me);
                            pp.cepQueue.offer(me);
                        }
                        else {
                            pp.sendout_pp.sendMessage(me.getMsgPlugin(),me);
                        }

                        //System.exit(0);
					}
				};

                channel.basicConsume(pp.copId, true, consumer);

	        }
		   catch(Exception ex)
		   {
	            logger.error(plugin.getStringFromError(ex));
	       }
	    }
	public void stop() {
		try {
			channel.close();
		}
		catch(Exception ex)
		{
			logger.error(plugin.getStringFromError(ex));
		}
	}
}
