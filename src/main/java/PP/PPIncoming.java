package PP;


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
	private PPEngine pp;
	private Channel channel;
	private Gson gson;

	public PPIncoming(Launcher plugin, PPEngine pp)
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
				channel.queueDeclare(pp.ppId , false, false, false, null);
                logger.info(" [*] Waiting for messages in " + pp.ppId);

                //Consumer consumer = new DefaultConsumer(channel);




				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
							throws IOException {
						String message = new String(body, "UTF-8");
                        logger.info(pp.ppId + " [x] Received '" + message + "'");
                        MsgEvent me = gson.fromJson(message, MsgEvent.class);
                        //gPayload me = gson.fromJson(json, gPayload.class);

                        //System.exit(0);
					}
				};

                channel.basicConsume(pp.ppId, true, consumer);

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
