package CP;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.client.*;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.io.IOException;

public class CPIncoming {

	private Launcher plugin;
	private CLogger logger;
	private CPEngine pp;
	private Channel channel;
	private Gson gson;

	public CPIncoming(Launcher plugin, CPEngine pp)
	{
		this.logger = new CLogger(CPIncoming.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
		this.plugin = plugin;
		this.pp = pp;
		gson = new GsonBuilder().create();
	}
	 public void start() {
	        try 
	        {
	            channel = pp.ppFactory.newConnection().createChannel();
                channel.basicQos(1);
                //channel.queueDeclare(pp.cpId , false, false, false, null);
                channel.queueDeclare(pp.cpId, true, false, true, null);

                logger.info(" [*] Waiting for messages in " + pp.cpId);

                //Consumer consumer = new DefaultConsumer(channel);




				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
							throws IOException {
						String message = new String(body, "UTF-8");
                        MsgEvent me = gson.fromJson(message, MsgEvent.class);
						logger.debug(pp.cpId + " [x] Received '" + message + "'");
						logger.info(pp.cpId + " [x] Received CP: " + me.getMsgRegion() + " COP: " + me.getMsgAgent() + " PP: " + me.getMsgPlugin() );
                        //sensor_alert: sensor_alert output: pp-44f1f9cd-504b-40f6-82f1-d48c5af08de8:s9:100
                        //car_data: car_data output: pp-5e25ced0-71ac-41c1-835b-77b88e4af587:44.6578947368421

						//pp.cepQueue.offer(me);

						//me.setMsgRegion(pp.cpId);
						//pp.sendout.sendMessage(me.getMsgAgent(),me);

						//gPayload me = gson.fromJson(json, gPayload.class);

                        //System.exit(0);
					}
				};

                channel.basicConsume(pp.cpId, true, consumer);

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
