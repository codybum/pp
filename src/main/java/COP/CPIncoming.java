package COP;


import com.rabbitmq.client.*;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.io.IOException;

public class CPIncoming {

	private Launcher plugin;
	private CLogger logger;
	private COPEngine pp;
	private Channel channel;

	public CPIncoming(Launcher plugin, COPEngine pp)
	{
		this.logger = new CLogger(CPIncoming.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		this.pp = pp;
		//start();
	}
	 public void start() {
	        try 
	        {
	            channel = pp.cpFactory.newConnection().createChannel();

				channel.queueDeclare(pp.copId , false, false, false, null);
				logger.info(" [*] Waiting for messages in " + pp.copId);

                //Consumer consumer = new DefaultConsumer(channel);




				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
							throws IOException {
						String message = new String(body, "UTF-8");
                        logger.info(" [x] Received '" + message + "'");

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
