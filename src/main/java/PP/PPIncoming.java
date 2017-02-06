package PP;


import com.rabbitmq.client.*;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.io.IOException;

public class PPIncoming implements Runnable {

	private Launcher plugin;
	private CLogger logger;
	private PPEngine pp;
	private String queueName;

	public PPIncoming(Launcher plugin, PPEngine pp)
	{
		this.logger = new CLogger(PPIncoming.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		this.pp = pp;
		queueName = "pp-" + pp.ppId;
	}
	 public void run() {
	        try 
	        {
	            pp.incomingActive = true;
				Channel channel = plugin.factory.newConnection().createChannel();

				channel.queueDeclare(queueName , false, false, false, null);
				logger.info(" [*] Waiting for messages. To exit press CTRL+C");

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

                channel.basicConsume(queueName, true, consumer);

                while(pp.incomingActive) {
                Thread.sleep(10000);
/*
                    //channel.basicConsume(queueName, true, consumer);

                            QueueingConsumer.Delivery delivery = consumer.nextDelivery(500);
                            if(!(delivery == null))
                            {
                                //String message = new String(delivery.getBody());
                                //Launcher.nodeMap.get(outputNode).input(message);
                            }
  */
                }
	        }
		   catch(Exception ex)
		   {
	            logger.error(plugin.getStringFromError(ex));
	       }
	    }  
}
