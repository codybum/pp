package COP;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.HashMap;
import java.util.Map;

public class CPoutgoing {

	private Launcher plugin;
	private CLogger logger;
	private COPEngine pp;
	//private String queueName;
	//private Channel channel;
	private Map<String,Channel> channelMap;
	private Gson gson;


	public CPoutgoing(Launcher plugin, COPEngine pp)
	{
		this.logger = new CLogger(CPoutgoing.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		this.pp = pp;
		//queueName = "pp-" + pp.ppId;
		channelMap = new HashMap<>();
        gson = new GsonBuilder().create();

	}

	public void sendMessage(String queueName, MsgEvent me) {

		try {
			if(!channelMap.containsKey(queueName)) {
			    createChannel(queueName);
            }
            String message = gson.toJson(me);

            Channel channel = channelMap.get(queueName);
            if(!channel.isOpen()) {
            	logger.error("Channel Is not Open");
				createChannel(queueName);
			}
            channel.basicQos(1);
			channel.basicPublish("", queueName, null, message.getBytes("UTF-8"));
            logger.debug("Send to: " + queueName + " [x] Sent '" + message + "'");
			//logger.info("Send to: " + queueName);

			channel.addShutdownListener(new ShutdownListener() {
				public void shutdownCompleted(ShutdownSignalException cause)
				{
        			logger.error("CONNECTION SHUTDOWN!!! " + cause.getMessage() + " by applicaiton: " + cause.isInitiatedByApplication());
				}
			});
		}
		catch(Exception ex) {
			logger.error(plugin.getStringFromError(ex));
		}

	}

	private boolean createChannel(String queueName) {
		boolean isCreated = false;
		try {
		    Channel channel = pp.cpFactory.newConnection().createChannel();
		    channel.queueDeclare(queueName, false, false, false, null);
		    channelMap.put(queueName,channel);
		    isCreated = true;
		}
		catch(Exception ex) {
			logger.error(plugin.getStringFromError(ex));
		}
		return isCreated;
	}


}
