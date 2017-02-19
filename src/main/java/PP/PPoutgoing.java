package PP;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.client.*;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PPoutgoing {

	private Launcher plugin;
	private CLogger logger;
	private PPEngine pp;
	//private String queueName;
	//private Channel channel;
	private Map<String,Channel> channelMap;
	private Gson gson;


	public PPoutgoing(Launcher plugin, PPEngine pp)
	{
		this.logger = new CLogger(PPoutgoing.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
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
            channel.basicPublish("", queueName, null, message.getBytes("UTF-8"));
            logger.debug("Send to: " + queueName + " [x] Sent '" + message + "'");
			logger.info("Send to: " + queueName );


		}
		catch(Exception ex) {
			logger.error(plugin.getStringFromError(ex));
		}

	}

	private boolean createChannel(String queueName) {
		boolean isCreated = false;
		try {
		    Channel channel = pp.ppFactory.newConnection().createChannel();
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
