package PP;


import com.rabbitmq.client.*;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.io.IOException;

public class PPoutgoing {

	private Launcher plugin;
	private CLogger logger;
	private PPEngine pp;
	private String queueName;
	private Channel channel;

	public PPoutgoing(Launcher plugin, PPEngine pp)
	{
		this.logger = new CLogger(PPoutgoing.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		this.pp = pp;
		queueName = "pp-" + pp.ppId;
		try {
		channel = plugin.factory.newConnection().createChannel();
		channel.queueDeclare(queueName, false, false, false, null);

		}
		catch(Exception ex) {
			logger.error(plugin.getStringFromError(ex));
		}
	}

	public void sendMessage(String message) {

		try {
			//String message = "Hello World!";
			channel.basicPublish("", queueName, null, message.getBytes("UTF-8"));
			logger.debug(" [x] Sent '" + message + "'");

		}
		catch(Exception ex) {
			logger.error(plugin.getStringFromError(ex));
		}

	}


}
