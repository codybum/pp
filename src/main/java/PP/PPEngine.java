package PP;


import com.rabbitmq.client.ConnectionFactory;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class PPEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
	public String ppId = null;
	public boolean incomingActive = false;
	public PPoutgoing sendout;
    public ConnectionFactory ppFactory;
    public String copId = null;

    private static final Random random = new Random();
    private static final String CHARS = "abcdefghijkmnopqrstuvwxyz1234567890";

    public PPEngine(Launcher plugin)
	{
		this.logger = new CLogger(PPEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		ppId = "pp-" + UUID.randomUUID().toString();

		copId = plugin.getConfig().getStringParam("cop_id","cop-0");
        //copQueue = "pp-" + ppId;
        ppFactory = new ConnectionFactory();
        String hostName = plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1");
        ppFactory.setHost(hostName);
        plugin.isReachable(hostName,"5672");
        logger.info("pp_amqp_host: " + plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1"));
        ppFactory.setUsername(plugin.getConfig().getStringParam("pp_amqp_username","admin"));
        ppFactory.setPassword(plugin.getConfig().getStringParam("pp_amqp_password","cody01"));
        //ppFactory.setConnectionTimeout(10000);

	}
	 public void run() {
	        try 
	        {
				PPIncoming incoming = new PPIncoming(plugin,this);
				incoming.start();

                sendout = new PPoutgoing(plugin,this);

                while(plugin.isActive) {

                    //logger.info("hour: " + hour);
                    MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, copId, ppId, "");
                    me.setParam("sensor_data",genSensorData());
                    me.setParam("car_data",genCarData(plugin.hour));
                    sendout.sendMessage(copId,me);
                    Thread.sleep(5000);
                }
                incoming.stop();

	        }
		   catch(Exception ex)
		   {
	            logger.error(plugin.getStringFromError(ex));
	       }
	    }

	    private String genSensorData() {
        String sensorData = null;
        try {
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < 10; i++) {

                int sensorValue = ThreadLocalRandom.current().nextInt(0, 10 + 1);

                sb.append("s" + i + ":" + sensorValue + ",");

            }

            sensorData = String.valueOf(sb.toString().substring(0,sb.length() -1));

            }
            catch(Exception ex) {
            logger.error("genSensorData " + ex.getMessage());
            }
            return sensorData;
        }

    private String genCarData(int hour) {
        String carData = null;
        try {
            StringBuilder sb = new StringBuilder();
            int h_bound;
            int l_bound;
            int maxSpeed;
            int minSpeed;

            switch (hour) {
                case 6:
                    h_bound = 30;
                    l_bound = 10;
                    maxSpeed = 30;
                    minSpeed = 20;
                    break;
                case 7:
                    h_bound = 50;
                    l_bound = 30;
                    maxSpeed = 20;
                    minSpeed = 10;
                    break;
                case 8:
                    h_bound = 70;
                    l_bound = 50;
                    maxSpeed = 10;
                    minSpeed = 0;
                    break;
                case 9:
                    h_bound = 50;
                    l_bound = 30;
                    maxSpeed = 20;
                    minSpeed = 10;
                    break;
                case 10:
                    h_bound = 30;
                    l_bound = 10;
                    maxSpeed = 30;
                    minSpeed = 20;
                    break;
                case 16:
                    h_bound = 30;
                    l_bound = 10;
                    maxSpeed = 30;
                    minSpeed = 20;
                    break;
                case 17:
                    h_bound = 50;
                    l_bound = 30;
                    maxSpeed = 20;
                    minSpeed = 10;
                    break;
                case 18:
                    h_bound = 70;
                    l_bound = 50;
                    maxSpeed = 10;
                    minSpeed = 0;
                    break;
                case 19:
                    h_bound = 50;
                    l_bound = 30;
                    maxSpeed = 20;
                    minSpeed = 10;
                    break;
                case 20:
                    h_bound = 30;
                    l_bound = 10;
                    maxSpeed = 30;
                    minSpeed = 20;
                    break;
                default:
                    h_bound = 10;
                    l_bound = 1;
                    maxSpeed = 50;
                    minSpeed = 40;
                    break;
            }

            int carCount = ThreadLocalRandom.current().nextInt(l_bound, h_bound + 1);

            for(int i = 0; i < carCount; i++) {

                String carSensorValue = String.valueOf(ThreadLocalRandom.current().nextInt(minSpeed, maxSpeed + 1));
                sb.append("c" + getToken(5) + ":" + carSensorValue + ",");
            }

            carData = String.valueOf(sb.toString().substring(0,sb.length() -1));

        }
        catch(Exception ex) {
            logger.error("genSensorData " + ex.getMessage());
        }
        return carData;
    }

    public static String getToken(int length) {
        StringBuilder token = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            token.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return token.toString();
    }


}
