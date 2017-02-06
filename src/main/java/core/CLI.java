package core;


import com.researchworx.cresco.library.messaging.MsgEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class CLI {


    private static final Logger logMessages = LoggerFactory.getLogger("Logging");

    public static void main(String[] args) throws Exception
	{
        try {

            String configFile = "/Users/cody/git/crescodbtest/config.ini";
            Config config = config = new Config(configFile);
            Launcher la = new Launcher();

            ConcurrentLinkedQueue<MsgEvent> msgQueue = new ConcurrentLinkedQueue<>();

            boolean isStarted = la.initialize(msgQueue, config.getConfig(), "region-test", "agent-test" , "plugin/0");

            while(true) {
                MsgEvent ce = msgQueue.poll();
                if(ce != null) {
                    try {
                        //System.out.println("[" + ce.getParams().toString());
                        logMessage(ce);
                    }
                    catch(Exception ex) {
                        ex.printStackTrace();
                    }
                    /*
                    if (ce.getMsgType() == MsgEvent.Type.LOG) {
                        logMessage(ce);
                    }
                    */
                }
                else {
                    Thread.sleep(1000);
                }
            }

        }
        catch(Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }



    }

    private static void logMessage(MsgEvent log) {
        //String className = log.getParam("full_class");
        String logMessage = "[" + log.getParam("src_plugin") + "]";
        logMessage = logMessage + " " + log.getMsgBody();

        if(log.getParam("log_level") == null) {
            System.out.println(logMessage);
            return;
        }



        switch (log.getParam("log_level").toLowerCase()) {
            case "error":
                logMessages.error(logMessage);
                break;
            case "warn":
                logMessages.warn(logMessage);
                break;
            case "info":
                logMessages.info(logMessage);
                break;
            case "debug":
                logMessages.debug(logMessage);
                break;
            case "trace":
                logMessages.trace(logMessage);
                break;
            default:
                logMessages.error("Unknown log_level [{}]", log.getParam("log_level"));
                break;
        }
    }

    private static String formatClassName(String className) {
        String newName = "";
        int lastIndex = 0;
        int nextIndex = className.indexOf(".", lastIndex + 1);
        while (nextIndex != -1) {
            newName = newName + className.substring(lastIndex, lastIndex + 1) + ".";
            lastIndex = nextIndex + 1;
            nextIndex = className.indexOf(".", lastIndex + 1);
        }
        return newName + className.substring(lastIndex);
    }


}
