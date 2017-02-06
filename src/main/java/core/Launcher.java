package core;

import com.google.auto.service.AutoService;
import com.rabbitmq.client.ConnectionFactory;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.messaging.RPC;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.commons.configuration.SubnodeConfiguration;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import PP.*;
import COP.*;
import CP.*;

@AutoService(CPlugin.class)
public class Launcher extends CPlugin {

    public boolean isActive = true;

    private Thread ppThread = null;
    public void start() {

        try {

            this.logger = new CLogger(Launcher.class, getMsgOutQueue(), getRegion(), getAgent(), getPluginID(), CLogger.Level.Trace);

            this.config = new ControllerConfig(config.getConfig());
            commInit();

            ppThread.start();

        }
        catch(Exception ex) {
            System.out.println("start() " + getStringFromError(ex));
        }
    }

    public void commInit() {
        try {


            int pathStage = config.getIntegerParam("path_stage",1);
            logger.debug("[pathStage] == {}", pathStage);
            switch (pathStage) {
                case 1:
                    logger.debug("Starting PP Thread");
                    PPEngine pp = new PPEngine(this);
                    ppThread = new Thread(pp);

                    CPEngine cp_test = new CPEngine(this);
                    new Thread(cp_test).start();


                    COPEngine cop_test = new COPEngine(this);
                    new Thread(cop_test).start();

                    break;
                case 2:
                    logger.debug("Starting COP Thread");
                    COPEngine cop = new COPEngine(this);
                    ppThread = new Thread(cop);
                    break;
                case 3:
                    logger.debug("Starting CP Thread");
                    CPEngine cp = new CPEngine(this);
                    ppThread = new Thread(cp);
                    break;
                default:
                    logger.trace("Encountered default switch path");
                    break;
            }
        }
        catch(Exception ex) {
            logger.error(getStringFromError(ex));
        }
    }

    @Override
    public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue, SubnodeConfiguration config, String region, String agent, String pluginID) {
        this.setMsgOutQueue(msgOutQueue);
        this.setExecutor();
        this.setConfig(new com.researchworx.cresco.library.core.Config(config));
        this.setRegion(region);
        this.setAgent(agent);
        this.setPluginID(pluginID);
        this.setLogger(new CLogger(this.msgOutQueue, this.region, this.agent, this.pluginID));
        this.setRPC(new RPC(this.msgOutQueue, this.region, this.agent, this.pluginID, this.logger));
        this.setWatchDog(new WatchDog(this.region, this.agent, this.pluginID, this.logger, this.config));

        try {
            this.start();
        } catch (Exception var7) {
            if(this.logger != null) {
                this.logger.error("Initialization failed. [Exception: {}]", new Object[]{var7.getMessage()});
            }

            return false;
        }

        this.startWatchDog();
        this.setActive(Boolean.valueOf(true));
        return true;
    }

    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }


}
