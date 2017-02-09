package COP;


import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

public class ESPEREngine implements Runnable {

    private Launcher plugin;
    private CLogger logger;
    private COPEngine pp;


    //ESPER
    private static EPRuntime cepRT;
    private static EPAdministrator cepAdm;
    private static ConcurrentHashMap<String,CEPListener> listners;
    private static ConcurrentHashMap<String,EPStatement> statements;
    //private String esper_querystring;

    private static Gson gson;

    public ESPEREngine(Launcher plugin, COPEngine pp)
    {
        this.logger = new CLogger(CPoutgoing.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        this.plugin = plugin;
        this.pp = pp;

        listners = new ConcurrentHashMap<String,CEPListener>();
        statements = new ConcurrentHashMap<String,EPStatement>();
        gson = new GsonBuilder().create();

        //esper_querystring = "select params('sensor_data') from MsgEvent";
        //esper_querystring = "select * from sensorMap where sensorValue > 5";

    }

    public void run()
    {
        try
        {


            //START ESPER

            //The Configuration is meant only as an initialization-time object.
            Configuration cepConfig = new Configuration();
            //cepConfig.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
            cepConfig.addEventType("MsgEvent", MsgEvent.class.getName());

            Map<String, Object> def = new HashMap<>();
            def.put("ppId", String.class);
            def.put("sensorId", String.class);
            def.put("sensorValue", int.class);

            cepConfig.addEventType("sensorMap", def);

            //epService.getEPAdministrator().getConfiguration().
            //        addEventType("CarLocUpdateEvent", def);

            //cepConfig.addEventType("sensorMap", java.util.Map.class.getName());

            EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
            cepRT = cep.getEPRuntime();
            cepAdm = cep.getEPAdministrator();

            /*
            if(!addQuery("0", esper_querystring))
            {
                String errorString = "QueryNode: Unable to add query: " + esper_querystring;
                System.out.println(errorString);

            }
            */
            //END ESPER

            //esper_querystring = "select params('sensor_data') from MsgEvent";
            //esper_querystring = "select * from sensorMap where sensorValue > 5";

            addQuery("0", "select params('sensor_data') from MsgEvent");
            addQuery("1", "select * from sensorMap where sensorValue > 99");

            addQuery("2", "select avg(sensorValue) as avgValue, count(sensorValue) as countValue, min(sensorValue) as minValue, max(sensorValue) as maxValue from sensorMap.win:time_batch(5 sec)");



            while (plugin.isActive)
            {
                try
                {
                    MsgEvent me = pp.cepQueue.poll();
                            if(me != null) {
                                //input(message);
                                cepRT.sendEvent(me);
                                String sensor_data = me.getParam("sensor_data");
                                String[] sensorArray = sensor_data.split(",");
                                for(String sensorEntry : sensorArray) {
                                    String[] sensorEntrySplit = sensorEntry.split(":");
                                    Map<String,Object> sensorMap = new HashMap<>();
                                    sensorMap.put("pp",me.getMsgPlugin());
                                    sensorMap.put("sensorId",sensorEntrySplit[0]);
                                    sensorMap.put("sensorValue", Integer.parseInt(sensorEntrySplit[1]));
                                    cepRT.sendEvent(sensorMap,"sensorMap");
                                }
                            }
                            else {
                                Thread.sleep(1000);
                            }

                }
                catch(Exception ex)
                {
                    String errorString = "QueryNode: Error: " + ex.toString();
                    System.out.println(errorString);
                }
            }

        }
        catch(Exception ex)
        {
            System.out.println("ESPEREngine Error: " + ex.toString());
        }

    }

    public class CEPListener implements UpdateListener {
        public String query_id;
        public String outExchange;

        public CEPListener(String query_id)
        {
            this.query_id = query_id;
            this.outExchange = outExchange;
        }
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null)
            {

                String str = newEvents[0].getUnderlying().toString();
                if(str != null)
                {
                    try
                    {
                        //tx_channel.basicPublish(outExchange, "", null, str.getBytes());
                        logger.info("id: " + query_id + " output: " +  str);
                        //System.out.println(str);
                    }
                    catch(Exception ex)
                    {
                        System.out.println("ESPEREngine : Error : " + ex.toString());
                    }
                }


            }
            if (oldEvents != null)
            {
                System.out.println("Old Event received: " + oldEvents[0].getUnderlying());
                //count++;
            }
        }
    }

    public boolean addQuery(String query_id, String query)
    {
        try
        {
            EPStatement cepStatement = cepAdm.createEPL(query);
            CEPListener c = new CEPListener(query_id);
            cepStatement.addListener(c);
            listners.put(query_id, c);
            statements.put(query_id, cepStatement);
            return true;
        }
        catch(Exception ex)
        {
            System.out.println("ESPEREngine addQuery: " + ex.toString());
            return false;
        }
    }

    public boolean delQuery(String query_id)
    {
        try
        {
            EPStatement cepStatement = statements.get(query_id);
            CEPListener c = listners.get(query_id);
            cepStatement.removeListener(c);
            c = null;
            cepStatement.stop();
            cepStatement.destroy();
            cepStatement = null;
            listners.remove(query_id);
            statements.remove(query_id);
            return true;
        }
        catch(Exception ex)
        {
            System.out.println("ESPEREngine delQuery: " + ex.toString());
            return false;
        }

    }

    public void input(String inputStr) throws ParseException
    {
        try
        {
            //netFlow flow = nFlowFromJson(inputStr);
            //cepRT.sendEvent(flow);
        }
        catch(Exception ex)
        {
            System.out.println("ESPEREngine : Input netFlow Error : " + ex.toString());
            System.out.println("ESPEREngine : Input netFlow Error : InputStr " + inputStr);
        }

    }

    /*
    private netFlow nFlowFromJson(String json)
    {
        netFlow flow = gson.fromJson(json, netFlow.class);
        return flow;
    }
    */

}
