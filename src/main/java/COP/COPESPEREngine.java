package COP;


import java.awt.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

public class COPESPEREngine implements Runnable {

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

    public COPESPEREngine(Launcher plugin, COPEngine pp)
    {
        this.logger = new CLogger(CPoutgoing.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
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
            //cepConfig.addEventType("MsgEvent", MsgEvent.class.getName());

            Map<String, Object> sdef = new HashMap<>();
            sdef.put("ppId", String.class);
            sdef.put("sensorId", String.class);
            sdef.put("sensorValue", int.class);

            cepConfig.addEventType("sensorMap", sdef);


            Map<String, Object> cdef = new HashMap<>();
            cdef.put("ppId", String.class);
            cdef.put("carId", String.class);
            cdef.put("carValue", int.class);

            cepConfig.addEventType("carMap", cdef);

            //epService.getEPAdministrator().getConfiguration().
            //        addEventType("CarLocUpdateEvent", def);

            //cepConfig.addEventType("sensorMap", java.util.Map.class.getName());

            //EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
            EPServiceProvider cep = EPServiceProviderManager.getDefaultProvider(cepConfig);


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

            //addQuery("0", "select ppId from sensorMap");

            addQuery("0", "select * from sensorMap");

            //addQuery("sensor_data", "select irstream ppId, sensorId, avg(sensorValue) as avgValue from sensorMap.win:time(15 sec) group by sensorId output snapshot every 5 seconds");

            //addQuery("car_data", "select irstream ppId, count(carValue) as avgValue from carMap.win:time(15 sec) group by ppId output snapshot every 5 seconds");

            //esper_querystring = "select params('sensor_data') from MsgEvent";
            //esper_querystring = "select * from sensorMap where sensorValue > 5";

            //addQuery("0", "select ppId, avg(sensorValue) as avgValue from sensorMap.win:time_batch(10 sec) group by ppId");
            //addQuery("0", "select irstream  ppId, avg(sensorValue) as avgValue, count(sensorValue) as countValue, min(sensorValue) as minValue, max(sensorValue) as maxValue from sensorMap.win:time(1 sec) group by rollup(ppId) output snapshot every 1 seconds");


            //select account, sum(amount)
            //    from Withdrawal.win:time_batch(1 sec)
            //group by account
            //addQuery("0", "select params('sensor_data') from MsgEvent");
            //addQuery("1", "select params('car_data') from MsgEvent");

            //addQuery("1", "select * from sensorMap where sensorValue > 99");

//            addQuery("3", "select ppId, avg(sensorValue) as avgValue, count(sensorValue) as countValue, min(sensorValue) as minValue, max(sensorValue) as maxValue from sensorMap.win:time_batch(5 sec) group by ppId");
            //addQuery("3", "select avg(sensorValue) as avgValue, count(sensorValue) as countValue, min(sensorValue) as minValue, max(sensorValue) as maxValue from sensorMap.win:time_batch(5 sec) group by ppId");
            //addQuery("3", "select ppId from sensorMap.win:time_batch(5 sec) group by ppId");
            //addQuery("4", "select avg(carValue) as avgValue, count(carValue) as countValue, min(carValue) as minValue, max(carValue) as maxValue from carMap.win:time_batch(5 sec)");
//group by feed output snapshot every 1 sec


            while (plugin.isActive)
            {
                try
                {
                    MsgEvent me = pp.cepQueue.poll();
                            if(me != null) {

                                if(me.getParam("sensor_data") != null) {
                                    String sensor_data = me.getParam("sensor_data");
                                    String[] sensorArray = sensor_data.split(",");
                                    for(String sensorEntry : sensorArray) {
                                        String[] sensorEntrySplit = sensorEntry.split(":");
                                        Map<String,Object> sensorMap = new HashMap<>();
                                        sensorMap.put("ppId",me.getMsgPlugin());
                                        sensorMap.put("sensorId",sensorEntrySplit[0]);
                                        sensorMap.put("sensorValue", Integer.parseInt(sensorEntrySplit[1]));
                                        cepRT.sendEvent(sensorMap,"sensorMap");
                                    }
                                }
                                else if(me.getParam("car_data") != null) {

                                    String car_data = me.getParam("car_data");
                                    String[] carArray = car_data.split(",");
                                    for(String carEntry : carArray) {
                                        String[] carEntrySplit = carEntry.split(":");
                                        Map<String,Object> carMap = new HashMap<>();
                                        carMap.put("ppId",me.getMsgPlugin());
                                        carMap.put("carId",carEntrySplit[0]);
                                        carMap.put("carValue", Integer.parseInt(carEntrySplit[1]));
                                        cepRT.sendEvent(carMap,"carMap");
                                    }
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
            System.out.println("COPESPEREngine Error: " + ex.toString());
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
            if (newEvents != null) {

                if (query_id.equals("sensor_data")) {

                    StringBuilder sb = new StringBuilder();
                    Map<String, List<EventBean>> eventMap = new HashMap<>();

                    for (EventBean eb : newEvents) {
                        try {
                            String ppId = eb.get("ppId").toString();
                            if(!eventMap.containsKey(ppId)) {
                                eventMap.put(ppId,new ArrayList<EventBean>());
                                //logger.error("Create new ppid " + ppId + " thread: " + Thread.currentThread().getId());
                            }
                            eventMap.get(ppId).add(eb);

                        } catch (Exception ex) {
                            System.out.println("COPESPEREngine : Error : " + ex.toString());
                        }

                    }


                    for (Map.Entry<String, List<EventBean>> entry : eventMap.entrySet()) {
                        String ppId= entry.getKey();
                        List<EventBean> ppEvents = new ArrayList<>(entry.getValue());

                        for (EventBean eb : ppEvents) {
                            try {
                                if(!eventMap.containsKey(ppId)) {
                                    eventMap.put(ppId,new ArrayList<EventBean>());
                                }
                                eventMap.get(ppId).add(eb);
                                String sensorValue = eb.get("avgValue").toString();
                                String sensorId = eb.get("sensorId").toString();
                                sb.append(sensorId + ":" + sensorValue + ",");
                                //tx_channel.basicPublish(outExchange, "", null, str.getBytes());

                                //logger.info("new id: " + query_id + " output: " + str);
                                //System.out.println(str);
                            } catch (Exception ex) {
                                System.out.println("COPESPEREngine : Error : " + ex.toString());
                            }

                        }
                        String sensorDataString = sb.toString().substring(0,sb.length() -1);
                        logger.debug("new id: " + query_id + " output: " + sensorDataString);
                        sendCPMessage(query_id,sensorDataString,ppId);
                    }


                } else if (query_id.equals("car_data")) {

                    String ppId = null;
                    StringBuilder sb = new StringBuilder();
                    for (EventBean eb : newEvents) {
                        try {
                            String carCount = eb.get("avgValue").toString();
                            ppId = eb.get("ppId").toString();
                            sb.append(ppId + ":" + carCount + ",");
                            //tx_channel.basicPublish(outExchange, "", null, str.getBytes());

                            //logger.info("new id: " + query_id + " output: " + str);
                            //System.out.println(str);
                        } catch (Exception ex) {
                            System.out.println("COPESPEREngine : Error : " + ex.toString());
                        }

                        String carDataString = sb.toString().substring(0,sb.length() -1);
                        logger.debug("new id: " + query_id + " output: " + carDataString);
                        sendCPMessage(query_id,carDataString,ppId);
                    }


                }
                if (oldEvents != null) {
                    for (EventBean eb : oldEvents) {
                        String str = eb.getUnderlying().toString();

                        try {
                            logger.debug("old id: " + query_id + " output: " + str);

                            //tx_channel.basicPublish(outExchange, "", null, str.getBytes());
                            //logger.info("id: " + query_id + " output: " +  str);
                            //System.out.println(str);
                        } catch (Exception ex) {
                            System.out.println("COPESPEREngine : Error : " + ex.toString());
                        }
                    }
                }
            }
        }
    }

    private void sendCPMessage(String key, String value, String ppId) {
        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, pp.copId, ppId, "");
        me.setParam(key,value);
        pp.sendout_cp.sendMessage(pp.cpId,me);
        //sendout.sendMessage(copId,me);

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
            System.out.println("COPESPEREngine addQuery: " + ex.toString());
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
            System.out.println("COPESPEREngine delQuery: " + ex.toString());
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
            System.out.println("COPESPEREngine : Input netFlow Error : " + ex.toString());
            System.out.println("COPESPEREngine : Input netFlow Error : InputStr " + inputStr);
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
