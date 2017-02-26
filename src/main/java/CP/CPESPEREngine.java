package CP;


import com.espertech.esper.client.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CPESPEREngine implements Runnable {

    private Launcher plugin;
    private CLogger logger;
    private CPEngine pp;


    //ESPER
    private static EPRuntime cepRT;
    private static EPAdministrator cepAdm;
    private static ConcurrentHashMap<String,CEPListener> listners;
    private static ConcurrentHashMap<String,EPStatement> statements;
    //private String esper_querystring;

    private static Gson gson;

    public CPESPEREngine(Launcher plugin, CPEngine pp)
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
            //cepConfig.addEventType("MsgEvent", MsgEvent.class.getName());

            /*
            Map<String, Object> sdef = new HashMap<>();
            sdef.put("ppId", String.class);
            sdef.put("copId", String.class);
            sdef.put("sensorId", String.class);
            sdef.put("sensorValue", float.class);

            cepConfig.addEventType("sensorMap", sdef);
            */

            Map<String, Object> csdef = new HashMap<>();
            csdef.put("ppId", String.class);
            csdef.put("copId", String.class);
            //cdef.put("carId", String.class);
            csdef.put("carValue", float.class);
            cepConfig.addEventType("carSpeedMap", csdef);

            Map<String, Object> ccdef = new HashMap<>();
            ccdef.put("ppId", String.class);
            ccdef.put("copId", String.class);
            //cdef.put("carId", String.class);
            ccdef.put("carValue", float.class);
            cepConfig.addEventType("carCountMap", ccdef);

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

            //addQuery("0", "select * from sensorMap");

            //addQuery("sensor_data", "select irstream copId, sensorId, avg(sensorValue) as avgValue from sensorMap.win:time(15 sec) group by sensorId output snapshot every 5 seconds");

            //ok
            //addQuery("car_data", "select irstream copId, count(carValue) as avgValue from carMap.win:time(15 sec) group by copId output snapshot every 5 seconds");
            //addQuery("car_data", "select irstream distinct copId, avg(carValue) as avgValue from carMap.win:time(15 sec) group by copId output snapshot every 5 seconds");
            addQuery("car_count", "select copId, avg(carValue) as avgValue from carCountMap.win:time_batch(15 sec) group by copId output snapshot every 5 seconds");

            //ok

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
                                //input(message);
                                //cepRT.sendEvent(me);

                                /*
                                String sensor_data = me.getParam("sensor_data");
                                String[] sensorArray = sensor_data.split(",");
                                for(String sensorEntry : sensorArray) {
                                    String[] sensorEntrySplit = sensorEntry.split(":");
                                    Map<String,Object> sensorMap = new HashMap<>();
                                    sensorMap.put("ppId",me.getMsgPlugin());
                                    sensorMap.put("copId",me.getMsgAgent());
                                    sensorMap.put("sensorId",sensorEntrySplit[0]);
                                    sensorMap.put("sensorValue", Float.parseFloat(sensorEntrySplit[1]));
                                    cepRT.sendEvent(sensorMap,"sensorMap");
                                }
                                */
                                /*
                        "car_count":"pp-eb468454-6d4c-472c-9adf-f725b91258b3:75"
                        "car_speed":"pp-9c5a58eb-9d09-4d37-952a-3c8e05ef4990:45.04615384615385"
                        "sensor_alert":"s8:100"
                        */
                                if(me.getParam("car_count") != null) {
                                    String[] car_data = me.getParam("car_count").split(":");
                                    Map<String,Object> carMap = new HashMap<>();
                                    carMap.put("ppId",me.getMsgPlugin());
                                    carMap.put("copId",me.getMsgAgent());
                                    carMap.put("carValue", Integer.parseInt(car_data[1]));
                                    logger.error(car_data[1]);
                                    //cepRT.sendEvent(carMap,"carCountMap");
                                }
                                else if(me.getParam("car_speed") != null) {
                                    String[] car_data = me.getParam("car_speed").split(":");
                                    Map<String,Object> carMap = new HashMap<>();
                                    carMap.put("ppId",me.getMsgPlugin());
                                    carMap.put("copId",me.getMsgAgent());
                                    carMap.put("carValue", Float.parseFloat(car_data[1]));
                                    cepRT.sendEvent(carMap,"carSpeedMap");
                                }


                                /*
                                String[] carArray = car_data.split(",");
                                for(String carEntry : carArray) {
                                    String[] carEntrySplit = carEntry.split(":");
                                    Map<String,Object> carMap = new HashMap<>();
                                    carMap.put("ppId",me.getMsgPlugin());
                                    carMap.put("copId",me.getMsgAgent());
                                    carMap.put("carId",carEntrySplit[0]);
                                    carMap.put("carValue", Integer.parseInt(carEntrySplit[1]));
                                    cepRT.sendEvent(carMap,"carMap");
                                }
                                */

                            }
                            else {
                                Thread.sleep(1000);
                            }

                }
                catch(Exception ex)
                {
                    String errorString = "QueryNode: Error: " + ex.toString();
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    ex.printStackTrace(pw);
                    logger.error(errorString);
                    logger.error(sw.toString());
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
                            String copId = eb.get("copId").toString();
                            if(!eventMap.containsKey(copId)) {
                                eventMap.put(copId,new ArrayList<EventBean>());
                                //logger.error("Create new ppid " + ppId + " thread: " + Thread.currentThread().getId());
                            }
                            eventMap.get(copId).add(eb);

                        } catch (Exception ex) {
                            System.out.println("CPESPEREngine : Error : " + ex.toString());
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
                        logger.info("new id: " + query_id + " output: " + sensorDataString);
                        //sendCPMessage(query_id,sensorDataString,ppId);
                    }


                } else if (query_id.equals("car_count")) {

                    String copId = null;
                    StringBuilder sb = new StringBuilder();
                    for (EventBean eb : newEvents) {
                        try {
                            String carCount = eb.get("avgValue").toString();
                            copId = eb.get("copId").toString();
                            logger.info(copId + ":" + carCount);
                            //sb.append(copId + ":" + carCount + ",");
                            //tx_channel.basicPublish(outExchange, "", null, str.getBytes());

                            //logger.info("new id: " + query_id + " output: " + str);
                            //System.out.println(str);
                        } catch (Exception ex) {
                            System.out.println("COPESPEREngine : Error : " + ex.toString());
                        }

                        //String carDataString = sb.toString().substring(0,sb.length() -1);
                       //logger.info("new id: " + query_id + " output: " + carDataString);
                        //sendCPMessage(query_id,carDataString,ppId);
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

    private void sendCPMessage(String key, String value, String ppId, String copId) {
        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, pp.cpId, copId, ppId, "");
        me.setParam(key,value);
        pp.sendout.sendMessage(copId,me);
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
