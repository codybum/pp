package app;


import CP.CPEngine;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.researchworx.cresco.library.messaging.MsgEvent;

import com.researchworx.cresco.library.utilities.CLogger;
import core.CLI;
import core.Launcher;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

public class AppTools {

    private Launcher plugin;
    private CLogger logger;
    private CPEngine cp;
    private Gson gson;

    public AppTools(Launcher plugin, CPEngine cp) {
        this.logger = new CLogger(AppTools.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        this.plugin = plugin;
        this.cp = cp;
        gson = new GsonBuilder().create();
    }

    public Map<String,String> getControllerResourceInventory() {

        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
        me.setParam("globalcmd", "resourceinventory");

        me = cp.cc.sendMsgEventReturn(me);

        String inventoryJSON = me.getParam("resourceinventory");
        Type type = new TypeToken<Map<String, String>>(){}.getType();
        Map<String, String> myMap = gson.fromJson(inventoryJSON, type);

        return myMap;
    }

    public String addQueues() {
        String pipelineId = null;
        try {
            MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
            me.setParam("globalcmd", "gpipelinesubmit");
            me.setParam("tenant_id", "0");

            Gson gson = new GsonBuilder().create();

            //public gNode(String type, String node_name, String node_id,Map<String, String> params)

            List<gNode> gNodes = new ArrayList<>();


            String[] locationIds = {"14", "15", "17", "18", "20", "21", "22", "23", "25", "26", "27", "28", "30", "31", "32", "33", "34", "36", "38"};

            int count = 0;

            for (String location : locationIds) {
                Map<String, String> n0Params = new HashMap<>();

                n0Params.put("pluginname", "cresco-container-plugin");
                n0Params.put("jarfile", "cresco-container-plugin-0.1.0.jar");
                n0Params.put("container_image", "rabbitmq:3-management");
                n0Params.put("e_params", "RABBITMQ_DEFAULT_USER:RABBITMQ_DEFAULT_PASS");
                n0Params.put("RABBITMQ_DEFAULT_USER", "admin");
                n0Params.put("RABBITMQ_DEFAULT_PASS", "cody01");
                n0Params.put("p_params", "5672:15672");
                n0Params.put("location", location);

                gNodes.add(new gNode("dummy", "node" + String.valueOf(count), String.valueOf(count), n0Params));
                count++;
            }

            //n0Params.put("pluginname","cresco-container-plugin");
            //n0Params.put("jarfile","cresco-container-plugin-0.1.0.jar");
            //n0Params.put("container_image", "gitlab.rc.uky.edu:4567/cresco/cresco-container");
            //CRESCO_GC_HOST
            //CRESCO_LOCATION
            //n0Params.put("e_params","CRESCO_LOCATION=home,CRESCO_AGENT_DISC_TIMEOUT=2000");
            //n0Params.put("location_region","home");
            //n0Params.put("location_agent","home");
            //n0Params.put("location","master");

        /*
        if(args.length == 3) {
            n0Params.put("location",args[2]);
            n0Params.put("e_params","CRESCO_LOCATION=" + args[2] + ",CRESCO_AGENT_DISC_TIMEOUT=2000");
        }
        else {
            n0Params.put("location","home");
            n0Params.put("e_params","CRESCO_LOCATION=home,CRESCO_AGENT_DISC_TIMEOUT=2000");
        }
        */

        /*
        if(args[1] != null) {
            int count = Integer.parseInt(args[1]);
            for(int i = 0; i < count; i++) {
                gNodes.add(new gNode("dummy", "node" + String.valueOf(i), String.valueOf(i), n0Params));
            }
        }
        else {
            gNodes.add(new gNode("dummy", "node0", "0", n0Params));
        }
        */

            //gNode n0 = new gNode("dummy", "node0", "0", n0Params);

            //List<gNode> gNodes = new ArrayList<>();
            //gNodes.add(n0);

            gEdge e0 = new gEdge("0", "1000000", "1000000");

            List<gEdge> gEdges = new ArrayList<>();
            gEdges.add(e0);

            gPayload gpay = new gPayload(gNodes, gEdges);
            gpay.pipeline_id = "0";
            gpay.pipeline_name = "demo_pipeline";

            String compressedGpay = DatatypeConverter.printBase64Binary(stringCompress(gson.toJson(gpay)));

            me.setParam("gpipeline_compressed", String.valueOf(Boolean.TRUE));

            me.setParam("gpipeline", compressedGpay);
            //gPayload me = gson.fromJson(json, gPayload.class);
            //System.out.println(p);
            //return gson.toJson(gpay);

            //System.out.println(me.getParams().toString());

            //me = CLI.cc.sendMsgEventReturn(me);

            me = cp.cc.sendJSONReturn("/addgpipeline", gson.toJson(me));

            //ce.setParam("gpipeline_id",gpay.pipeline_id);
            //System.out.println(returnString);
            //System.out.println("SUBMITTED");
            //System.out.println("PipelineId = " + me.getParam("gpipeline_id"));
            pipelineId = me.getParam("gpipeline_id");
        }
        catch(Exception ex) {
            logger.error("addQueues Error: " + ex.getMessage());
        }
        return pipelineId;
    }

    public String addCOP() {
        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
        me.setParam("globalcmd", "gpipelinesubmit");
        me.setParam("tenant_id","0");

        Gson gson = new GsonBuilder().create();

        //public gNode(String type, String node_name, String node_id,Map<String, String> params)

        List<gNode> gNodes = new ArrayList<>();


        String[] locationIds = {"14", "15", "17", "18", "20", "21", "22", "23", "25", "26", "27", "28", "30", "31", "32", "33", "34", "36", "38"};
        //String[] locationIds = {"14", "15", "17", "18", "20", "21", "22", "23", "25", "26", "27", "28", "30", "31", "32", "33", "34", "36", "38"};

        int count = 0;

        for(String location: locationIds) {
            Map<String, String> n0Params = new HashMap<>();

            //path_stage
            //ppFactory.setHost(plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1"));
            //ppFactory.setUsername(plugin.getConfig().getStringParam("pp_amqp_username","admin"));
            //ppFactory.setPassword(plugin.getConfig().getStringParam("pp_amqp_password","cody01"));

            //cpFactory.setHost(plugin.getConfig().getStringParam("cp_amqp_host","127.0.0.1"));
            //cpFactory.setUsername(plugin.getConfig().getStringParam("cp_amqp_username","admin"));
            //cpFactory.setPassword(plugin.getConfig().getStringParam("cp_amqp_password","cody01"));


            n0Params.put("pluginname", "cresco-pp");
            n0Params.put("jarfile", "cresco-pp-1.0-SNAPSHOT.jar");
            n0Params.put("pp_amqp_host","128.163.202." + location);
            n0Params.put("cop_id","cop-" + location);
            n0Params.put("pp_amqp_username","admin");
            n0Params.put("pp_amqp_password","cody01");
            n0Params.put("cp_amqp_host","cresco.uky.edu");
            n0Params.put("cp_amqp_username","admin");
            n0Params.put("cp_amqp_password","cody01");
            n0Params.put("path_stage","2");
            n0Params.put("location", location);

            gNodes.add(new gNode("dummy", "node" + String.valueOf(count), String.valueOf(count), n0Params));
            count++;
        }

        //n0Params.put("pluginname","cresco-container-plugin");
        //n0Params.put("jarfile","cresco-container-plugin-0.1.0.jar");
        //n0Params.put("container_image", "gitlab.rc.uky.edu:4567/cresco/cresco-container");
        //CRESCO_GC_HOST
        //CRESCO_LOCATION
        //n0Params.put("e_params","CRESCO_LOCATION=home,CRESCO_AGENT_DISC_TIMEOUT=2000");
        //n0Params.put("location_region","home");
        //n0Params.put("location_agent","home");
        //n0Params.put("location","master");

        /*
        if(args.length == 3) {
            n0Params.put("location",args[2]);
            n0Params.put("e_params","CRESCO_LOCATION=" + args[2] + ",CRESCO_AGENT_DISC_TIMEOUT=2000");
        }
        else {
            n0Params.put("location","home");
            n0Params.put("e_params","CRESCO_LOCATION=home,CRESCO_AGENT_DISC_TIMEOUT=2000");
        }
        */

        /*
        if(args[1] != null) {
            int count = Integer.parseInt(args[1]);
            for(int i = 0; i < count; i++) {
                gNodes.add(new gNode("dummy", "node" + String.valueOf(i), String.valueOf(i), n0Params));
            }
        }
        else {
            gNodes.add(new gNode("dummy", "node0", "0", n0Params));
        }
        */

        //gNode n0 = new gNode("dummy", "node0", "0", n0Params);

        //List<gNode> gNodes = new ArrayList<>();
        //gNodes.add(n0);

        gEdge e0 = new gEdge("0","1000000","1000000");

        List<gEdge> gEdges = new ArrayList<>();
        gEdges.add(e0);

        gPayload gpay = new gPayload(gNodes,gEdges);
        gpay.pipeline_id = "0";
        gpay.pipeline_name = "demo_pipeline";

        String compressedGpay = DatatypeConverter.printBase64Binary(stringCompress(gson.toJson(gpay)));

        me.setParam("gpipeline_compressed",String.valueOf(Boolean.TRUE));

        me.setParam("gpipeline",compressedGpay);
        //gPayload me = gson.fromJson(json, gPayload.class);
        //System.out.println(p);
        //return gson.toJson(gpay);

        System.out.println(me.getParams().toString());

        //me = CLI.cc.sendMsgEventReturn(me);

        me = cp.cc.sendJSONReturn("/addgpipeline",gson.toJson(me));

        //ce.setParam("gpipeline_id",gpay.pipeline_id);
        //System.out.println(returnString);
        //System.out.println("SUBMITTED");
        //System.out.println("PipelineId =" +  me.getParam("gpipeline_id"));
        return me.getParam("gpipeline_id");
    }

    public String addPP() {
        String pipelineId = null;
        try {
            MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
            me.setParam("globalcmd", "gpipelinesubmit");
            me.setParam("tenant_id", "0");

            Gson gson = new GsonBuilder().create();

            //public gNode(String type, String node_name, String node_id,Map<String, String> params)

            List<gNode> gNodes = new ArrayList<>();


            String[] locationIds = {"14", "15", "17", "18", "20", "21", "22", "23", "25", "26", "27", "28", "30", "31", "32", "33", "34", "36", "38"};
            //String[] locationIds = {"14", "15", "17", "18", "20", "21", "22", "23", "25", "26", "27", "28", "30", "31", "32", "33", "34", "36", "38"};

            int count = 0;

            for (String location : locationIds) {
                Map<String, String> n0Params = new HashMap<>();

                //path_stage
                //ppFactory.setHost(plugin.getConfig().getStringParam("pp_amqp_host","127.0.0.1"));
                //ppFactory.setUsername(plugin.getConfig().getStringParam("pp_amqp_username","admin"));
                //ppFactory.setPassword(plugin.getConfig().getStringParam("pp_amqp_password","cody01"));

                //cpFactory.setHost(plugin.getConfig().getStringParam("cp_amqp_host","127.0.0.1"));
                //cpFactory.setUsername(plugin.getConfig().getStringParam("cp_amqp_username","admin"));
                //cpFactory.setPassword(plugin.getConfig().getStringParam("cp_amqp_password","cody01"));

            /*
            n0Params.put("pluginname", "cresco-pp");
            n0Params.put("jarfile", "cresco-pp-1.0-SNAPSHOT.jar");
            n0Params.put("pp_amqp_host","128.163.202." + location);
            n0Params.put("pp_amqp_username","admin");
            n0Params.put("pp_amqp_password","cody01");
            n0Params.put("cop_id","cop-" + location);
            n0Params.put("path_stage","1");
            n0Params.put("location", location);
            */

                n0Params.put("pluginname", "cresco-container-plugin");
                n0Params.put("jarfile", "cresco-container-plugin-0.1.0.jar");
                n0Params.put("container_image", "gitlab.rc.uky.edu:4567/cresco/pp");
                n0Params.put("e_params", "CRESCO_path_stage:CRESCO_cop_id:CRESCO_pp_amqp_host:CRESCO_pp_amqp_username:CRESCO_pp_amqp_password:CRESCO_discovery_secret_agent:CRESCO_discovery_ipv4_agent_timeout");
                n0Params.put("CRESCO_path_stage", "1");
                n0Params.put("CRESCO_cop_id", "cop-" + location);
                n0Params.put("CRESCO_pp_amqp_host", "128.163.202." + location);
                n0Params.put("CRESCO_pp_amqp_username", "admin");
                n0Params.put("CRESCO_pp_amqp_password", "cody01");
                n0Params.put("location", location);
                n0Params.put("CRESCO_discovery_secret_agent", "cresco_discovery_secret" + location);
                n0Params.put("CRESCO_discovery_ipv4_agent_timeout", "20000");


                gNodes.add(new gNode("dummy", "node" + String.valueOf(count), String.valueOf(count), n0Params));
                count++;
            }

            //n0Params.put("pluginname","cresco-container-plugin");
            //n0Params.put("jarfile","cresco-container-plugin-0.1.0.jar");
            //n0Params.put("container_image", "gitlab.rc.uky.edu:4567/cresco/cresco-container");
            //CRESCO_GC_HOST
            //CRESCO_LOCATION
            //n0Params.put("e_params","CRESCO_LOCATION=home,CRESCO_AGENT_DISC_TIMEOUT=2000");
            //n0Params.put("location_region","home");
            //n0Params.put("location_agent","home");
            //n0Params.put("location","master");

        /*
        if(args.length == 3) {
            n0Params.put("location",args[2]);
            n0Params.put("e_params","CRESCO_LOCATION=" + args[2] + ",CRESCO_AGENT_DISC_TIMEOUT=2000");
        }
        else {
            n0Params.put("location","home");
            n0Params.put("e_params","CRESCO_LOCATION=home,CRESCO_AGENT_DISC_TIMEOUT=2000");
        }
        */

        /*
        if(args[1] != null) {
            int count = Integer.parseInt(args[1]);
            for(int i = 0; i < count; i++) {
                gNodes.add(new gNode("dummy", "node" + String.valueOf(i), String.valueOf(i), n0Params));
            }
        }
        else {
            gNodes.add(new gNode("dummy", "node0", "0", n0Params));
        }
        */

            //gNode n0 = new gNode("dummy", "node0", "0", n0Params);

            //List<gNode> gNodes = new ArrayList<>();
            //gNodes.add(n0);

            gEdge e0 = new gEdge("0", "1000000", "1000000");

            List<gEdge> gEdges = new ArrayList<>();
            gEdges.add(e0);

            gPayload gpay = new gPayload(gNodes, gEdges);
            gpay.pipeline_id = "0";
            gpay.pipeline_name = "demo_pipeline";

            String compressedGpay = DatatypeConverter.printBase64Binary(stringCompress(gson.toJson(gpay)));

            me.setParam("gpipeline_compressed", String.valueOf(Boolean.TRUE));

            me.setParam("gpipeline", compressedGpay);

            me = cp.cc.sendJSONReturn("/addgpipeline",gson.toJson(me));

            //ce.setParam("gpipeline_id",gpay.pipeline_id);
            //System.out.println(returnString);
            //System.out.println("SUBMITTED");
            //System.out.println("PipelineId =" +  me.getParam("gpipeline_id"));
            pipelineId = me.getParam("gpipeline_id");
        }
        catch(Exception ex) {
            logger.error("AddPP Error: " + ex.getMessage());
        }
        return pipelineId;
    }

    public String getGpipelineStatus(String pipelineId) {
        String pipelineStatus = null;
        try {

            MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
            me.setParam("globalcmd", "getgpipelinestatus");
            me.setParam("pipeline_id", pipelineId);
            me = cp.cc.sendMsgEventReturn(me);

            if (me == null) {
                //System.out.println("Can't get gpipeline");
                pipelineStatus = "-1";
            } else {
                //System.out.println(me.getParams().toString());
                if (me.getParam("status_code") != null) {
                    pipelineStatus = me.getParam("status_code");
                }
            }
        }
        catch (Exception ex) {
            logger.error("getGpipelineStatus Error: " + ex.getMessage());
        }
        return pipelineStatus;
    }

    public String getPluginStatus(String resource_id, String inode_id) {
        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
        //me.setParam("src_region", "external");
        //me.setParam("src_agent", "external");
        //me.setParam("dst_region", "external");
        //me.setParam("dst_agent", "external");
        me.setParam("globalcmd", "getpluginstatus");
        me.setParam("inode_id", inode_id);
        me.setParam("resource_id", resource_id);

        //return CLI.cc.sendMsgEvent(me);
        me = cp.cc.sendMsgEventReturn(me);
        //me.setParam("configparams", "perflevel="+ perflevel + ",pluginname=DummyPlugin,jarfile=cresco-agent-dummy-plugin-0.5.0-SNAPSHOT-jar-with-dependencies.jar,region=" + region  + ",watchdogtimer=5000");
        System.out.println(me.getMsgBody());

        if (me.getParam("status_code") != null) {
            System.out.println("status_desc:" + me.getParam("status_desc"));
            return me.getParam("status_code");
        }
        return null;
    }

    public void removeAllPipelines() {

        for(String pipelineId : getGpipelineList() ) {
            System.out.println("Removing Pipeline_id: " + pipelineId);
            removeGpipeline(pipelineId);
        }
    }

    public List<String> getGpipelineList() {

        List<String> pipelineList = null;
        try {
            pipelineList = new ArrayList<>();
            MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
            me.setParam("globalcmd", "getgpipelinelist");
            me = cp.cc.sendMsgEventReturn(me);

            if (me == null) {
                System.out.println("Can't get gpipeline");
            } else {
                if (me.getParam("gpipeline_ids") != null) {
                    String pipelineIdString = me.getParam("gpipeline_ids");
                    //System.out.println(pipelineIdString);
                    if(pipelineIdString.contains(",")) {
                        String[] pipelineIds = pipelineIdString.split(",");
                        for(String pipelineId : pipelineIds) {
                            pipelineList.add(pipelineId);
                        }
                    }
                    else {
                        pipelineList.add(pipelineIdString);
                    }
                }
                //System.out.println(me.getParams().toString());
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return pipelineList;
    }

    public void removeGpipeline(String pipelineId) {
        //String pipelineId = args[1];

            MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
            me.setParam("globalcmd", "gpipelineremove");
            me.setParam("pipeline_id", pipelineId);
            me = cp.cc.sendMsgEventReturn(me);

            if(me == null) {
                System.out.println("Can't get gpipeline");
            }
            else {
                System.out.println(me.getParams().toString());
            }

    }


    public byte[] stringCompress(String str) {
        byte[] dataToCompress = str.getBytes(StandardCharsets.UTF_8);
        byte[] compressedData = null;
        try
        {
            ByteArrayOutputStream byteStream =
                    new ByteArrayOutputStream(dataToCompress.length);
            try
            {
                GZIPOutputStream zipStream =
                        new GZIPOutputStream(byteStream);
                try
                {
                    zipStream.write(dataToCompress);
                }
                finally
                {
                    zipStream.close();
                }
            }
            finally
            {
                byteStream.close();
            }

            compressedData = byteStream.toByteArray();

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return compressedData;
    }

    public void getPluginInfo(String plugin_id) {
        List<String> inventory = new ArrayList<String>();
        MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get plugin info");
        me.setParam("globalcmd", "plugininfo");
        me.setParam("plugin_id", plugin_id);

        me = cp.cc.sendMsgEventReturn(me);

        if (me.getParam("node_name") != null) {
            System.out.println(me.getParam("node_name"));
            System.out.println("queue");
            System.out.println(me.getParam("node_id"));
            System.out.println(me.getParam("params"));
        }

    }

    public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Entry<K, V>> entries = new LinkedList<Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Entry<K, V>>() {

            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
                //return o1.getValue().compareTo(o2.getValue());
                return o2.getValue().compareTo(o1.getValue());

            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }


}
