package app;

import java.util.Map;


public class gNode {

	  public String type;
	  public String node_name;
	  public String node_id;
	  //public String query;
	  //public String outExchange;
	  public String top;
	  public String left;
	  public boolean isSource = false;
	  public Map<String, String> params;
	  
	  public gNode(String type, String node_name, String node_id, String top, String left, Map<String, String> params)
	  {
		  this.params = params;
		  this.type = type;
		  this.node_id = node_id;
		  this.node_name = node_name;
		  //this.query = query;
		  //this.outExchange = outExchange;
		  this.top = top;
		  this.left = left;
	  }
	  public gNode(String type, String node_name, String node_id,Map<String, String> params)
	  {
		  this.params = params;
		  this.type = type;
		  this.node_id = node_id;
		  this.node_name = node_name;
		  //this.query = query;
		  //this.outExchange = outExchange;
		 
	  }
	  
	 public String[] getIdentKey()
	 {
		 String[] identkey = null;
		 if(type.equals("query"))
		 {
			 //identkey = new String[] {"node_name","node_type","query"};
		 }
		 else
		 {
			 identkey = new String[] {"node_name","node_type"};
		 }
		 return identkey;
	 }
	 public Object[] getIdentValue()
	 {
		 
		 Object[] identkey = null;
		 if(type.equals("query"))
		 {
			 //identkey = new Object[] { "Sherlock" ,"Holmes"};
		//	 identkey = new Object[] {node_name,type,query};
		 }
		 else
		 {
			 identkey = new Object[] {node_name,type};
		 }
		 return identkey;
	 }
	  
	}