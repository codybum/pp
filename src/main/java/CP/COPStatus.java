package CP;

import app.gEdge;
import app.gNode;
import com.researchworx.cresco.library.utilities.CLogger;
import core.Launcher;

import java.util.List;


public class COPStatus {

	private Launcher plugin;
	private CLogger logger;

	private CPEngine cp;
	public String copId;
	public String pipelineId;
	public String status_code;
	private boolean inAction = false;


	  public COPStatus(Launcher plugin, CPEngine cp, String copId)
	  {
		  this.logger = new CLogger(COPStatus.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		  this.plugin = plugin;
          this.cp = cp;
		  this.copId = copId;
	  	//this.pipelineId = pipelineId;
	  	//this.status_code = status_code;
	  }

	  public void update(String carCount) {
		 if(!inAction) {
		 	inAction = true;
			 if (Float.parseFloat(carCount) > 300.0) {
				 if (pipelineId == null) {
					 //pipelineId = cp.at.addCOP(copId);
				     logger.info("start " + pipelineId + " high " + copId + ":" + carCount);
				 }
			 } else if (Float.parseFloat(carCount) < 240.0) {
			 	if(pipelineId != null) {
					//logger.info("stop low " + copId + ":" + carCount);
					//check that it has started
                    status_code = cp.at.getGpipelineStatus(pipelineId);
                    //if(status_code.equals("10")) {
                    //    cp.at.removeGpipeline(pipelineId);
                        logger.info("removing " + pipelineId + " low " + copId + ":" + carCount);
                    //}
				}
			 }
			 inAction = false;
		 }
	  }

	}