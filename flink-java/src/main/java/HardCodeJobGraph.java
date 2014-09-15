import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatelessSerializerFactory;
import org.apache.flink.client.minicluster.NepheleMiniCluster;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobInputVertex;
import org.apache.flink.runtime.jobgraph.JobOutputVertex;
import org.apache.flink.runtime.operators.DataSinkTask;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.operators.util.TaskConfig;


public class HardCodeJobGraph {

	//Data
	static Collection<Integer> inputData=new LinkedList<Integer>();
	static {
		inputData.add(1);inputData.add(2);inputData.add(3);
	}
	
	//*************************************************************************//
	//																		   //
	//       NEPHELE SETTINGS; ALMOST COPIED FROM LOCAL EXECUTOR 		       //
	//																		   //
	//*************************************************************************//
	
	private static NepheleMiniCluster nephele;
	private static final Object lock = new Object();	// we lock to ensure singleton execution
	
	// ---------------------------------- config options ------------------------------------------
	
		private static boolean DEFAULT_OVERWRITE = false;

		private static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = -1;
	
		private static int jobManagerRpcPort = -1;
		
		private static int taskManagerRpcPort = -1;
		
		private static int taskManagerDataPort = -1;

		private static int taskManagerNumSlots = DEFAULT_TASK_MANAGER_NUM_SLOTS;

		private static String configDir;

		private static String hdfsConfigFile;
		
		private static boolean defaultOverwriteFiles = DEFAULT_OVERWRITE;
		
		private static boolean defaultAlwaysCreateDirectory = false;

	// --------------------------------------------------------------------------------------------

	
	//*************************************************************************//
	//																		   //
	//       THE ACTUAL PROGRAM / JOB GRAPH						 		       //
	//																		   //
	//*************************************************************************//
		
	public static void main(String args[]) throws Exception{
		
		//initially create the JobGraph
		JobGraph jobGraph=buildJobGraph();
		
		//start up Nephele
		startNephele();
		
		//submit our job
		JobClient jobClient = nephele.getJobClient(jobGraph);
		JobExecutionResult result = jobClient.submitJobAndWait();
		
		//print some data from the result
		System.out.println("JobExecutionResult:\n"+
				"   NetRuntime: "+result.getNetRuntime()+"\n"+
				"   toString: "+result.toString()
		);
		
		//stop Nephele
		stop();
	}
	
	
	
	public static JobGraph buildJobGraph() throws Exception{
		// initially create the JobGraph
		JobGraph jg = new JobGraph("TEST");

		// create the vertices
		JobInputVertex inputVertex=createTheInputVertex(jg);
		JobOutputVertex outputVertex=createTheOutputVertex(jg);

		//connect vertexes
		inputVertex.connectTo(outputVertex);
		
		//return the final JobGraph
		return jg;
	}
	
	/**
	 * Started from the code in NepheleJobGraphGenerator.createDataSinkVertex(SinkPlanNode node)
	 * and tried to replace all not working method calls with respective object creations.
	 * @param jobGraph
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static JobInputVertex createTheInputVertex(JobGraph jobGraph){
		
		//**************************************************************
		//***Begin: code from NepheleJobGraphGenerator.createDataSourceVertex(SourcePlanNode node)
		
		//Values observed by debugging:
		//node.getNodeName() = "DataSource ([0, 1, 2, 3])"
		//node.getSerializer() = new RuntimeStatelessSerializerFactory(new IntSerializer(),Integer.class);
		//node.getPactContract().getUserCodeWrapper() = new UserCodeObjectWrapper<CollectionInputFormat<Integer>>(new CollectionInputFormat<Integer>(inputData, new IntSerializer()));
		//node.getPactContract().getParameters() = new Configuration()
		
		final JobInputVertex vertex = new JobInputVertex(
				"DataSource ([0, 1, 2, 3])" /*node.getNodeName()*/,
				jobGraph);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		
		//only for testing -> Hardcoded configurations
		vertex.getConfiguration().setInteger("out.num", 1);
		vertex.getConfiguration().setInteger("out.shipstrategy.0", 2);
		

		vertex.setInvokableClass(DataSourceTask.class);

		// set user code
		config.setStubWrapper(
				new UserCodeObjectWrapper<CollectionInputFormat<Integer>>(
						new CollectionInputFormat<Integer>(inputData, new IntSerializer())
				)
		);
		
		config.setStubParameters(new Configuration()); //Not 100% sure if this is correct

		config.setOutputSerializer(/*node.getSerializer()*/
				//unchecked rawtypes : I don't know what the correct type to be set at the moment,
				//therefore suppressed warnings...
				new RuntimeStatelessSerializerFactory(new IntSerializer(),Integer.class)
		);
		
		//***End: code from NepheleJobGraphGenerator.createDataSourceVertex(SourcePlanNode node)
		//**************************************************************
		
		return vertex;
	}
	
	
	/**
	 * Started from the code in NepheleJobGraphGenerator.createDataSinkVertex(SinkPlanNode node)
	 * and tried to replace all not working method calls with respective object creations.
	 * @param jobGraph
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static JobOutputVertex createTheOutputVertex(JobGraph jobGraph){
		
		//**************************************************************
		//***Begin: code from NepheleJobGraphGenerator.createDataSinkVertex(SinkPlanNode node)
		
		//Values observed by debugging:
		//node.getNodeName() = "DataSink(Print to System.out)"
		//node.getDegreeOfParallelism() = 2  //changed it to 1 for testing
		//node.getPactContract().getUserCodeWrapper()) = new UserCodeObjectWrapper(new PrintingOutputFormat())
		//node.getPactContract().getParameters() = new Configuration()
		
		final JobOutputVertex vertex = new JobOutputVertex(
				"DataSink(Print to System.out)" /*node.getNodeName()*/, jobGraph
		);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());

		vertex.setInvokableClass(DataSinkTask.class);
		vertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, 1 /*node.getDegreeOfParallelism()*/);
		
		// set user code
		config.setStubWrapper(
				//unchecked rawtypes : I don't know what the correct type to be set at the moment,
				//therefore suppressed warnings...
				new UserCodeObjectWrapper(new PrintingOutputFormat())
		);
		
		config.setStubParameters(new Configuration()); //Not sure if this is required...
		
		//only for testing -> Hardcoded configurations
		config.setInputSerializer(new RuntimeStatelessSerializerFactory(new IntSerializer(),Integer.class),0);
		vertex.getConfiguration().setInteger("in.groupsize.0",1);
		vertex.getConfiguration().setInteger("in.num", 1);
		
		//***End: code from NepheleJobGraphGenerator.createDataSinkVertex(SinkPlanNode node)
		//**************************************************************
		
		return vertex;
	}
	
	
	
	//*************************************************************************//
	//																		   //
	//       NEPHELE STARTUP AND STOP; ALMOST COPIED FROM LOCAL EXECUTOR       //
	//																		   //
	//*************************************************************************//
	
	private static void startNephele() throws Exception{
		if (nephele == null) {
			// configure the number of local slots equal to the parallelism of
			// the local plan
			if (taskManagerNumSlots == DEFAULT_TASK_MANAGER_NUM_SLOTS) {
				int maxParallelism = 2;//plan.getMaximumParallelism();
				if (maxParallelism > 0) {
					taskManagerNumSlots = maxParallelism;
				}
			}

			start();
		} else {
		}
	}
	
	private static void start() throws Exception {
		synchronized (lock) {
			if (nephele == null) {
				
				// create the embedded runtime
				nephele = new NepheleMiniCluster();
				
				// configure it, if values were changed. otherwise the embedded runtime uses the internal defaults
				if (jobManagerRpcPort > 0) {
					nephele.setJobManagerRpcPort(jobManagerRpcPort);
				}
				if (taskManagerRpcPort > 0) {
					nephele.setTaskManagerRpcPort(jobManagerRpcPort);
				}
				if (taskManagerDataPort > 0) {
					nephele.setTaskManagerDataPort(taskManagerDataPort);
				}
				if (configDir != null) {
					nephele.setConfigDir(configDir);
				}
				if (hdfsConfigFile != null) {
					nephele.setHdfsConfigFile(hdfsConfigFile);
				}
				nephele.setDefaultOverwriteFiles(defaultOverwriteFiles);
				nephele.setDefaultAlwaysCreateDirectory(defaultAlwaysCreateDirectory);
				nephele.setTaskManagerNumSlots(taskManagerNumSlots);
				
				// start it up
				nephele.start();
			} else {
				throw new IllegalStateException("The local executor was already started.");
			}
		}
	}

	/**
	 * Stop the local executor instance. You should not call executePlan after this.
	 */
	private static void stop() throws Exception {
		synchronized (lock) {
			if (nephele != null) {
				nephele.stop();
				nephele = null;
			} else {
				throw new IllegalStateException("The local executor was not started.");
			}
		}
	}
	
}
