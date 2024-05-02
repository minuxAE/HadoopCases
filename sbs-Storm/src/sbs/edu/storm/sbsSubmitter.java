package sbs.edu.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

// 构造Topology对象，并向storm集群提交
public class sbsSubmitter {
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		// 1. 获取一个Topology构建器
		TopologyBuilder builder = new TopologyBuilder();
		
		// 2. 指定topo所用的spout组件类：参数1：spout的id; 参数2：spout的实例对象
		builder.setSpout("Tech-Spout", new sbsSpout());
		
		// 3. 指定topo所用的bolt组件, 并指定消息流的来源
		// 3.1 bolt 1
		builder.setBolt("upper-case-bolt", new sbsToUpperCaseBolt()).shuffleGrouping("Tech-Spout");
		// 3.2 bolt 2
		builder.setBolt("suffix-bolt", new sbsSuffixBolt()).shuffleGrouping("upper-case-bolt");
		
		// 4. 创建Topology对象
		StormTopology sbsTopo = builder.createTopology();
		
		// 5. 提交topo提交给集群
		// 5.1 建立config对象中封装参数
		Config config = new Config();
		// 5.2 设置为6个worker执行
		config.setNumWorkers(6);
		
		if(args.length>0){
			// 在Storm集群运行
			StormSubmitter.submitTopology(args[0], config, sbsTopo);
		}else{
			// 本地模拟测试, 需要修改文件写出的路径
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("sbs-Topo", config, sbsTopo);
		}
	}
	
}
