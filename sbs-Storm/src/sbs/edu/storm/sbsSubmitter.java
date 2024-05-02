package sbs.edu.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

// ����Topology���󣬲���storm��Ⱥ�ύ
public class sbsSubmitter {
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		// 1. ��ȡһ��Topology������
		TopologyBuilder builder = new TopologyBuilder();
		
		// 2. ָ��topo���õ�spout����ࣺ����1��spout��id; ����2��spout��ʵ������
		builder.setSpout("Tech-Spout", new sbsSpout());
		
		// 3. ָ��topo���õ�bolt���, ��ָ����Ϣ������Դ
		// 3.1 bolt 1
		builder.setBolt("upper-case-bolt", new sbsToUpperCaseBolt()).shuffleGrouping("Tech-Spout");
		// 3.2 bolt 2
		builder.setBolt("suffix-bolt", new sbsSuffixBolt()).shuffleGrouping("upper-case-bolt");
		
		// 4. ����Topology����
		StormTopology sbsTopo = builder.createTopology();
		
		// 5. �ύtopo�ύ����Ⱥ
		// 5.1 ����config�����з�װ����
		Config config = new Config();
		// 5.2 ����Ϊ6��workerִ��
		config.setNumWorkers(6);
		
		if(args.length>0){
			// ��Storm��Ⱥ����
			StormSubmitter.submitTopology(args[0], config, sbsTopo);
		}else{
			// ����ģ�����, ��Ҫ�޸��ļ�д����·��
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("sbs-Topo", config, sbsTopo);
		}
	}
	
}
