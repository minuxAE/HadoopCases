package sbs.webStat;

import java.util.Map;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class statTopology {
	
	// Spout类, 负责数据读取, tuple封装和发送
	private static class StatSpout extends BaseRichSpout{

		private SpoutOutputCollector collector = null;
		
		@Override
		public void nextTuple() {
			Utils.sleep(1000);
			// 从消息缓存中获取URL：使用模拟产生的数据, ASCII在65~97
			int sed = (int)(Math.random()*25 + 65);
			char addr = (char)sed;
			String url = "https://"+addr+"/"+UUID.randomUUID();
			
			// 封装并发送tuple
			collector.emit(new Values(url));
		}

		@Override
		public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer decl) {
			decl.declare(new Fields("URL"));
			
		}
	}
	
	// Bolt类, 将处理后的数据写入存储系统中
	private static class StatBolt extends BaseBasicBolt{
		
		private Jedis jedis; 
		
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			jedis = new Jedis("hadoop-server-00", 6379);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String url = tuple.getString(0);
			
			if(url.length()<=10) return; // 合法性判定
			String site = url.substring(0, 10);
			
			// 写入到存储系统中（Redis）
			// key: zset的主键
			// score: 统计增加的权重
			// member: 被统计的对象
			jedis.zincrby("topSites", 1, site);
			
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer decl) {
			
			
		}
	}
	
	// topology的submitter, 提交到集群
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("statSpout", new StatSpout());
		builder.setBolt("statBolt", new StatBolt()).shuffleGrouping("statSpout");
		
		StormTopology topo = builder.createTopology();
		Config config = new Config();
		config.setNumWorkers(6);
		
		if(args.length > 0){
			StormSubmitter.submitTopology("urlTopo", config, topo);
		}else{
			LocalCluster lc = new LocalCluster();
			lc.submitTopology("urlTopo", config, topo);
		}
	}
}







