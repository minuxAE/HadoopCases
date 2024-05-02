package sbs.edu.storm;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

// bolt组件2：添加后缀，输出到外部存储
public class sbsSuffixBolt extends BaseBasicBolt{

	private FileWriter fw;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// bolt组件的初始化方法, 在bolt组件实例化时被调用
		try {
			// 集群模式提交：在Linux中的路径
			fw = new FileWriter("/root/storm-output/"+UUID.randomUUID());
			// 本地模型提交：在Windows中的路径
			// fw = new FileWriter("C:/Minux/Tutorials/SBS-Hadoop/ExpFiles/storm-output/"+UUID.randomUUID());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// 业务逻辑
		
		// 1. 从tuple中获取数据
		String upperCaseTech = tuple.getString(0);
		
		// 2. 添加后缀
		String res = upperCaseTech + "-2024-05-05";
		
		// 3. 数据写入到文件中
		try {
			fw.write(res+"\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}
