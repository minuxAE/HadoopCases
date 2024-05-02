package sbs.edu.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

// bolt组件1：功能是将发送过来的string转为大写
public class sbsToUpperCaseBolt extends BaseBasicBolt{

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// 业务逻辑, 被executor不断调用
		// tuple是上一个组件发过来的消息
		// collector用于向后发送消息
		String tech = tuple.getStringByField("Tech-Language"); // 根据字段获取数据
		// String tech = tuple.getString(0); // 根据字段索引编号获取数据
	
		// 转为大写
		String upperCaseTech = tech.toUpperCase();
		// 处理结果封装为消息发送出去
		collector.emit(new Values(upperCaseTech));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 用来声明组件发出消息的schema
		declarer.declare(new Fields("upper-tech"));
	}
	
}
