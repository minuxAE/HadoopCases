package sbs.edu.storm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

//Spout组件负责获取数据
public class sbsSpout extends BaseRichSpout{
	// 定义传输数据的成员变量
	private SpoutOutputCollector collector;
	
	// 输入数据
	String[] strs = {"java", "python", "cpp", "rust", "go", "ruby", "perl", "javascript", "delphi"};
	
	@Override
	public void nextTuple() {
		// 设置业务被调用的周期
		Utils.sleep(1000);
		// 业务逻辑，像后续流程发送消息（tuple）,被worker中executor不断调用
		int index = new Random().nextInt(strs.length);
		String tech = strs[index];
		
		// 数据封装为tuple并发送
		collector.emit(new Values(tech));
		
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collect) {
		// 组件初始化方法
		this.collector = collect;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明输出消息的字段, 即tuple的schema
		declarer.declare(new Fields("Tech-Language"));
	}
	
	
}
