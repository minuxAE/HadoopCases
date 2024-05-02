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

//Spout��������ȡ����
public class sbsSpout extends BaseRichSpout{
	// ���崫�����ݵĳ�Ա����
	private SpoutOutputCollector collector;
	
	// ��������
	String[] strs = {"java", "python", "cpp", "rust", "go", "ruby", "perl", "javascript", "delphi"};
	
	@Override
	public void nextTuple() {
		// ����ҵ�񱻵��õ�����
		Utils.sleep(1000);
		// ҵ���߼�����������̷�����Ϣ��tuple��,��worker��executor���ϵ���
		int index = new Random().nextInt(strs.length);
		String tech = strs[index];
		
		// ���ݷ�װΪtuple������
		collector.emit(new Values(tech));
		
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collect) {
		// �����ʼ������
		this.collector = collect;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// ���������Ϣ���ֶ�, ��tuple��schema
		declarer.declare(new Fields("Tech-Language"));
	}
	
	
}
