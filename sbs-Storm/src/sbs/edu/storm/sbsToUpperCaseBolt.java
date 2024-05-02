package sbs.edu.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

// bolt���1�������ǽ����͹�����stringתΪ��д
public class sbsToUpperCaseBolt extends BaseBasicBolt{

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// ҵ���߼�, ��executor���ϵ���
		// tuple����һ���������������Ϣ
		// collector�����������Ϣ
		String tech = tuple.getStringByField("Tech-Language"); // �����ֶλ�ȡ����
		// String tech = tuple.getString(0); // �����ֶ�������Ż�ȡ����
	
		// תΪ��д
		String upperCaseTech = tech.toUpperCase();
		// ��������װΪ��Ϣ���ͳ�ȥ
		collector.emit(new Values(upperCaseTech));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// �����������������Ϣ��schema
		declarer.declare(new Fields("upper-tech"));
	}
	
}
