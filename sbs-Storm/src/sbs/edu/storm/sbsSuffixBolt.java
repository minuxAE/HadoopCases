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

// bolt���2����Ӻ�׺��������ⲿ�洢
public class sbsSuffixBolt extends BaseBasicBolt{

	private FileWriter fw;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// bolt����ĳ�ʼ������, ��bolt���ʵ����ʱ������
		try {
			// ��Ⱥģʽ�ύ����Linux�е�·��
			fw = new FileWriter("/root/storm-output/"+UUID.randomUUID());
			// ����ģ���ύ����Windows�е�·��
			// fw = new FileWriter("C:/Minux/Tutorials/SBS-Hadoop/ExpFiles/storm-output/"+UUID.randomUUID());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// ҵ���߼�
		
		// 1. ��tuple�л�ȡ����
		String upperCaseTech = tuple.getString(0);
		
		// 2. ��Ӻ�׺
		String res = upperCaseTech + "-2024-05-05";
		
		// 3. ����д�뵽�ļ���
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
