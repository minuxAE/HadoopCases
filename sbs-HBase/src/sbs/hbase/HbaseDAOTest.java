package sbs.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;



public class HbaseDAOTest {
	
	@Test
	public void testPut() throws IOException{
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
		
		// ����в�������
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		Put put = new Put(Bytes.toBytes("user_01"));
		put.add(Bytes.toBytes("School"), Bytes.toBytes("school_id"), Bytes.toBytes("info-001-002"));
		put.add(Bytes.toBytes("School"), Bytes.toBytes("school_name"), Bytes.toBytes("infomation"));
		put.add(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"), Bytes.toBytes("Jack"));
		
		t_sbs.put(put);
		t_sbs.close();
	}
	
	
	@Test
	public void testPuts() throws IOException{
		// ������������
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
		
		// ����в�������
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		Put put1 = new Put(Bytes.toBytes("user_02"));
		put1.add(Bytes.toBytes("School"), Bytes.toBytes("school_id"), Bytes.toBytes("info-002-002"));
		put1.add(Bytes.toBytes("School"), Bytes.toBytes("school_name"), Bytes.toBytes("computer"));
		put1.add(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"), Bytes.toBytes("Oack"));
		
		Put put2 = new Put(Bytes.toBytes("user_03"));
		put2.add(Bytes.toBytes("School"), Bytes.toBytes("school_id"), Bytes.toBytes("info-002-001"));
		put2.add(Bytes.toBytes("School"), Bytes.toBytes("school_name"), Bytes.toBytes("engineer"));
		put2.add(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"), Bytes.toBytes("Zack"));
		
		ArrayList<Put> puts = new ArrayList<Put>();
		puts.add(put1);
		puts.add(put2);
		
		t_sbs.put(puts);
		t_sbs.close();
	}
	
	@Test
	public void testDel() throws IOException{
		// ����ɾ������
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
		
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		Delete del1 = new Delete(Bytes.toBytes("user_02"));
		del1.deleteColumn(Bytes.toBytes("School"), Bytes.toBytes("school_name"));
		
		Delete del2 = new Delete(Bytes.toBytes("user_03"));
		del2.deleteColumn(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"));
		
		ArrayList<Delete> dels = new ArrayList<Delete>();
		dels.add(del1);
		dels.add(del2);
		
		t_sbs.delete(dels);
		t_sbs.close();
	}
	
	@Test
	public void testGet() throws IOException{
		// ����Get��ѯ����
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
				
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		Get get = new Get(Bytes.toBytes("user_01"));
		// �����Ҫ��ѯ����
		get.addColumn(Bytes.toBytes("School"), Bytes.toBytes("school_id"));
		get.addColumn(Bytes.toBytes("School"), Bytes.toBytes("school_name"));
		get.addColumn(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"));
		
		Result res = t_sbs.get(get);
		
		byte[] user01_school_id = res.getValue(Bytes.toBytes("School"), Bytes.toBytes("school_id"));
		byte[] user01_school_name = res.getValue(Bytes.toBytes("School"), Bytes.toBytes("school_name"));
		byte[] user01_prof_id = res.getValue(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"));
		
		System.out.println(Bytes.toString(user01_school_id));
		System.out.println(Bytes.toString(user01_school_name));
		System.out.println(Bytes.toString(user01_prof_id));
		
		t_sbs.close();
	}
	
	@Test
	public void testScan() throws IOException{
		// ����ʹ��scan��ѯ����
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
				
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		// ��ѯ��ΧΪ[)����ʽ
		Scan scan = new Scan(Bytes.toBytes("user_01"), Bytes.toBytes("user_04"));
		ResultScanner scanner = t_sbs.getScanner(scan);
		
		Iterator<Result> itor = scanner.iterator();
		
		while(itor.hasNext()){
			Result res = itor.next();
			
			byte[] u_school_id = res.getValue(Bytes.toBytes("School"), Bytes.toBytes("school_id"));
			byte[] u_school_name = res.getValue(Bytes.toBytes("School"), Bytes.toBytes("school_name"));
			byte[] u_prof_id = res.getValue(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"));
			
			System.out.println(Bytes.toString(u_school_id)+" "+Bytes.toString(u_school_name)+" "+Bytes.toString(u_prof_id));
		}
		
		t_sbs.close();
	}
	
	@Test
	public void testScanFilter() throws IOException{
		// �������˲�ѯ
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
				
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		// ��ѯ��ΧΪ[)����ʽ
		Scan scan = new Scan(Bytes.toBytes("user_01"), Bytes.toBytes("user_04"));
		scan.addFamily(Bytes.toBytes("School")); // ���School�������
		
		// 1.ǰ׺������
		// Filter preFilter = new PrefixFilter(Bytes.toBytes("user"));
		// scan.setFilter(preFilter);
		
		// 2.�й�����
		// ByteArrayComparable comp = new BinaryComparator(Bytes.toBytes("user_01"));
		// RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, comp);
		// scan.setFilter(rowFilter);
		
		// 3.��ֵ������
		SubstringComparator comp = new SubstringComparator("002"); // ��ֵ(school_id)����002�ĸ����˳���
		SingleColumnValueFilter colFilter = new SingleColumnValueFilter(Bytes.toBytes("School"),
				Bytes.toBytes("school_id"), 
				CompareOp.EQUAL, 
				comp);
		colFilter.setFilterIfMissing(true);
		scan.setFilter(colFilter);
		
		// ������ѯ���
		ResultScanner scanner = t_sbs.getScanner(scan);
		
		Iterator<Result> itor = scanner.iterator();
		
		while(itor.hasNext()){
			Result res = itor.next();
			
			byte[] u_school_id = res.getValue(Bytes.toBytes("School"), Bytes.toBytes("school_id"));
			byte[] u_school_name = res.getValue(Bytes.toBytes("School"), Bytes.toBytes("school_name"));
			byte[] u_prof_id = res.getValue(Bytes.toBytes("Title"), Bytes.toBytes("prof_id"));
			
			System.out.println(Bytes.toString(u_school_id)+" "+Bytes.toString(u_school_name)+" "+Bytes.toString(u_prof_id));
		}
		
		t_sbs.close();
		
	}
	
	
	
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		// ʹ��HBaseConfiguration.create()�����ͻ���
		// �����Զ�����xml�����ļ�
		Configuration conf = HBaseConfiguration.create();
		
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
		
		// DDL ��������
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Admin hbaseAdmin = conn.getAdmin();
		
		// HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
		
		// ��default namespace�½���
		TableName t_sbs = TableName.valueOf("t_sbs");
		
		// ��������
		HTableDescriptor htableDesc = new HTableDescriptor(t_sbs);
		
		// ����������: school 
		HColumnDescriptor school = new HColumnDescriptor("School");

		// ��������������ӵ�����
		school.setMaxVersions(3); // �汾��ౣ��3��
		htableDesc.addFamily(school);
		
		// ������������title
		HColumnDescriptor title = new HColumnDescriptor("Title");
		title.setMaxVersions(3);
		htableDesc.addFamily(title);
		
		hbaseAdmin.createTable(htableDesc); // ������
		
		System.out.println("Table t_sbs created successfully...");
		
		hbaseAdmin.close();
	}
}
