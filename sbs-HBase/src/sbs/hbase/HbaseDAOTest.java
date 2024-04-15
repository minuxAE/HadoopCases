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
		
		// 向表中插入数据
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
		// 测试批量插入
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
		
		// 向表中插入数据
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
		// 测试删除数据
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
		// 测试Get查询数据
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
				
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		Get get = new Get(Bytes.toBytes("user_01"));
		// 添加需要查询的列
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
		// 测试使用scan查询数据
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
				
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		// 查询范围为[)的形式
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
		// 条件过滤查询
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
				
		HTable t_sbs = new HTable(conf, "t_sbs");
		
		// 查询范围为[)的形式
		Scan scan = new Scan(Bytes.toBytes("user_01"), Bytes.toBytes("user_04"));
		scan.addFamily(Bytes.toBytes("School")); // 针对School列族过滤
		
		// 1.前缀过滤器
		// Filter preFilter = new PrefixFilter(Bytes.toBytes("user"));
		// scan.setFilter(preFilter);
		
		// 2.行过滤器
		// ByteArrayComparable comp = new BinaryComparator(Bytes.toBytes("user_01"));
		// RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, comp);
		// scan.setFilter(rowFilter);
		
		// 3.列值过滤器
		SubstringComparator comp = new SubstringComparator("002"); // 列值(school_id)中有002的给过滤出来
		SingleColumnValueFilter colFilter = new SingleColumnValueFilter(Bytes.toBytes("School"),
				Bytes.toBytes("school_id"), 
				CompareOp.EQUAL, 
				comp);
		colFilter.setFilterIfMissing(true);
		scan.setFilter(colFilter);
		
		// 迭代查询结果
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
		// 使用HBaseConfiguration.create()创建客户端
		// 可以自动加载xml配置文件
		Configuration conf = HBaseConfiguration.create();
		
		conf.set("hbase.zookeeper.quorum", "hadoop-server-00:2181,hadoop-server-01:2181,hadoop-server-02:2181");
		
		// DDL 操作对象
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Admin hbaseAdmin = conn.getAdmin();
		
		// HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
		
		// 在default namespace下建表
		TableName t_sbs = TableName.valueOf("t_sbs");
		
		// 表描述符
		HTableDescriptor htableDesc = new HTableDescriptor(t_sbs);
		
		// 列族描述符: school 
		HColumnDescriptor school = new HColumnDescriptor("School");

		// 将列族描述符添加到表中
		school.setMaxVersions(3); // 版本最多保持3个
		htableDesc.addFamily(school);
		
		// 列族描述符：title
		HColumnDescriptor title = new HColumnDescriptor("Title");
		title.setMaxVersions(3);
		htableDesc.addFamily(title);
		
		hbaseAdmin.createTable(htableDesc); // 创建表
		
		System.out.println("Table t_sbs created successfully...");
		
		hbaseAdmin.close();
	}
}
