package sbs.haHDFS;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class haHDFSTest {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// ���������ļ����н���hdfs-site.xml��core-site.xml
		// ���������Զ�����
		// ��ʹ���������active�ڵ�ʧЧ��HA���Լ�ʱ�л�standby�ڵ���ɴ�������
		fs.copyFromLocalFile(new Path("C:/Minux/Tutorials/SBS-Hadoop/ExpFiles/jdk-8.gz"), new Path("/"));
		fs.close();
	}
}
