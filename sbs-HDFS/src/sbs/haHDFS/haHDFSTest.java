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
		// 加载配置文件进行解析hdfs-site.xml和core-site.xml
		// 参数可以自动解析
		// 即使传输过程中active节点失效，HA可以即时切换standby节点完成传输任务
		fs.copyFromLocalFile(new Path("C:/Minux/Tutorials/SBS-Hadoop/ExpFiles/jdk-8.gz"), new Path("/"));
		fs.close();
	}
}
