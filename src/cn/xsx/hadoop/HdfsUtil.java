package cn.xsx.hadoop;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtil {
	FileSystem fs = null;
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop:9000/");
//		fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://hadoop:9000/"), conf, "hadoop");
	}
	
	/**
	 * 下载文件
	 */
	@Test
	public  void download() throws Exception {
		// upload file
		Path src = new Path("hdfs://hadoop:9000/系统镜像/win8.1.iso");
		fs.copyToLocalFile(false,src, new Path("F:/系统镜像/win8.1.iso"),true);
	}
	
	/**
	 * 上传文件
	 * @throws Exception 
	 */
	@Test
	public void upload() throws Exception{
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop:9000/");
//		FileSystem fs = FileSystem.get(conf);
		
		Path dst = new Path("hdfs://hadoop:9000/test/misnshi.docx");
		FSDataOutputStream os = fs.create(dst);
		
		FileInputStream is = new FileInputStream("C:\\Users\\xusha\\Desktop/柴柴-BI 校招试题.docx");
		IOUtils.copy(is, os);
	}
	
	@Test
	public void upload2() throws Exception{
		fs.mkdirs(new Path("/系统镜像"));
		//fs.copyFromLocalFile(new Path("C:/Users/xusha/Desktop/大数据+云计算（30多套教程打包珍藏版）.txt"), new Path("hdfs://hadoop:9000/test/大数据资料.txt"));
		fs.copyFromLocalFile(new Path("f:/cn_windows_8.1_with_update_x64_dvd_4048046.iso"), new Path("hdfs://hadoop:9000/winiso/win8.1.iso"));
	}
	
	/**
	 * 查看文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException{
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while(files.hasNext()){
			LocatedFileStatus file = files.next();
			Path fielPath = file.getPath();
			String fileName = fielPath.getName();
			System.out.println(fileName+"  "+file.getLen()+"   "+file.getBlockSize()+"  "+fileName.getBytes());
		}
		
		System.out.println("=================================");
		FileStatus[] listStstus = fs.listStatus(new Path("/系统镜像"));
		for(FileStatus status:listStstus){
			String name = status.getPath().getName();
			System.out.println(name);
		}
	
	}
	
	/**
	 * 创建目录
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, Exception{
		fs.mkdirs(new Path("/test/a/b"));
	}
	
	/**
	 * 删除文件/文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void rm() throws Exception{
		fs.delete(new Path("/系统镜像"),true);
//		fs.moveToLocalFile(new Path("hdfs://hadoop:9000/系统镜像/win8.1.iso"), new Path("hdfs://hadoop:9000/win8.1.iso"));
	}
}
