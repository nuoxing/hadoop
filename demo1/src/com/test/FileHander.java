package com.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * �ļ�����
 * @author sjakl
 *
 */
public class FileHander {

	

	public class FileSystemAPI {
		private static final String BASE_URL = "hdfs://192.168.88.128:9000";
		/**
		 * ׷�ӵ��ļ�
		 * @throws IOException 
		 */
		@Test
		public void testAppend() throws IOException{
			Configuration conf= new Configuration();
		    FileSystem fs = FileSystem.get(URI.create(BASE_URL+"/1.txt"),conf);
		    fs.append(new Path(BASE_URL+"/user/root/input/2.txt"));
		    System.out.println();
		}
		/**�����ļ��ϴ�����
		 * @throws IOException
		 */
		@Test
		public void testCopyFromLocalFile() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			fs.copyFromLocalFile(new Path("C:\\Users\\lenovo\\Desktop\\3.txt"), new Path(BASE_URL+"/user/root/input"));
		}
		/**��hdfs�����ļ�������
		 * @throws IOException
		 */
		@Test
		public void testCopyToLocalFile() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			fs.copyToLocalFile(new Path(BASE_URL+"/user/root/input/3.txt"), new Path("C:\\Users\\lenovo\\Desktop\\4.txt"));
		}
		/**�����ļ�����
		 * @throws IOException
		 */
		@Test
		public void testCreate() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/4.txt");
			FSDataOutputStream out = fs.create(path,true);
			out.write("hello hadoop".getBytes());
		}
		/**�������ļ�
		 * @throws IOException
		 */
		@Test
		public void testCreateNewFile() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/5.txt");
			fs.createNewFile(path);
		}
		/**ɾ���ļ�
		 * @throws IOException
		 */
		@Test
		public void testDelete() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/5.txt");
			fs.delete(path);
		}
		/**�ļ��Ƿ����
		 * @throws IOException
		 */
		@Test
		public void testExists() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/5.txt");
			System.out.println(fs.exists(path));
		}
		/**�ļ�״̬
		 * @throws IOException
		 */
		@Test
		public void testGetFileStatus() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/1.txt");
			FileStatus status = fs.getFileStatus(path);
			System.out.println(status.getModificationTime());
		}
		/**�ļ�״̬�����Ի���ļ�����Ϣ
		 * @throws IOException
		 */
		@Test
		public void testListFile() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input");
			FileStatus[] status = fs.listStatus(path);
			for (FileStatus fileStatus : status) {
				System.out.println(fileStatus.getPath());
			}
		}
		
		/**�����ļ���
		 * @throws IOException
		 */
		@Test
		public void testMkdirs() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/sub");
			fs.mkdirs(path);
		}
		/**�ƶ������ļ���hdfs
		 * @throws IOException
		 */
		@Test
		public void testMoveFromLocalFile() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/sub");
			fs.moveFromLocalFile(new Path("C:\\Users\\lenovo\\Desktop\\4.txt"), path);
		}
		/**������
		 * @throws IOException
		 */
		@Test
		public void testRename() throws IOException{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
			Path path = new Path("/user/root/input/sub/4.txt");
			Path dest = new Path("/user/root/input/sub/4_1.txt");
			fs.rename(path, dest);
		}
	}

}
