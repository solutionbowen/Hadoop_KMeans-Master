import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class CenterInitial {
	public void run(String[] args) throws IOException{
		String[] clist;
		int k = 5;
		String string = "";
		String inpath = args[0]+"/cluster";  //cluster
		String outpath = args[1]+"/center";  //center
		Configuration conf1 = new Configuration(); //讀取hadoop文件系统的配置
		conf1.set("hadoop.job.ugi", "hadoop,hadoop"); 
		FileSystem fs = FileSystem.get(URI.create(inpath),conf1); //FileSystem是用户操作HDFS的核心類，它獲得URI對應的HDFS文件系统 
		FSDataInputStream in = null; 
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try{ 
			in = fs.open( new Path(inpath) ); 
			IOUtils.copyBytes(in,out,50,false);  //用Hadoop的IOUtils工具方法來讓這個文件的指定字節複製到標準輸出流上 
			clist = out.toString().split(" ");
			} finally { 
				IOUtils.closeStream(in);
			}
		FileSystem filesystem = FileSystem.get(URI.create(outpath), conf1); 
		for(int i=0;i<k;i++){
			int j=(int) (Math.random()*100) % clist.length;
			if(string.contains(clist[j])){  // choose the same one
				k++;
				continue;
			}
			string = string + clist[j].replace(" ", "") + " ";
		}
		OutputStream out2 = filesystem.create(new Path(outpath) ); 
		IOUtils.copyBytes(new ByteArrayInputStream(string.getBytes()), out2, 4096,true); //write string
		System.out.println(string);
	}

}