import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {    //KMeans主程式
	public static void main(String[] args) throws Exception{   //拋異常
		CenterInitial centerInitial = new CenterInitial();   //new一個CenterInitial實體(centerInitial)
		centerInitial.run(args);      //執行centerInitial的run方法(引數為args)
		int times = 0;    //宣告int型態time，值為0(次數)
		double s = 0;	  //宣告double型態s，值為0
		double shold = 0.0001;	//宣告double型態shold，值為0.0001
		do {      //do-while迴圈(先做一次，再判斷while條件是否再繼續進行
			Configuration conf = new Configuration();	
			conf.set("fs.default.name", "hdfs://140.120.108.223:9000");   //public String set(String name, String value)
			Job job = new Job(conf,"KMeans");
			job.setJarByClass(KMeans.class);
			job.setOutputKeyClass(Text.class);  //設定輸出key的類別
			job.setOutputValueClass(Text.class); //設定輸出的類別
			job.setMapperClass(KMapper.class);   //設定Mapper的類別
			job.setMapOutputKeyClass(Text.class);  //設定Map輸出key的類別
			job.setMapOutputValueClass(Text.class);  //設定Map輸出value的類別
			job.setReducerClass(KReducer.class);   //設定Reducer的類別
			/*
			Java抽象類org.apache.hadoop.fs.FileSystem定義了hadoop的一個文件系统接口。
			該類是一個抽象類，通過以下兩種靜態工廠方法可以過去FileSystem實例： 
			public static FileSystem.get(Configuration conf) throws IOException 
			public static FileSystem.get(URI uri, Configuration conf) throws IOException 
			*/
			FileSystem fs = FileSystem.get(conf);   
			fs.delete(new Path(args[2]),true);   //
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			job.waitForCompletion(true);
			if(job.waitForCompletion(true)){
				NewCenter newCenter = new NewCenter();
				s = newCenter.run(args);
				times++;
			}
		} while(s > shold);
		System.out.println("Iterator: " + times);		
	}
}