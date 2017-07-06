package drug;



import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path; //get list of input path for mapreduce job

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; //inputformat>>validate the input specification job,split up the file into logical instances
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.Iterator;  

public class Drug 
{	
   public static class chemical  extends 
   Mapper<LongWritable, Text, Text, Text>
   
{
    
    
  public void  map(LongWritable arg0,Text Value, Context context)
  throws IOException,InterruptedException  
  	{
		
String line = Value.toString();

if (!(line.length() == 0))  
{
String name = line.substring(0,16).trim();  
String year = line.substring(18,28);
float age1 = Float.parseFloat(line.substring(31,36).trim());
float age2 = Float.parseFloat(line.substring(39,44).trim());
float age3 = Float.parseFloat(line.substring(47,52).trim());
float age4 = Float.parseFloat(line.substring(55,60).trim());
float age5 = Float.parseFloat(line.substring(63,68).trim());
float age6 = Float.parseFloat(line.substring(71,76).trim());
float age7 = Float.parseFloat(line.substring(79,84).trim());
float age8 = Float.parseFloat(line.substring(87,92).trim());
float age9 = Float.parseFloat(line.substring(95,100).trim());


Float[] f =new Float[] {age1,age2,age3,age4,age5,age6,age7,age8,age9};

  for (int i=0 ;i<=8;i++)
   { 
	if (f[0]<f[i])
	 f[0]=f[i];
   }
  float max =f[0];

  for (int j=0 ;j<=8;j++)
  { 
	if (f[0]>f[j])
	 f[0]=f[j];
  }
  float min =f[0];
  
  	
context.write(new Text("chemical:- " +name + " year: " +year    )
		,new Text(" max value of "+name + " is:-" +max +" and min value is:- " +min ) );


  } 
}
}
public static class greater_age extends
Reducer<Text, Text, Text, Text> 
 {
    
    public void reduce(Text key, Iterator<Text> Values, Context context)
    		throws IOException, InterruptedException  
   {

String view = Values.next().toString();
context.write(key,new Text(view));

   }
 }
//main method

public static void main(String[] args) throws Exception 

 {

    Configuration conf = new Configuration();
     Job job = new Job( conf ,"drug");
    
    job.setJarByClass(Drug.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setMapperClass(chemical.class);
    job.setReducerClass(greater_age.class);

    job.setInputFormatClass(TextInputFormat.class);  //inputformat>>validate the input specification job,split up the file into logical instances
    job.setOutputFormatClass(TextOutputFormat.class);

    Path OutputPath = new Path(args[1]);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    OutputPath.getFileSystem(conf).delete(OutputPath);
    System.exit(job.waitForCompletion(true) ? 0 :1);
 }

}

