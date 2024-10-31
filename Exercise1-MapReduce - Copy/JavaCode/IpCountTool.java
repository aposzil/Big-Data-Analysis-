import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IpCountTool extends Configured implements Tool {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      if (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text, IntWritable, IntWritable, Text> {
    private IntWritable result = new IntWritable();
    Map<IntWritable, List<Text>> listara = new TreeMap<>(Collections.reverseOrder());

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      if (!(key.toString().equals("ip"))){
	      if (listara.containsKey(result)) {
	    	  listara.get(new IntWritable(result.get())).add(new Text(key.toString()));
	      }else {
	    	  List<Text> tmp = new ArrayList<Text>();
	    	  tmp.add(new Text(key.toString()));
	    	  listara.put(new IntWritable(result.get()), tmp);
	      }
	  }
    }
    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
    	super.cleanup(context);
    	IntWritable hope1 = new IntWritable();
    	Text hope2 = new Text();
    	for (Map.Entry<IntWritable, List<Text>> entry : listara.entrySet()) {
    		hope1.set(entry.getKey().get());
    		hope2.set(entry.getValue().toString());
    		context.write(hope1, hope2);
    	} 	
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new IpCountTool(), args);
    System.exit(res);
  }



  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "ipcount");
    job.setJarByClass(IpCountTool.class);
    job.setNumReduceTasks(Integer.parseInt(args[2]));
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
