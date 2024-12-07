import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CHIJoin {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CHI <crash data path> <weather data path> <output path>");
            System.exit(-1);
        }

        Job jobJoin = Job.getInstance();
        jobJoin.setJarByClass(CHIJoin.class);
        jobJoin.setJobName("Chicago Crash Join Weather");
        MultipleInputs.addInputPath(jobJoin, new Path(args[0]), TextInputFormat.class, CHIJoinCrashMapper.class);
        MultipleInputs.addInputPath(jobJoin, new Path(args[1]), TextInputFormat.class, CHIJoinWeatherMapper.class);
        FileOutputFormat.setOutputPath(jobJoin, new Path(args[2]));
        jobJoin.setReducerClass(CHIJoinReducer.class);
        jobJoin.setMapOutputKeyClass(Text.class);
        jobJoin.setMapOutputValueClass(Text.class);
        jobJoin.setOutputKeyClass(NullWritable.class);
        jobJoin.setOutputValueClass(Text.class);
	    jobJoin.setNumReduceTasks(1);
        jobJoin.waitForCompletion(true);
    }
}