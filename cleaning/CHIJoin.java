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

        Job jobCrashCleaning = Job.getInstance();
        jobCrashCleaning.setJarByClass(CHIJoin.class);
        jobCrashCleaning.setJobName("Chicago Crash Data Cleaning");
        FileInputFormat.addInputPath(jobCrashCleaning, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobCrashCleaning, new Path("project/output/crash_clean"));
        jobCrashCleaning.setMapperClass(CHICrashMapper.class);
        jobCrashCleaning.setReducerClass(CHICrashReducer.class);
        jobCrashCleaning.setMapOutputKeyClass(Text.class);
        jobCrashCleaning.setMapOutputValueClass(IntWritable.class);
        jobCrashCleaning.setOutputKeyClass(NullWritable.class);
        jobCrashCleaning.setOutputValueClass(Text.class);
	    jobCrashCleaning.setNumReduceTasks(1);
        jobCrashCleaning.waitForCompletion(true);

        Job jobWeatherCleaning = Job.getInstance();
        jobWeatherCleaning.setJarByClass(CHIJoin.class);
        jobWeatherCleaning.setJobName("Chicago Weather Data Cleaning");
        FileInputFormat.addInputPath(jobWeatherCleaning, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobWeatherCleaning, new Path("project/output/weather_clean"));
        jobWeatherCleaning.setMapperClass(CHIWeatherMapper.class);
        jobWeatherCleaning.setReducerClass(CHIWeatherReducer.class);
        jobWeatherCleaning.setOutputKeyClass(NullWritable.class);
        jobWeatherCleaning.setOutputValueClass(Text.class);
	    jobWeatherCleaning.setNumReduceTasks(1);
        jobWeatherCleaning.waitForCompletion(true);

        // all good before here

        Job jobJoin = Job.getInstance();
        jobJoin.setJarByClass(CHIJoin.class);
        jobJoin.setJobName("Chicago Crash Join Weather");
        MultipleInputs.addInputPath(jobJoin, new Path("project/output/crash_clean/part-r-00000"), TextInputFormat.class, CHIJoinCrashMapper.class);
        MultipleInputs.addInputPath(jobJoin, new Path("project/output/weather_clean/part-r-00000"), TextInputFormat.class, CHIJoinWeatherMapper.class);
        FileOutputFormat.setOutputPath(jobJoin, new Path(args[2]));
        jobJoin.setMapOutputKeyClass(Text.class);
        jobJoin.setMapOutputValueClass(Text.class);
        jobJoin.setOutputKeyClass(NullWritable.class);
        jobJoin.setOutputValueClass(Text.class);
	    jobJoin.setNumReduceTasks(1);
        jobJoin.waitForCompletion(true);

    }
}