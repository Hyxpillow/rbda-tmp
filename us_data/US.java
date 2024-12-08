import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class US {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: US <input path>  <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(US.class);
        job.setJobName("US Crash & Weather Cleaning");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(USMapper.class);
        job.setReducerClass(USReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }
}