import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CHICrash {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CHICrash <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(CHICrash.class);
        job.setJobName("Chicago Crash Profiling");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CHICrashMapper.class);
        job.setReducerClass(CHICrashReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);
	

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

