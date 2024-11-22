import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CHIWeather {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CHIWeather <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(CHIWeather.class);
        job.setJobName("Chicago Weather Profiling");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CHIWeatherMapper.class);
        job.setReducerClass(CHIWeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

