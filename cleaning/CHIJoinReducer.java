import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import java.util.List;
import java.util.ArrayList;

public class CHIJoinReducer
    extends Reducer<Text, Text, NullWritable, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        
        String crashLine = null;
        String weatherLine = null;
        
        for (Text value : values) {
            if (value.charAt(0) == 'C') { // Crash
                crashLine = value.toString().substring(1);
            } else if (value.charAt(0) == 'W') { // Weather
                weatherLine = value.toString().substring(1);
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(key.toString());
        sb.append(",");
        sb.append(crashLine);
        sb.append(",");
        sb.append(weatherLine);

        context.write(NullWritable.get(), new Text(sb.toString()));

    }
}

