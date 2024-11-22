import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import java.util.List;
import java.util.ArrayList;

public class CHIWeatherReducer
    extends Reducer<LongWritable, Text, LongWritable, Text> {

    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}

