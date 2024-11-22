import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CHICrashMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (key.get() == 0) {
            return;
        }
        String line = value.toString();
        List<String> splitLine = line.split(",");
        String crashDate = splitLine[1];
        String dateWithoutHour = crashDate.substring(0,10);
        
        context.write(new Text(dateWithoutHour), new IntWritable(1));
    }
}

