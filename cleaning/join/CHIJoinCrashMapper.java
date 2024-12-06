import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CHIJoinCrashMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String line = value.toString();
        String[] splitLine = line.split(",");
        String crashDate = splitLine[0];
        String crashCount = splitLine[1];
        
        context.write(new Text(crashDate), new Text("C" + crashCount));
    }
}