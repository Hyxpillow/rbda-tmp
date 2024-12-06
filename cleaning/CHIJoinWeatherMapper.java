import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CHIJoinWeatherMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitLine = line.split(",");

        String weatherDate = splitLine[0];
        String weatherTemp = splitLine[1];
        String weatherRain = splitLine[2];
        
        context.write(new Text(weatherDate), new Text("W" + weatherTemp + "," + weatherRain));
    }
}