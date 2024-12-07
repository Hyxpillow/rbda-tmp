import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class CHIJoinCrashMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (key.get() == 0) {
            return;
        }
        String line = value.toString();
        String[] splitLine = line.split(",");
        String crashDate = splitLine[2];
        String dateWithoutHour = crashDate.substring(0,10);

        DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(key.toString(), inputFormat);
        crashDate = date.format(outputFormat);
        
        context.write(new Text(crashDate), new Text("Crash"));
    }
}