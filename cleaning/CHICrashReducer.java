import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class CHICrashReducer
    extends Reducer<Text, IntWritable, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        
        int crashCount = 0;
        for (IntWritable value : values) {
            crashCount += value.get();
        }
        DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(key.toString(), inputFormat);
        String formattedDate = date.format(outputFormat);

        StringBuilder newLine = new StringBuilder();
        newLine.append(formattedDate);
        newLine.append(",");
        newLine.append(crashCount);

        context.write(NullWritable.get(), new Text(newLine.toString()));
    }
}

