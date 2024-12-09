import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class USMapper
    extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (key.get() == 0) {
            return;
        }

        String line = value.toString();
        List<String> splitLine = parseCSVLine(line);


        StringBuilder newLine = new StringBuilder();
        String dateWithoutHour = splitLine.get(3).substring(0,10);
        newLine.append(dateWithoutHour);
        newLine.append(",");
        newLine.append(splitLine.get(20));
        newLine.append(",");
        newLine.append(splitLine.get(22));
        newLine.append(",");
        newLine.append(splitLine.get(24));
        newLine.append(",");

        String conditions = splitLine.get(28);
        String rainOrNot;
        if (!conditions.contains("Clear")) {
            rainOrNot = "1";
        } else {
            rainOrNot = "0";
        }
        newLine.append(rainOrNot);

        context.write(NullWritable.get(), new Text(newLine.toString()));
    }

    public static List<String> parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        currentField.append(c);
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    currentField.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    result.add(currentField.toString());
                    currentField.setLength(0);
                } else {
                    currentField.append(c);
                }
            }
        }
        result.add(currentField.toString());
        return result;
    }
}



