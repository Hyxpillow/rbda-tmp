package join;

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
        if (key.get() == 0) {
            return;
        }

        String line = value.toString();
        List<String> splitLine = parseCSVLine(line);
        String crashDate = splitLine.get(1);

        StringBuilder newLine = new StringBuilder();
        newLine.append(splitLine.get(4)); // temperature
        newLine.append(",");
        newLine.append(splitLine.get(9)); // humidity
        newLine.append(",");
        newLine.append(splitLine.get(21)); // visibility
        newLine.append(",");
        String conditions = splitLine.get(29);
        String rainOrNot;
        if (conditions.contains("Rain")) {
            rainOrNot = "1";
        } else {
            rainOrNot = "0";
        }
        newLine.append(rainOrNot);

        context.write(crashDate, new Text(newLine.toString()));
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