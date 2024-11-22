import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Mapper;
public class CHIWeatherMapper
    extends Mapper<LongWritable, Text, Text, FloatWritable> {
    
    private static final String[] columnNames = {
        "name", "datetime", "tempmax", "tempmin", "temp", "feelslikemax", 
        "feelslikemin", "feelslike", "dew", "humidity", "precip", "precipprob", 
        "precipcover", "preciptype", "snow", "snowdepth", "windgust", 
        "windspeed", "winddir", "sealevelpressure", "cloudcover", "visibility", 
        "solarradiation", "solarenergy", "uvindex", "severerisk", "sunrise", 
        "sunset", "moonphase", "conditions", "description", "icon", "stations"
    };

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (key.get() == 0) {
            return;
        }

        String line = value.toString();
        List<String> splitLine = parseCSVLine(line);

        for (int i = 1; i < 21; i++) {
            String columeName = columnNames[i];
            if (columeName == "preciptype") {
                continue;
            }
            float columeValue = Float.parseFloat(splitLine.get(i));
            context.write(new Text(columeName), new FloatWritable(columeValue));
        }
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



