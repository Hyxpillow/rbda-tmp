import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.ArrayList;

public class CHICrashReducer
    extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        
        List<String> valueStrs = new ArrayList<>();
        for (Text value : values) {
            valueStrs.add(value.toString());
        }

        StringBuilder outputStr = new StringBuilder();
        getNull(valueStrs, outputStr);
        
        if (key.toString().equals("CRASH_YEAR")) {
            getMax(valueStrs, outputStr);
            getMin(valueStrs, outputStr);
        } else if (key.toString().equals("POSTED_SPEED_LIMIT")) {
            getMax(valueStrs, outputStr);
            getMin(valueStrs, outputStr);
        } else if (key.toString().equals("INJURIES_TOTAL")) {
            getSum(valueStrs, outputStr);
        }
        context.write(key, new Text(outputStr.toString()));
    }

    private void getNull(List<String> values, StringBuilder outputStr) {
        int totalCount = 0;
        int nullCount = 0;
        for (String value : values) {
            totalCount += 1;
            if (value.isEmpty()) {
                nullCount += 1;
            }
        }
        outputStr.append("nullCount:");
        outputStr.append(nullCount);
        outputStr.append(" ");
    }

    private void getMax(List<String> values, StringBuilder outputStr) {
        int maxValue = Integer.MIN_VALUE;
        for (String value : values) {
            if (!value.isEmpty()) {
                int year = Integer.parseInt(value.toString());
                maxValue = Math.max(maxValue, year);
            }
        }
        outputStr.append("max:");
        outputStr.append(maxValue);
        outputStr.append(" ");
        
    }

    private void getMin(List<String> values, StringBuilder outputStr) {
        int minValue = Integer.MAX_VALUE;
        for (String value : values) {
            if (!value.isEmpty()) {
                int year = Integer.parseInt(value.toString());
                minValue = Math.min(minValue, year);
            }
        }
        outputStr.append("min:");
        outputStr.append(minValue);
        outputStr.append(" ");
    }

    private void getAvg(List<String> values, StringBuilder outputStr) {
        int totalCount = 0;
        long totalSum = 0;
        for (String value : values) {
            if (!value.isEmpty()) {
                totalSum += Integer.parseInt(value.toString());
                totalCount += 1;
            }
        }
        outputStr.append("average:");
        outputStr.append(totalSum / totalCount);
        outputStr.append(" ");
    }

    private void getSum(List<String> values, StringBuilder outputStr) {
        long totalSum = 0;
        for (String value : values) {
            if (!value.isEmpty()) {
                totalSum += Integer.parseInt(value.toString());
            }
        }
        outputStr.append("sum:");
        outputStr.append(totalSum);
        outputStr.append(" ");
    }

}

