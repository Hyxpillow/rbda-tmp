import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CHICrashMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    private static final String[] columnNames = {
        "CRASH_RECORD_ID", "CRASH_DATE_EST_I", "CRASH_DATE", "POSTED_SPEED_LIMIT",
        "TRAFFIC_CONTROL_DEVICE", "DEVICE_CONDITION", "WEATHER_CONDITION", 
        "LIGHTING_CONDITION", "FIRST_CRASH_TYPE", "TRAFFICWAY_TYPE", 
        "LANE_CNT", "ALIGNMENT", "ROADWAY_SURFACE_COND", "ROAD_DEFECT", 
        "REPORT_TYPE", "CRASH_TYPE", "INTERSECTION_RELATED_I", 
        "NOT_RIGHT_OF_WAY_I", "HIT_AND_RUN_I", "DAMAGE", "DATE_POLICE_NOTIFIED", 
        "PRIM_CONTRIBUTORY_CAUSE", "SEC_CONTRIBUTORY_CAUSE", "STREET_NO", 
        "STREET_DIRECTION", "STREET_NAME", "BEAT_OF_OCCURRENCE", 
        "PHOTOS_TAKEN_I", "STATEMENTS_TAKEN_I", "DOORING_I", "WORK_ZONE_I", 
        "WORK_ZONE_TYPE", "WORKERS_PRESENT_I", "NUM_UNITS", 
        "MOST_SEVERE_INJURY", "INJURIES_TOTAL", "INJURIES_FATAL", 
        "INJURIES_INCAPACITATING", "INJURIES_NON_INCAPACITATING", 
        "INJURIES_REPORTED_NOT_EVIDENT", "INJURIES_NO_INDICATION", 
        "INJURIES_UNKNOWN", "CRASH_HOUR", "CRASH_DAY_OF_WEEK", "CRASH_MONTH", 
        "LATITUDE", "LONGITUDE", "LOCATION"
    };

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String line = value.toString();
        String[] splitLine = line.split(",", -1);

        for (int i = 0; i < splitLine.length; i++) {
            String columeName = columnNames[i];
            String columeValue = splitLine[i];
            if (columeName == "CRASH_DATE") {
                columeName = "CRASH_YEAR";
                columeValue = columeValue.substring(6, 10);
            }
            context.write(new Text(columeName), new Text(columeValue));
        }
    }
}

