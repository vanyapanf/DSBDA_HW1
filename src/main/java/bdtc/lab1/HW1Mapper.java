package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Маппер: проставляет единицы по определенным названиям областей экрана
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // одна writable сущность
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // справочник областей экрана
    private Map<String, int[]> screen_areas = new HashMap<String, int[]>() {{
        put("top-left", new int[] {0, 426, 682, 1024});
        put("mid-left", new int[] {0, 426, 341, 682});
        put("bot-left", new int[] {0, 426, 0, 341});
        put("top-mid", new int[] {426, 852, 682, 1024});
        put("mid-mid", new int[] {426, 852, 341, 682});
        put("bot-mid", new int[] {426, 852, 0, 341});
        put("top-right", new int[] {852, 1280, 682, 1024});
        put("mid-right", new int[] {852, 1280, 341, 682});
        put("bot-right", new int[] {852, 1280, 0, 341});
    }};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // формат строки
        String checkLogLineRegex = "^(\\d+) (\\d+) (\\d+) (\\d+)$";
        if (Pattern.matches(checkLogLineRegex, line)) {
            String[] line_parts = line.split(" ");
            int x = Integer.parseInt(line_parts[0]);
            int y = Integer.parseInt(line_parts[1]);
            // проверка попадания координат в одну из областей экрана
            if (getScreenArea(x,y) != null) {
                word.set(getScreenArea(x,y));
                context.write(word, one);
            }
            else {
                // плохие координаты
                context.getCounter(CounterType.MALFORMED).increment(1);
            }
        } else {
            // несоответствует формату строки
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }

    /**
     * функция для определения названия области экрана по координатам
     */
    protected String getScreenArea(int x, int y) {
        for (Map.Entry<String,int[]> entry: screen_areas.entrySet()) {
            int[] value = entry.getValue();
            if (x >= value[0] && x <= value[1] && y >= value[2] && y <= value[3])
                return entry.getKey();
        }
        return null;
    }


}
