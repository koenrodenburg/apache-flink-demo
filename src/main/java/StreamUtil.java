import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil {
    public static DataStream<String> getDataStream(StreamExecutionEnvironment env, String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = null;
        if(params.has("input")) {
            // Read the data from the text file at the provided path
            System.out.println("Executing with file input");
            dataStream = env.readTextFile(params.get("input"));
        } else if(params.has("host")) {
            // Read the data from the provided socket
            System.out.println("Executing with socket stream");
            dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        } else {
            System.out.println("Use --host and --port to specify socket connection");
            System.out.println("Use --input to specify file input");
            System.exit(1);
        }
        return dataStream;
    }
}
