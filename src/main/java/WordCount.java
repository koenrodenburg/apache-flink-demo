import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = StreamUtil.getDataStream(env, args);

        DataStream<Tuple2<String, Integer>> wordCountStream = dataStream
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (sentence, out) -> {
                    for(String word : sentence.split(" ")) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>(){}.getTypeInfo())
                .keyBy(0)
                .sum(1);

        wordCountStream.print();

        env.execute("WordCount");
    }
}
