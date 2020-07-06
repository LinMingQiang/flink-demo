package com.flink.java.test;

import com.flink.learn.bean.WordCountGroupByKey;
import com.flink.learn.bean.WordCountPoJo;
import com.flink.learn.reader.WordCountJavaPojoKeyreader;
import com.flink.learn.reader.WordCountJavaPojoOpearateKeyreader;
import com.flink.learn.trans.AccountJavaPojoKeyedStateBootstrapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;


/**
 * 当 state 使用 ttlconfig的时候，readKeyedState的时候里面也要一样配
 */
public class StateProcessorTest extends AbstractTestBase implements Serializable {
    public static ExecutionEnvironment bEnv = null;

    static {
//        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
//                "LocalFlinkTest");
//        System.out.println(FlinkLearnPropertiesUtil.param().toMap().toString());
        bEnv = ExecutionEnvironment.getExecutionEnvironment();
//        FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
//                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
//                10000L);
    }

    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String sourcePath = path + "/SocketJavaPoJoWordcountTest/202007061807/e10174e03999d77fb7655a6e9c4f64b4/chk-3";
    public static String newPath = path + "/javatanssavepoint";


    /**
     * @throws Exception
     */
    @Test
    public void testPojoStateProcessor() throws Exception {
        remove(new File(newPath.substring(7, newPath.length())));
        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
        DataSet<WordCountPoJo> oldState1 = existSp.readKeyedState(
                uid,
                new WordCountJavaPojoKeyreader("wordcountState")
        );
        oldState1.print();
        // 对原始state做转换
        BootstrapTransformation<WordCountPoJo> transformation = OperatorTransformation
                .bootstrapWith(oldState1)
                // 必须要用 KeySelector 否则报 The generic type parameters of 'Tuple2' are missin
                .keyBy(new KeySelector<WordCountPoJo, WordCountGroupByKey>() {
                    @Override
                    public WordCountGroupByKey getKey(WordCountPoJo value) throws Exception {
                        WordCountGroupByKey k = new WordCountGroupByKey();
                        k.setKey(value.word);
                        return k;
                    }
                })// 确认状态的key
                .transform(new AccountJavaPojoKeyedStateBootstrapFunction()); // 对数据做修改
        //转换后的数据写入新的savepoint path
        existSp
                .removeOperator(uid)
                .withOperator(uid, transformation)
                .write(newPath);
        bEnv.execute("jel");

        ExistingSavepoint existSp2 = Savepoint.load(bEnv, newPath, new RocksDBStateBackend(path));
        existSp2.readKeyedState(
                uid,
                new WordCountJavaPojoKeyreader("wordcountState")
        ).print();
    }

    @Test
    public void testOpearteStateProcessor() throws Exception {
        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
        DataSet<WordCountPoJo> oldState1 = existSp.readListState(
                "wordcountsink",
                "opearatorstate",
                Types.POJO(WordCountPoJo.class));
        oldState1.print();
    }
//    @Test
//    public void testTuple2StateProcessor() throws Exception {
//        remove(new File(newPath.substring(7, newPath.length())));
//        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
//        DataSet<WordCountPoJo> oldState1 = existSp.readKeyedState(
//                uid,
//                new WordCountJavaTuple2Keyreader("wordcountState")
//        );
//        oldState1.print();
//        // 对原始state做转换
//        BootstrapTransformation<WordCountPoJo> transformation = OperatorTransformation
//                .bootstrapWith(oldState1)
//                // 必须要用 KeySelector 否则报 The generic type parameters of 'Tuple2' are missin
//                // https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/java_lambdas.html
//                .keyBy(new KeySelector<WordCountPoJo, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(WordCountPoJo value) throws Exception {
//                        return new Tuple2<String, String>(value.word, value.word);
//                    }
//                })// 确认状态的key
//                .transform(new AccountJavaTuple2KeyedStateBootstrapFunction()); // 对数据做修改
//        //转换后的数据写入新的savepoint path
//        existSp
//                .removeOperator(uid)
//                .withOperator(uid, transformation)
//                .write(newPath);
//        bEnv.execute("jel");
//
//        ExistingSavepoint existSp2 = Savepoint.load(bEnv, newPath, new RocksDBStateBackend(path));
//        existSp2.readKeyedState(
//                uid,
//                new WordCountJavaTuple2Keyreader("wordcountState")
//        ).print();
//    }


    /**
     * @param dir
     */
    public static void remove(File dir) {
        if (dir.exists()) {
            File files[] = dir.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    remove(files[i]);
                } else {
                    files[i].delete();
                }
            }
            //删除目录
            dir.delete();
        }
    }
}
