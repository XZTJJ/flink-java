package com.zhouhc.streaming.ch04;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 *  使用文件作为source来读取 ,
 *  读文件默认是并行度读，将文件分块然后读取
 *  所以不一定是原始的文件顺序
 */
public class ReadFromFileSource {

    public static void main(String[] args) throws Exception {
        final String filePath = "C:\\Users\\f1355\\Desktop\\test\\certs\\test.txt";
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> filesource = environment.readFile(new TextInputFormat(new Path(filePath)),
                filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000, BasicTypeInfo.STRING_TYPE_INFO);
        //这里不做处理，直接删除
        filesource.print("file source");
        environment.execute("file source");
    }
}
