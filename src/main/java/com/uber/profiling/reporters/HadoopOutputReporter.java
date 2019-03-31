package com.uber.profiling.reporters;

import com.uber.profiling.ArgumentUtils;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.JsonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HadoopOutputReporter implements Reporter {

    public final static String ARG_OUTPUT_PATH = "outputPath";

    private static final AgentLogger logger = AgentLogger.getLogger(HadoopOutputReporter.class.getName());

    private String basePath; // Should end in a '/' for s3
    private ConcurrentHashMap<String, PrintWriter> fileWriters = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    private FileSystem fs;

    public HadoopOutputReporter() { }

    public String getPath() {
        return basePath;
    }

    @Override
    public void updateArguments(Map<String, List<String>> parsedArgs) {
        String pathValue = ArgumentUtils.getArgumentSingleValue(parsedArgs, ARG_OUTPUT_PATH).toLowerCase();
        String accessKey = ArgumentUtils.getArgumentSingleValue(parsedArgs, "accesskey");
        String secretKey = ArgumentUtils.getArgumentSingleValue(parsedArgs, "secretkey");
        if (ArgumentUtils.needToUpdateArg(pathValue)) {
            basePath = pathValue.endsWith("/") ? pathValue : pathValue + "/";
            logger.info("Got argument value for pathValue: " + pathValue);
        }
        if (ArgumentUtils.needToUpdateArg(accessKey) && ArgumentUtils.needToUpdateArg(secretKey)) {
            logger.info("Got argument values for accessKey and secretKey"); // Don't print
        }

        if (pathValue.startsWith("s3://")) { // S3 OutputReporter
            String bucket = basePath.substring(0, basePath.indexOf("/", 5) + 1);
            try {
                Configuration conf = new Configuration();
                conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                if (ArgumentUtils.needToUpdateArg(accessKey) && ArgumentUtils.needToUpdateArg(secretKey)) {
                    conf.set("fs.s3a.access.key", accessKey);
                    conf.set("fs.s3a.secret.key", secretKey);
                }
                fs = FileSystem.get(URI.create(bucket), conf);
            } catch (IOException e) {
               e.printStackTrace();
            }
        } else { // ToDO: Implement HDFS OutputReporter

        }


    }

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        if (closed) {
            logger.info("Report already closed, do not report metrics");
            return;
        }

        PrintWriter writer = fileWriters.computeIfAbsent(profilerName, t -> createPrintWriter(t));
        writer.write(JsonUtils.serialize(metrics));
        writer.write(System.lineSeparator());
    }

    @Override
    public void close() {
        closed = true;
        List<PrintWriter> copy = new ArrayList<>(fileWriters.values());
        for (PrintWriter entry : copy) {
            entry.flush();
            entry.close();
        }
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private PrintWriter createPrintWriter(String profilerName) {
        Path currentPath = new Path(basePath + profilerName + ".jsons");
        try {
            FSDataOutputStream fsDataOutputStream = fs.create(currentPath);
            return new PrintWriter(fsDataOutputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create file writer: " + currentPath.getName(), e);
        }
    }


}
