import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Iterator;

/**
 * Description: TestMain
 * Author: DIYILIU
 * Update: 2018-07-27 14:15
 */
public class TestMain {


    @Test
    public void test() {
        String uri = "hdfs://xg151:9000/tstar/mstar_tiza_trackdata/20180608";
        Configuration conf = new Configuration();
        // conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(new URI(uri), conf);
            Path path = new Path(uri);
            FileStatus status[] = fs.listStatus(path);
            for (int i = 0; i < status.length; i++) {
                System.out.println(status[i].getPath().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 输出文件内容
     */
    @Test
    public void test2() {
        String uri = "hdfs://xg151:9000/test/20180727/xg151.test1";
        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(new URI(uri), conf);
            Path path = new Path(uri);

            InputStream is = fs.open(path);
            String result = IOUtils.toString(is, StandardCharsets.UTF_8);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test3() {
        String uri = "hdfs://xg151:9000/test/";
        Configuration conf = new Configuration();

        Date date = new Date();
        String dateStr = String.format("%1$tY%1$tm%1$td", date);

        uri += dateStr;
        try {
            FileSystem fs = FileSystem.get(new URI(uri), conf);
            Path path = new Path(uri);

            boolean exist = fs.exists(path);
            if (exist) {

                System.out.println("文件已存在");
            } else {

                fs.mkdirs(path);
                System.out.println("成功创建目录");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件
     */
    @Test
    public void test4() {
        String uri = "hdfs://xg151:9000/test/";
        Configuration conf = new Configuration();

        Date date = new Date();
        String dateStr = String.format("%1$tY%1$tm%1$td", date);

        uri += dateStr + "/xg151.test1";
        try {
            FileSystem fs = FileSystem.get(new URI(uri), conf);
            Path path = new Path(uri);

            FSDataOutputStream outputStream = fs.create(path);
            for (int i = 0; i < 10; i++) {
                outputStream.writeBytes("data:" + i + System.getProperty("line.separator"));
            }
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件追加
     */
    @Test
    public void test5() {
        String uri = "hdfs://xg151:9000/test/";
        Configuration conf = new Configuration();

        Date date = new Date();
        String dateStr = String.format("%1$tY%1$tm%1$td", date);

        uri += dateStr + "/xg151.test3";
        try {
            FileSystem fs = FileSystem.get(new URI(uri), conf);
            Path path = new Path(uri);

            FSDataOutputStream outputStream = fs.append(path);
            for (int i = 0; i < 10; i++) {
                outputStream.writeBytes("data: -" + i + System.getProperty("line.separator"));
            }
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件删除
     */
    @Test
    public void test6() {
        String uri = "hdfs://xg151:9000/test";
        Configuration conf = new Configuration();

        try {
            FileSystem fs = FileSystem.get(new URI(uri), conf);
            Path path = new Path(uri);

            if (fs.exists(path)){
                boolean isDelete = fs.delete(path, true);
                //递归删除文件夹下所有文件
                String str = isDelete ? "Sucess" : "Error";
                System.out.println("删除" + str);
            }else {

                System.out.println("文件不存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
