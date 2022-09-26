package hadoop_test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;


public class Utils_hadoop {

    /**
     * 路径是否存在
     */
    public static boolean testExist(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }

    /**
     * 创建目录
     */
    public static boolean mkdir(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        boolean result = fs.mkdirs(dirPath);
        fs.close();
        return result;
    }

    /**
     * 删除目录
     */
    public static boolean rmDir(Configuration conf, String remoteDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        /* 第二个参数表示是否递归删除所有文件 */
        boolean result = fs.delete(dirPath, true);
        fs.close();
        return result;
    }

    /**
     * 创建文件
     */
    public static void touch(Configuration conf, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataOutputStream outputStream = fs.create(remotePath);
        outputStream.close();
        fs.close();
    }

    /**
     * 删除文件
     */
    public static boolean rm(Configuration conf, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        boolean result = fs.delete(remotePath, false);
        fs.close();
        return result;
    }

    /**
     * 追加文件内容  到末尾
     */
    public static void appendContentToFile(Configuration conf, String content, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataOutputStream out = fs.append(remotePath);
        out.write(content.getBytes());
        out.close();
        fs.close();
    }

    /**
     * 追加文件内容  到开头
     */
    public static void appendContentToFile1(Configuration conf, String content, String remoteFilePath) throws IOException {
        String localTmpPath = "/usr/local/hadoop/enen.txt";

        // 移动到本地
        moveToLocalFile(conf, remoteFilePath, localTmpPath);
        // 创建一个新文件
        touch(conf, remoteFilePath);
        // 先写入新内容
        appendContentToFile(conf, content, remoteFilePath);
        // 再写入原来内容
        appendContentToFile(conf, localTmpPath, remoteFilePath);

        System.out.println("已追加内容到文件开头: " + remoteFilePath);
    }

    /**
     * 复制文件到指定路径
     * <p>
     * 若路径已存在，则进行覆盖
     */

    public static void copyFromLocalFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        Path localPath = new Path(localFilePath);

        Path remotePath = new Path(remoteFilePath);

        /* fs.copyFromLocalFile 第一个参数表示是否删除源文件，第二个参数表示是否覆盖 */

        fs.copyFromLocalFile(false, true, localPath, remotePath);

        fs.close();

    }

    /**
     * 将文件1写入文件2
     */

    public static void appendFile1ToFile2(Configuration conf, String remoteFilePath, String remoteFilePath2) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(remoteFilePath);
        FSDataInputStream getIt = fs.open(file);
        BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
        String content1 = d.readLine();
        Path remotePath = new Path(remoteFilePath2);
        FSDataOutputStream out = fs.append(remotePath);
        out.write(content1.getBytes());
        d.close();
        out.close();
        fs.close();
    }

    /**
     * 追加文件内容
     */
    public static void appendToFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        Path remotePath = new Path(remoteFilePath);

        /* 创建一个文件读入流 */

        FileInputStream in = new FileInputStream(localFilePath);

        /* 创建一个文件输出流，输出的内容将追加到文件末尾 */

        FSDataOutputStream out = fs.append(remotePath);

        /* 读写文件内容 */

        byte[] data = new byte[1024];

        int read = -1;

        while ((read = in.read(data)) > 0) {

            out.write(data, 0, read);

        }

        out.close();

        in.close();

        fs.close();

    }

    /**
     * 下载文件到本地
     * <p>
     * 判断本地路径是否已存在，若已存在，则自动进行重命名
     */

    public static void copyToLocal(Configuration conf, String remoteFilePath, String localFilePath) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        Path remotePath = new Path(remoteFilePath);

        File f = new File(localFilePath);

        /* 如果文件名存在，自动重命名(在文件名后面加上 _0, _1 ...) */

        if (f.exists()) {

            System.out.println(localFilePath + " 文件已存在.");

            Integer i = 0;

            while (true) {

                f = new File(localFilePath + "_" + i.toString());

                if (!f.exists()) {

                    localFilePath = localFilePath + "_" + i.toString();

                    System.out.println("将重新命名为: " + localFilePath);

                    break;//重命名文件

                }

                i++;

            }

            // System.out.println("将重新命名为: " + localFilePath);

        } else

            System.out.println(localFilePath + " 文件不存在.");


        // 下载文件到本地

        Path localPath = new Path(localFilePath);

        fs.copyToLocalFile(remotePath, localPath);

        fs.close();

    }

    /**
     * 移动文件到本地
     * <p>
     * 移动后，删除源文件
     */
    public static void moveToLocalFile(Configuration conf, String remoteFilePath, String localFilePath) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        Path remotePath = new Path(remoteFilePath);

        Path localPath = new Path(localFilePath);

        fs.moveToLocalFile(remotePath, localPath);

    }

    /**
     * 读取文件内容
     */
    public static void readFile(Configuration conf, String data_path) throws Exception {
        String uri = data_path;
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        try (InputStream in = fs.open(new Path(uri))) {
            IOUtils.copy(in, System.out);
            ;
        }
    }


}