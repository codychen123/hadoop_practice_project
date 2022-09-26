package hadoop_test.Filter;


public class main {
    public static void main(String[] args) {

        BloomFilter b = new BloomFilter();
        b.addValue("www.baidu.com");
        b.addValue("www.sohu.com");

        System.out.println(b.contains("www.baidu.com"));
        System.out.println(b.contains("www.sina.com"));
        System.out.println(b.contains("www.souh.com"));
    }
}
