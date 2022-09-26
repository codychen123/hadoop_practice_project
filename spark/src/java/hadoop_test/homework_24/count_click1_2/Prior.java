package hadoop_test.homework_24.count_click1_2;

public class Prior {
    private String id;
    private int ts;

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }

    public int getTs() {
        return ts;
    }

    public void setTs(int ts) {
        this.ts = ts;
    }

    public Prior(int ts, String id) {
        super();
        this.id = id;
        this.ts = ts;
    }
    public Prior() {
        super();
    }

}
