package domain;

/**
 * Created by Administrator on 2019/1/8.
 */
public class FlowStat {

    private String day;
    private String uvCount;

    public FlowStat() {
    }

    public FlowStat(String day, String uvCount) {
        this.day = day;
        this.uvCount = uvCount;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getUvCount() {
        return uvCount;
    }

    public void setUvCount(String uvCount) {
        this.uvCount = uvCount;
    }
}
