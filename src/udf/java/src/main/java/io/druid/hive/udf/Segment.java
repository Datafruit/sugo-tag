package io.druid.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

/**
 * Created by penghuan on 2017/10/19.
 */
public final class Segment extends UDF {
    public ArrayList<Map<String, String>> evaluate(final ArrayList<Double> arr) {
        ArrayList<Map<String, String>> ret = new ArrayList<>();
        if (null == arr || 0 == arr.size()) {
            return ret;
        }

        ArrayList<Double> arr_new = new ArrayList<>();
        Set<Double> set = new HashSet<>();
        for (Double v : arr) {
            if (!set.contains(v)) {
                arr_new.add(v);
                set.add(v);
            }
        }

        for (int i=0; i<arr_new.size(); i++) {
            if (0 == i) {
                Map<String, String> map = new HashMap<>();
                map.put("name", String.format("(0, %.1f]", arr_new.get(i)));
                map.put("value", String.format("0`%.1f", arr_new.get(i)));
                ret.add(map);
            }
            if (i > 0) {
                Map<String, String> map = new HashMap<>();
                map.put("name", String.format("(%.1f, %.1f]", arr_new.get(i-1), arr_new.get(i)));
                map.put("value", String.format("%.1f`%.1f", arr_new.get(i-1), arr_new.get(i)));
                ret.add(map);
            }
            if (arr_new.size() == (i+1)) {
                Map<String, String> map = new HashMap<>();
                map.put("name", String.format(">%.1f", arr_new.get(i)));
                map.put("value", String.format("%.1f`", arr_new.get(i)));
                ret.add(map);
            }
        }
        return ret;
    }
}
