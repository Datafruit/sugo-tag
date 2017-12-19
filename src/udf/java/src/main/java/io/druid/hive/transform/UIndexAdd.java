package io.druid.hive.transform;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * Created by penghuan on 2017/10/10.
 */
public class UIndexAdd {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new RuntimeException("need args: [master] [data source]");
        }
        String hMaster = args[0];
        String dataSource = args[1];
        List<String> fieldsName = new ArrayList<>();
        if (args.length >= 3) {
            Iterable<String> fieldsNameIter = Splitter.on("`").trimResults().split(args[2]);
            fieldsName = Lists.newArrayList(fieldsNameIter);
        }
        UIndexDml dml = new UIndexDml(hMaster, dataSource);
        try {
            Scanner input = new Scanner(System.in);
            while (input.hasNext()) {
                String line = input.nextLine();
                Iterable<String> fieldsIter = Splitter.on("\t").trimResults(CharMatcher.is('\n')).split(line);
                List<Object> fields = Lists.newArrayList(fieldsIter);
                List<Object> fields_new_list = new ArrayList<>();
                Map<String, Object> fields_new_map = new HashMap<>();
                for (int i=0; i<fields.size(); i++) {
                    Object fd = fields.get(i);
                    Object fd_new = null;
                    if (!"\\N".equals(String.valueOf(fd))) {
//                        fd_new = String.valueOf(fd).replaceAll(",", ";");
                        fd_new = fd;
                    }
                    if (fieldsName.size() > 0) {
                        String fd_name = fieldsName.get(i);
                        fields_new_map.put(fd_name, fd_new);
                    } else {
                        fields_new_list.add(fd_new);
                    }
                }
                if (fieldsName.size() > 0) {
                    dml.update(fields_new_map);
                } else {
                    dml.add(fields_new_list);
                }
                System.out.println("0");
            }
        } finally {
            dml.close();
        }
    }
}
