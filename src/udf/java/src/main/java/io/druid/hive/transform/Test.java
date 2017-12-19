package io.druid.hive.transform;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Scanner;

/**
 * Created by penghuan on 2017/10/10.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new RuntimeException("need args: [master] [data source]");
        }
        Scanner input = new Scanner(System.in);
        while (input.hasNext()) {
            String line = input.nextLine();
            Iterable<String> fieldsIter = Splitter.on("\t").trimResults(CharMatcher.is('\n')).split(line);
            List<Object> fields = Lists.newArrayList(fieldsIter);
            System.out.println(fields);
        }
    }

}
