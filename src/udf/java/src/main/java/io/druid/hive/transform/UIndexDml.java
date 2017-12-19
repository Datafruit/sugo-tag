package io.druid.hive.transform;

import io.druid.hyper.client.imports.DataSender;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by penghuan on 2017/10/10.
 */
public class UIndexDml {

    private final String hMaster;
    private final String dataSource;
    private final DataSender sender;

    public UIndexDml(String hMaster, String dataSource) {
        this.hMaster = hMaster;
        this.dataSource = dataSource;
        this.sender = getSender();
    }

    private DataSender getSender() {
        return DataSender.builder().toServer(this.hMaster).ofDataSource(this.dataSource).build();
    }

    public void add(List<Object> values) throws Exception {
        this.sender.add(values);
    }

    public void update(Map<String, Object> values) throws Exception {
        this.sender.update(values);
    }

    public void close() throws IOException {
        this.sender.close();
    }

}
