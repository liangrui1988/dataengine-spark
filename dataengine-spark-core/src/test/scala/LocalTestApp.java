import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;

public class LocalTestApp {
    public static void main(String[] args) throws IOException {
        BinaryLogClient client = new BinaryLogClient("127.0.0.1", 3306, "test", "root", "");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        //client.setServerId(1767354141);
//        client.setBinlogFilename("mysql-bin-changelog.140068");
//        client.setBinlogPosition(8993);

        client.setConnectTimeout(60000);
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                final EventData data = event.getData();
//                System.out.println("EventData:"+data);
                if (data instanceof WriteRowsEventData) {
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData) data;
                    System.out.println("writer:");
                    System.out.println(writeRowsEventData);
                } else if (data instanceof UpdateRowsEventData) {
                    System.out.println("update:");
                    UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) data;
                    System.out.println(updateRowsEventData);
                } else if (data instanceof DeleteRowsEventData) {
                    System.out.println("delete:");
                    DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) data;
                    System.out.println(deleteRowsEventData);
                } else if (data instanceof RowsQueryEventData) {
                    System.out.println("query:");
                    RowsQueryEventData rowsQueryEventData = (RowsQueryEventData) data;
                    System.out.println(rowsQueryEventData);
                } else if (data instanceof QueryEventData) {
                    System.out.println("QueryEventData:");
                    QueryEventData queryEventData = (QueryEventData) data;
                    System.out.println(queryEventData);
                }


            }
        });
        client.connect();
    }


}
