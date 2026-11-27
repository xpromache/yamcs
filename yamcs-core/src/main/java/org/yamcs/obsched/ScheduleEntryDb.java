package org.yamcs.obsched;

import static org.yamcs.yarch.query.Query.createStream;
import static org.yamcs.yarch.query.Query.createTable;
import static org.yamcs.yarch.query.Query.selectStream;
import static org.yamcs.yarch.query.Query.upsertIntoTable;

import java.util.ArrayList;
import java.util.List;

import org.yamcs.InitException;
import org.yamcs.logging.Log;
import org.yamcs.protobuf.obsched.ScheduleEntryInfo;
import org.yamcs.utils.parser.ParseException;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.streamsql.StreamSqlException;

/**
 * Persists the schedule entries of all the on-board schedulers of an instance.
 * <p>
 * Entries are never removed from the table: deleted entries are kept with intent DELETED for the record.
 */
public class ScheduleEntryDb {

    public static final String TABLE_NAME = "onboard_schedule_entry";
    static final String CNAME_ID = "id";
    static final String CNAME_SCHEDULER = "scheduler";
    static final String CNAME_RELEASE_TIME = "release_time";
    static final String CNAME_ENTRY = "entry";

    private static final TupleDefinition TDEF = new TupleDefinition();
    static {
        TDEF.addColumn(CNAME_ID, DataType.UUID);
        TDEF.addColumn(CNAME_SCHEDULER, DataType.STRING);
        TDEF.addColumn(CNAME_RELEASE_TIME, DataType.TIMESTAMP);
        TDEF.addColumn(CNAME_ENTRY, DataType.protobuf(ScheduleEntryInfo.class));
    }

    private final Log log;
    private final YarchDatabaseInstance ydb;
    private final Stream tableStream;

    public ScheduleEntryDb(String yamcsInstance) throws InitException {
        log = new Log(ScheduleEntryDb.class, yamcsInstance);
        ydb = YarchDatabase.getInstance(yamcsInstance);
        try {
            var streamName = TABLE_NAME + "_in";
            synchronized (ScheduleEntryDb.class) {
                if (ydb.getTable(TABLE_NAME) == null) {
                    var q = createTable(TABLE_NAME, TDEF).primaryKey(CNAME_ID);
                    ydb.execute(q.toStatement());
                }
                if (ydb.getStream(streamName) == null) {
                    ydb.execute(createStream(streamName, TDEF).toStatement());
                    ydb.execute(upsertIntoTable(TABLE_NAME).query(selectStream(streamName).toSQL()).toStatement());
                }
            }
            tableStream = ydb.getStream(streamName);
        } catch (StreamSqlException | ParseException e) {
            throw new InitException(e);
        }
    }

    /**
     * Loads all the entries of a scheduler, including the deleted ones.
     */
    public List<ScheduleEntry> load(String scheduler) {
        var result = new ArrayList<ScheduleEntry>();
        var r = ydb.executeUnchecked("select * from " + TABLE_NAME + " where " + CNAME_SCHEDULER + " = ?",
                scheduler);
        try {
            while (r.hasNext()) {
                Tuple t = r.next();
                try {
                    ScheduleEntryInfo info = t.getColumn(CNAME_ENTRY);
                    result.add(new ScheduleEntry(info));
                } catch (Exception e) {
                    log.error("Cannot decode schedule entry from tuple {}", t, e);
                }
            }
        } finally {
            r.close();
        }
        return result;
    }

    public void save(ScheduleEntry entry) {
        var t = new Tuple(TDEF, new Object[] {
                entry.getId(),
                entry.getScheduler(),
                entry.getReleaseTime(),
                entry.toProto(true) });
        tableStream.emitTuple(t);
    }
}
