package org.yamcs.web.rest.archive;

import org.yamcs.archive.AlarmRecorder;
import org.yamcs.protobuf.Alarms.AlarmData;
import org.yamcs.protobuf.Alarms.ParameterAlarmData;
import org.yamcs.protobuf.Rest.ListAlarmsResponse;
import org.yamcs.web.HttpException;
import org.yamcs.web.rest.RestHandler;
import org.yamcs.web.rest.RestRequest;
import org.yamcs.web.rest.RestRequest.IntervalResult;
import org.yamcs.web.rest.RestStreams;
import org.yamcs.web.rest.Route;
import org.yamcs.web.rest.SqlBuilder;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.XtceDb;
import org.yamcs.xtceproc.XtceDbFactory;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;

public class ArchiveAlarmRestHandler extends RestHandler {

    @Route(path = "/api/archive/:instance/alarms", method = "GET")
    public void listAlarms(RestRequest req) throws HttpException {
        String instance = verifyInstance(req, req.getRouteParam("instance"));

        long pos = req.getQueryParameterAsLong("pos", 0);
        int limit = req.getQueryParameterAsInt("limit", 100);

        SqlBuilder sqlbParam = new SqlBuilder(AlarmRecorder.PARAMETER_ALARM_TABLE_NAME);
        SqlBuilder sqlbEvent = new SqlBuilder(AlarmRecorder.EVENT_ALARM_TABLE_NAME);
        IntervalResult ir = req.scanForInterval();
        if (ir.hasInterval()) {
            sqlbParam.where(ir.asSqlCondition("triggerTime"));
            sqlbEvent.where(ir.asSqlCondition("triggerTime"));
        }

        /*
         * if (req.hasRouteParam("triggerTime")) { sqlb.where("triggerTime = " + req.getDateRouteParam("triggerTime"));
         * }
         */
        sqlbParam.descend(req.asksDescending(true));
        sqlbEvent.descend(req.asksDescending(true));
        sqlbParam.limit(pos, limit);
        sqlbEvent.limit(pos, limit);
        
      
        ListAlarmsResponse.Builder responseb = ListAlarmsResponse.newBuilder();
        String q = "MERGE ("+sqlbParam.toString()+"), ("+sqlbEvent.toString()+") USING triggerTime ORDER DESC";
        RestStreams.stream(instance, q, sqlbParam.getQueryArguments(), new StreamSubscriber() {

            @Override
            public void onTuple(Stream stream, Tuple tuple) {
                AlarmData alarm;
                if(tuple.hasColumn("parameter")) {
                    alarm = ArchiveHelper.parameterAlarmTupleToAlarmData(tuple);
                } else {
                    alarm = ArchiveHelper.eventAlarmTupleToAlarmData(tuple);
                }
                responseb.addAlarm(alarm);
            }

            @Override
            public void streamClosed(Stream stream) {
                completeOK(req, responseb.build());
            }
        });

    }
    
    
    @Route(path = "/api/archive/:instance/alarms/:parameter*", method = "GET")
    // @Route(path="/api/archive/:instance/alarms/:parameter*/:triggerTime?", method="GET") // same comment as below
    public void listParameterAlarms(RestRequest req) throws HttpException {
        String instance = verifyInstance(req, req.getRouteParam("instance"));

        long pos = req.getQueryParameterAsLong("pos", 0);
        int limit = req.getQueryParameterAsInt("limit", 100);

        SqlBuilder sqlb = new SqlBuilder(AlarmRecorder.PARAMETER_ALARM_TABLE_NAME);
        IntervalResult ir = req.scanForInterval();
        if (ir.hasInterval()) {
            sqlb.where(ir.asSqlCondition("triggerTime"));
        }

        if (req.hasRouteParam("parameter")) {
            XtceDb mdb = XtceDbFactory.getInstance(instance);
            Parameter p = verifyParameter(req, mdb, req.getRouteParam("parameter"));
            sqlb.where("parameter = ?", p.getQualifiedName());
        }
        /*
         * if (req.hasRouteParam("triggerTime")) { sqlb.where("triggerTime = " + req.getDateRouteParam("triggerTime"));
         * }
         */
        sqlb.descend(req.asksDescending(true));
        sqlb.limit(pos, limit);
        ListAlarmsResponse.Builder responseb = ListAlarmsResponse.newBuilder();
        RestStreams.stream(instance, sqlb.toString(), sqlb.getQueryArguments(), new StreamSubscriber() {

            @Override
            public void onTuple(Stream stream, Tuple tuple) {
                ParameterAlarmData alarm = ArchiveHelper.tupleToParameterAlarmData(tuple);
                responseb.addParameterAlarm(alarm);
            }

            @Override
            public void streamClosed(Stream stream) {
                completeOK(req, responseb.build());
            }
        });

    }

    /*-
     Commented out because in its current form the handling is ambiguous to the previous
     operation. Perhaps should use queryparams instead. and have parameter* always be terminal
    @Route(path="/api/archive/:instance/alarms/:parameter*   /:triggerTime/:seqnum", method="GET")
    public ChannelFuture getAlarm(RestRequest req) throws HttpException {
        String instance = verifyInstance(req, req.getRouteParam("instance"));
        
        XtceDb mdb = XtceDbFactory.getInstance(instance);
        Parameter p = verifyParameter(req, mdb, req.getRouteParam("parameter"));
        
        long triggerTime = req.getDateRouteParam("triggerTime");
        int seqNum = req.getIntegerRouteParam("seqnum");
        
        String sql = new SqlBuilder(AlarmRecorder.TABLE_NAME)
                .where("triggerTime = " + triggerTime)
                .where("seqNum = " + seqNum)
                .where("parameter = '" + p.getQualifiedName() + "'")
                .toString();
        
        List<AlarmData> alarms = new ArrayList<>();
        RestStreams.streamAndWait(instance, sql, new RestStreamSubscriber(0, 2) {
    
            @Override
            public void processTuple(Stream stream, Tuple tuple) {
                AlarmData alarm = ArchiveHelper.tupleToAlarmData(tuple);
                alarms.add(alarm);
            }
        });
        
        if (alarms.isEmpty()) {
            throw new NotFoundException(req, "No alarm for id (" + p.getQualifiedName() + ", " + triggerTime + ", " + seqNum + ")");
        } else if (alarms.size() > 1) {
            throw new InternalServerErrorException("Too many results");
        } else {
            return sendOK(req, alarms.get(0), SchemaAlarms.AlarmData.WRITE);
        }
    }*/
}
