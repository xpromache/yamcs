package org.yamcs.http.api;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.yamcs.YamcsServer;
import org.yamcs.api.Observer;
import org.yamcs.http.BadRequestException;
import org.yamcs.http.Context;
import org.yamcs.http.InternalServerErrorException;
import org.yamcs.http.NotFoundException;
import org.yamcs.obsched.InvalidEntryException;
import org.yamcs.obsched.OnboardSchedulerService;
import org.yamcs.obsched.OnboardSchedulerService.SyncMode;
import org.yamcs.obsched.ScheduleEntry;
import org.yamcs.protobuf.obsched.AbstractOnboardSchedulerApi;
import org.yamcs.protobuf.obsched.CreateEntryRequest;
import org.yamcs.protobuf.obsched.DeleteEntryRequest;
import org.yamcs.protobuf.obsched.GetEntryRequest;
import org.yamcs.protobuf.obsched.GetSchedulerRequest;
import org.yamcs.protobuf.obsched.ListEntriesRequest;
import org.yamcs.protobuf.obsched.ListEntriesResponse;
import org.yamcs.protobuf.obsched.ListSchedulersRequest;
import org.yamcs.protobuf.obsched.ListSchedulersResponse;
import org.yamcs.protobuf.obsched.GetPendingChangesRequest;
import org.yamcs.protobuf.obsched.PendingChangesInfo;
import org.yamcs.protobuf.obsched.ReadBackRequest;
import org.yamcs.protobuf.obsched.ReplaceEntryRequest;
import org.yamcs.protobuf.obsched.ScheduleEntryInfo;
import org.yamcs.protobuf.obsched.SchedulerInfo;
import org.yamcs.protobuf.obsched.SubscribeEntriesRequest;
import org.yamcs.protobuf.obsched.SyncInfo;
import org.yamcs.protobuf.obsched.SyncRequest;
import org.yamcs.protobuf.obsched.UpdateEntryRequest;
import org.yamcs.security.SystemPrivilege;
import org.yamcs.utils.TimeEncoding;

public class OnboardSchedulerApi extends AbstractOnboardSchedulerApi<Context> {

    @Override
    public void listSchedulers(Context ctx, ListSchedulersRequest request, Observer<ListSchedulersResponse> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ReadOnboardSchedule);
        var instance = InstancesApi.verifyInstance(request.getInstance());
        var b = ListSchedulersResponse.newBuilder();
        for (var service : getServices(instance)) {
            b.addSchedulers(service.getSchedulerInfo());
        }
        observer.complete(b.build());
    }

    @Override
    public void getScheduler(Context ctx, GetSchedulerRequest request, Observer<SchedulerInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ReadOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        observer.complete(service.getSchedulerInfo());
    }

    @Override
    public void listEntries(Context ctx, ListEntriesRequest request, Observer<ListEntriesResponse> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ReadOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        long start = request.hasStart() ? TimeEncoding.fromProtobufTimestamp(request.getStart()) : Long.MIN_VALUE;
        long stop = request.hasStop() ? TimeEncoding.fromProtobufTimestamp(request.getStop()) : Long.MAX_VALUE;
        var b = ListEntriesResponse.newBuilder();
        for (var e : service.getEntries(start, stop, request.getIncludeDeleted())) {
            b.addEntries(e.toProto(request.getDetails()));
        }
        observer.complete(b.build());
    }

    @Override
    public void getEntry(Context ctx, GetEntryRequest request, Observer<ScheduleEntryInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ReadOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        observer.complete(verifyEntry(service, request.getId()).toProto(true));
    }

    @Override
    public void createEntry(Context ctx, CreateEntryRequest request, Observer<ScheduleEntryInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ControlOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        if (!request.hasCommand()) {
            throw new BadRequestException("command is required");
        }
        if (!request.hasReleaseTime()) {
            throw new BadRequestException("releaseTime is required");
        }
        try {
            var e = service.createEntry(ctx.user, request.getCommand(),
                    request.hasArgs() ? request.getArgs() : null,
                    TimeEncoding.fromProtobufTimestamp(request.getReleaseTime()),
                    request.hasOptions() ? request.getOptions() : null,
                    request.hasDescription() ? request.getDescription() : null,
                    request.getTagsList());
            observer.complete(e.toProto(true));
        } catch (InvalidEntryException e) {
            throw new BadRequestException(e.getMessage());
        }
    }

    @Override
    public void updateEntry(Context ctx, UpdateEntryRequest request, Observer<ScheduleEntryInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ControlOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        var id = verifyEntry(service, request.getId()).getId();
        try {
            var e = service.updateEntry(ctx.user, id,
                    request.hasCommand() ? request.getCommand() : null,
                    request.hasArgs() ? request.getArgs() : null,
                    request.hasReleaseTime() ? TimeEncoding.fromProtobufTimestamp(request.getReleaseTime()) : null,
                    request.hasOptions() ? request.getOptions() : null,
                    request.hasDescription() ? request.getDescription() : null,
                    request.hasTags() ? request.getTags().getTagsList() : null);
            observer.complete(e.toProto(true));
        } catch (InvalidEntryException e) {
            throw new BadRequestException(e.getMessage());
        }
    }

    @Override
    public void replaceEntry(Context ctx, ReplaceEntryRequest request, Observer<ScheduleEntryInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ControlOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        var id = verifyEntry(service, request.getId()).getId();
        if (!request.hasCommand()) {
            throw new BadRequestException("command is required");
        }
        if (!request.hasReleaseTime()) {
            throw new BadRequestException("releaseTime is required");
        }
        try {
            var e = service.replaceEntry(ctx.user, id, request.getCommand(),
                    request.hasArgs() ? request.getArgs() : null,
                    TimeEncoding.fromProtobufTimestamp(request.getReleaseTime()),
                    request.hasOptions() ? request.getOptions() : null,
                    request.hasDescription() ? request.getDescription() : null,
                    request.getTagsList());
            observer.complete(e.toProto(true));
        } catch (InvalidEntryException e) {
            throw new BadRequestException(e.getMessage());
        }
    }

    @Override
    public void deleteEntry(Context ctx, DeleteEntryRequest request, Observer<ScheduleEntryInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ControlOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        var id = verifyEntry(service, request.getId()).getId();
        try {
            observer.complete(service.deleteEntry(ctx.user, id).toProto(true));
        } catch (InvalidEntryException e) {
            throw new BadRequestException(e.getMessage());
        }
    }

    @Override
    public void getPendingChanges(Context ctx, GetPendingChangesRequest request,
            Observer<PendingChangesInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ReadOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        observer.complete(service.getPendingChanges());
    }

    @Override
    public void readBack(Context ctx, ReadBackRequest request, Observer<SyncInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ControlOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        runSync(ctx, service, SyncMode.READ_BACK, observer);
    }

    @Override
    public void sync(Context ctx, SyncRequest request, Observer<SyncInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ControlOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        runSync(ctx, service, SyncMode.SYNC, observer);
    }

    private void runSync(Context ctx, OnboardSchedulerService service, SyncMode mode, Observer<SyncInfo> observer) {
        service.sync(ctx.user, mode).whenComplete((info, err) -> {
            if (err != null) {
                var cause = err.getCause() != null ? err.getCause() : err;
                if (cause instanceof IllegalStateException) {
                    observer.completeExceptionally(new BadRequestException(cause.getMessage()));
                } else {
                    observer.completeExceptionally(new InternalServerErrorException(cause));
                }
            } else {
                observer.complete(info);
            }
        });
    }

    @Override
    public void subscribeEntries(Context ctx, SubscribeEntriesRequest request, Observer<ScheduleEntryInfo> observer) {
        ctx.checkSystemPrivilege(SystemPrivilege.ReadOnboardSchedule);
        var service = verifyService(request.getInstance(), request.getScheduler());
        Consumer<ScheduleEntry> listener = e -> observer.next(e.toProto(false));
        observer.setCancelHandler(() -> service.removeEntryListener(listener));
        service.addEntryListener(listener);
    }

    private static List<OnboardSchedulerService> getServices(String instance) {
        var ysi = YamcsServer.getServer().getInstance(instance);
        return ysi == null ? new ArrayList<>() : ysi.getServices(OnboardSchedulerService.class);
    }

    static OnboardSchedulerService verifyService(String instance, String scheduler) {
        instance = InstancesApi.verifyInstance(instance);
        for (var service : getServices(instance)) {
            if (service.getSchedulerName().equals(scheduler)) {
                return service;
            }
        }
        throw new NotFoundException("No on-board scheduler named '" + scheduler + "'");
    }

    private static ScheduleEntry verifyEntry(OnboardSchedulerService service, String id) {
        UUID uuid;
        try {
            uuid = UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("Invalid entry id '" + id + "'");
        }
        var e = service.getEntry(uuid);
        if (e == null) {
            throw new NotFoundException("No entry " + id);
        }
        return e;
    }
}
