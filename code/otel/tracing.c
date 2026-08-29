#include "tracing.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

static void gen_hex(char *out, size_t n_bytes) {
    FILE *rng = fopen("/dev/urandom", "rb");
    for (size_t i = 0; i < n_bytes; i++) {
        unsigned char b = 0;
        fread(&b, 1, 1, rng);
        sprintf(out + i * 2, "%02x", b);
    }
    out[n_bytes * 2] = '\0';
    fclose(rng);
}

static void ts_to_iso8601(struct timespec ts, char *buf, size_t cap) {
    struct tm t;
    gmtime_r(&ts.tv_sec, &t);
    char base[24];
    strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &t);
    snprintf(buf, cap, "%s.%09ldZ", base, ts.tv_nsec);
}

TraceWriter trace_open(const char *experiment) {
    mkdir("traces", 0755);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    struct tm t;
    gmtime_r(&now.tv_sec, &t);
    char stamp[20];
    strftime(stamp, sizeof(stamp), "%Y%m%dT%H%M%SZ", &t);

    TraceWriter tw;
    strncpy(tw.experiment, experiment, sizeof(tw.experiment) - 1);
    tw.experiment[sizeof(tw.experiment) - 1] = '\0';
    gen_hex(tw.trace_id, 16);

    char path[256];
    snprintf(path, sizeof(path), "traces/%s_%s.jsonl", experiment, stamp);
    tw.fp = fopen(path, "w");
    fprintf(stderr, "tracing -> %s\n", path);
    return tw;
}

void trace_span(TraceWriter *tw, const char *span_name,
                const char *algorithm,
                size_t input_size_b, size_t output_size_b,
                struct timespec t0, struct timespec t1) {
    char span_id[17];
    gen_hex(span_id, 8);

    char start_s[40], end_s[40];
    ts_to_iso8601(t0, start_s, sizeof(start_s));
    ts_to_iso8601(t1, end_s,   sizeof(end_s));

    long duration_ns = (long)(t1.tv_sec - t0.tv_sec) * 1000000000L
                     + (t1.tv_nsec - t0.tv_nsec);

    fprintf(tw->fp,
        "{\"name\":\"%s\","
        "\"context\":{\"trace_id\":\"0x%s\",\"span_id\":\"0x%s\",\"trace_state\":\"[]\"},"
        "\"kind\":\"SpanKind.INTERNAL\","
        "\"parent_id\":null,"
        "\"start_time\":\"%s\","
        "\"end_time\":\"%s\","
        "\"status\":{\"status_code\":\"OK\"},"
        "\"attributes\":{"
            "\"algorithm\":\"%s\","
            "\"input_size_b\":%zu,"
            "\"output_size_b\":%zu,"
            "\"duration_ns\":%ld,"
            "\"experiment\":\"%s\""
        "},"
        "\"events\":[],"
        "\"links\":[],"
        "\"resource\":{\"attributes\":{\"service.name\":\"%s\"},\"schema_url\":\"\"}}"
        "\n",
        span_name,
        tw->trace_id, span_id,
        start_s, end_s,
        algorithm, input_size_b, output_size_b, duration_ns,
        tw->experiment,
        tw->experiment);
}

void trace_close(TraceWriter *tw) {
    if (tw->fp) {
        fclose(tw->fp);
        tw->fp = NULL;
    }
}
