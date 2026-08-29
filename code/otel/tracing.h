#ifndef OTEL_TRACING_H
#define OTEL_TRACING_H

#include <stddef.h>
#include <stdio.h>
#include <time.h>

typedef struct {
    FILE *fp;
    char  trace_id[33];  /* 128-bit id as 32 hex chars + NUL */
    char  experiment[64];
} TraceWriter;

/* Open a new JSONL trace file under traces/<experiment>_<timestamp>.jsonl. */
TraceWriter trace_open(const char *experiment);

/* Append one span to the trace file. */
void trace_span(TraceWriter *tw, const char *span_name,
                const char *algorithm,
                size_t input_size_b, size_t output_size_b,
                struct timespec t0, struct timespec t1);

/* Flush and close the trace file. */
void trace_close(TraceWriter *tw);

#endif /* OTEL_TRACING_H */
