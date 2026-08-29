OPENZL_DIR := ../modules/openzl

CFLAGS  += -Wall -I. -I$(OPENZL_DIR)/include
LDLIBS  += -lzstd -llz4 -llzma $(OPENZL_DIR)/libopenzl.a -lstdc++ -lpthread -lm

OBJS := compress.o decompress.o otel/tracing.o

roundtrip: roundtrip.c $(OBJS) $(OPENZL_DIR)/libopenzl.a
	$(CC) $(CFLAGS) $< $(OBJS) $(LDLIBS) -o $@

compress.o: compress.c compress.h
	$(CC) $(CFLAGS) -c $< -o $@

decompress.o: decompress.c decompress.h compress.h
	$(CC) $(CFLAGS) -c $< -o $@

otel/tracing.o: otel/tracing.c otel/tracing.h
	$(CC) $(CFLAGS) -c $< -o $@

$(OPENZL_DIR)/libopenzl.a:
	$(MAKE) -C $(OPENZL_DIR) libopenzl.a

.PHONY: clean
clean:
	rm -f roundtrip $(OBJS)
