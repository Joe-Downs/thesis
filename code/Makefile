OPENZL_DIR := ../modules/openzl

CFLAGS += -Wall -I$(OPENZL_DIR)/include
LDLIBS += -lzstd -llz4 -llzma $(OPENZL_DIR)/libopenzl.a -lstdc++ -lpthread

compress: compress.c
	$(CC) $(CFLAGS) $< $(LDLIBS) -o $@

.PHONY: clean
clean:
	rm -f compress
