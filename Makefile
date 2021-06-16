SHELL=/bin/bash

EXE = go-search

all: $(EXE)

go-search:
	@echo "building $@ ..."
	$(MAKE) -s -f make.inc 

clean:
	rm -f $(EXE)

