EXEC = bin

default: $(EXEC)

$(EXEC): main.c
	mpicc $^ -o $@
	
run:
	mpirun -n 251 bin
	
clean:
	@rm -f *.o
