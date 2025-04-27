# time mpirun -mca btl_tcp_if_include ibp216s0f0 -np 8 --hostfile <host_file> dht_count <input_file> <query_file>

# Define the compiler and flags
CC = mpicc
CFLAGS = -libverbs
TARGET = dht_count

# Source files
SRCS = dht_count.c

# Specify the executable name
OBJ = $(SRCS:.c=.o)

# Default target
all: $(TARGET)

# Build the target executable
$(TARGET): $(OBJ)
	$(CC) -o $(TARGET) $(OBJ) $(CFLAGS)

# Compile the source files
%.o: %.c
	$(CC) -c $< $(CFLAGS)

# Clean up build files
clean:
	rm -f $(OBJ) $(TARGET)

# Run the program with arguments
run: $(TARGET)
	mpirun -np 8 ./$(TARGET) test1.txt

.PHONY: all clean run

