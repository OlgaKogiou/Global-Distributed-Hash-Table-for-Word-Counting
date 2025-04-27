Project Overview
The Global Distributed Hash Table for Word Counting project leverages MPI (Message Passing Interface) for parallel word counting across multiple nodes. The program divides a text file into portions, processes each portion in parallel, and counts the frequency of words, then aggregates the results into a distributed hash table.

Prerequisites
1. Programming Language
Written in C.

2. Compiler
Requires an MPI-compatible C compiler:

Use mpicc (MPI C Compiler).

Install OpenMPI or MVAPICH2:

For Ubuntu:

bash
Copy
Edit
sudo apt install openmpi-bin openmpi-common libopenmpi-dev
For CentOS/RHEL:

bash
Copy
Edit
sudo yum install openmpi
3. MPI (Message Passing Interface)
MPI is required to run the program across multiple nodes.

Use mpirun for parallel execution.

You must configure the MPI environment and create a hostfile specifying the compute nodes for the distributed execution.

4. RDMA (Remote Direct Memory Access)
libverbs is used for RDMA functionality for high-performance communication.

Install the RDMA libraries:

For Ubuntu:

bash
Copy
Edit
sudo apt install librdmacm-dev libibverbs-dev
5. Hostfile
The hostfile lists the compute nodes that will run the program.

Example of a hostfile (hostfile):

nginx
Copy
Edit
node1 slots=4
node2 slots=4
Build Instructions
Clone or download the project files.

Ensure the necessary MPI and RDMA libraries are installed.

Navigate to the project directory containing the Makefile and dht_count.c.

Build the program:

bash
Copy
Edit
make
This will compile the source file dht_count.c into an executable named dht_count.

Clean up build files:

bash
Copy
Edit
make clean
This will remove the object files and executable.

Run Instructions
1. Prepare the Hostfile
The hostfile should list the available compute nodes. Example content for a hostfile (hostfile):

nginx
Copy
Edit
node1 slots=4
node2 slots=4
2. Run the Program
Use the following command to run the program:

bash
Copy
Edit
mpirun -mca btl_tcp_if_include ibp216s0f0 -np 8 --hostfile <hostfile> ./dht_count <input_file> <query_file>
-mca btl_tcp_if_include ibp216s0f0: Specifies the network interface for communication.

-np 8: Runs the program with 8 processes.

--hostfile <hostfile>: Path to the hostfile.

./dht_count <input_file> <query_file>: Run the executable with input and query files.

3. Example Run
bash
Copy
Edit
mpirun -mca btl_tcp_if_include ibp216s0f0 -np 8 --hostfile hostfile ./dht_count test1.txt query.txt
test1.txt: Input file for word counting.

query.txt: Query file (can be used for additional processing).

Clean Up
After execution, remove the compiled files by running:

bash
Copy
Edit
make clean
This will delete the object files and the executable.

