# Global Distributed Hash Table for Word Counting Project

This README provides an overview of the project, including system prerequisites and instructions on how to build, run, and use the `dht_count` program for distributed word counting using MPI.

---

## Project Overview

The **Global Distributed Hash Table for Word Counting** project leverages MPI (Message Passing Interface) for parallel word counting across multiple nodes. The program divides a text file into portions, processes each portion in parallel, and counts the frequency of words, then aggregates the results into a distributed hash table.

---

## Prerequisites

### 1. **Programming Language**
- Written in **C**.

### 2. **Compiler**
- Requires an **MPI-compatible C compiler**:
  - Use `mpicc` (MPI C Compiler).
  - Install OpenMPI or MVAPICH2:
    - For Ubuntu:
      ```bash
      sudo apt install openmpi-bin openmpi-common libopenmpi-dev
      ```
    - For CentOS/RHEL:
      ```bash
      sudo yum install openmpi
      ```

### 3. **MPI (Message Passing Interface)**
- **MPI** is required to run the program across multiple nodes.
  - Use `mpirun` for parallel execution.
  - You must configure the **MPI environment** and create a **hostfile** specifying the compute nodes for the distributed execution.

### 4. **RDMA (Remote Direct Memory Access)**
- **libverbs** is used for RDMA functionality for high-performance communication.
  - Install the RDMA libraries:
    - For Ubuntu:
      ```bash
      sudo apt install librdmacm-dev libibverbs-dev
      ```

### 5. **Hostfile**
- The **hostfile** lists the compute nodes that will run the program.
  - Example of a hostfile (`hostfile`):
    ```
    node1 slots=4
    node2 slots=4
    ```

---

## Build Instructions

1. **Clone or download** the project files.
2. Ensure the necessary **MPI** and **RDMA** libraries are installed.
3. **Navigate to the project directory** containing the `Makefile` and `dht_count.c`.
4. **Build the program**:
   ```bash
   make
