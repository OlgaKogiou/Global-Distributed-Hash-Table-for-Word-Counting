#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <mpi.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <assert.h>
#include <immintrin.h> 

#define NUM_BUCKETS 256        		// Number of buckets per hash table
#define BUCKET_SIZE (4 * 1024 * 1024) 	// 4MB per bucket
#define RECORD_SIZE 128        		// Each record is 128 bytes
#define MAX_RECORDS 32767 		// Maximum number of records per bucket
#define HASH_TABLE_SIZE (1 * 1024 * 1024 * 1024) // 1GB

int rank, size;
int *portion_sizes = NULL;
int *offsets = NULL;
int my_portion_size;
int my_offset;
char *my_buffer = NULL;
const char *output_file = "results.txt";
int outstanding_reads;
void* memory_block;

static int page_size;

struct conn_info {
	int lid;
	int qpn;
	int psn;
	uint32_t rkey;
	uint64_t addr;
};

struct conn_context {
	struct ibv_context *context;
	struct ibv_comp_channel *channel;
	struct ibv_pd *pd;
	struct ibv_mr *mr;
	struct ibv_dm *dm;
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	struct ibv_qp_ex *qpx;
	char *buf;
	int size;
	int send_flags;
	int rx_depth;
	int pending;
	struct ibv_port_attr portinfo;
};

typedef struct {
    char word[64]; // Word: max 62 characters + null terminator
    uint64_t frequency; // Freq: 8 bytes to track occurrences
    uint64_t locations[7]; // Locs: 7 locations (8 bytes each) = 56 bytes
} Record;

typedef struct {
    char header[128];         // 128-byte header (can be defined to store metadata)
    Record records[32767];    // Space for 32,767 records per bucket
    int record_count;
} Bucket;

Bucket **hash_table;

typedef struct {
    char word[64];    // Word: max 62 characters + null terminator
    int bucket_id;    // Which bucket it goes to
    uint64_t location; // Byte offset in the file where the word has been seen
} Packet;

typedef struct {
    int *portion_sizes;
    int *offsets;
} SizesAndOffsets;

struct ibv_mr* all_mrs;
uint16_t *all_qp_numbers;
struct ibv_cq *cq;
struct ibv_qp *qp;

struct conn_info *all_info;
void update_hash_table(Packet packet);

uint16_t getLocalId(struct ibv_context* context, int ib_port) {
  struct ibv_port_attr port_attr;
  ibv_query_port(context, ib_port, &port_attr);
  return port_attr.lid;
}


uint32_t getQueuePairNumber(struct ibv_qp* qp) {
  return qp->qp_num;
}

unsigned int compute_hash(const char *word) {
	unsigned int hash = 0;
	for (int i = 0; word[i] != '\0'; i++) {
		unsigned int multiplier = (i % 2 == 0) ? 121 : 1331;
		hash = (hash + (toupper(word[i]) * multiplier)) % 2048;
	}
	return hash;
}

int compare(const void *a, const void *b) {
	uint64_t loc_a = *(uint64_t *)a;
	uint64_t loc_b = *(uint64_t *)b;
	return (loc_a > loc_b) - (loc_a < loc_b);
}

void find_top_frequency_record(const char *output_file) {
	// Variables to track the top record
	Record *top_record = NULL;
	int top_bucket_index = -1;

	for (int i = 0; i < NUM_BUCKETS; i++) {
		Bucket *bucket = hash_table[i];
		if (!bucket) continue; // Skip empty buckets
		
		for (int j = 0; j < bucket->record_count; j++) {
			Record *current_record = &bucket->records[j];

			if (!top_record || current_record->frequency > top_record->frequency) {
				top_record = current_record;
				top_bucket_index = i;
			}
		}
	}

	// If no records were found, return early
	if (!top_record) {
		printf("Rank %d has no records in its hash table.\n", rank);
		return;
	}

	if (top_record != NULL) {
		// Write the top record information to the file
		FILE *file = fopen(output_file, "a");
		if (!file) {
			fprintf(stderr, "Failed to open file %s for writing.\n", output_file);
			return;
		}
		qsort(top_record->locations, 7, sizeof(uint64_t), compare);
		// Write the formatted top record information
		fprintf(file, "Rank %d: %s - Freq: %lu; Loc (<= 7): ", rank, top_record->word, top_record->frequency);

		for (int k = 0; k < 7; k++) {
			if (top_record->locations[k] != '\0') {
				fprintf(file, "%lu ", top_record->locations[k]);
			}
		}
		fprintf(file, "\n");
		fclose(file);
	}
}

char** query(const char *filename, int *word_count) {
	FILE *ofile = fopen(output_file, "a");
         if (!ofile) {
         	fprintf(stderr, "Failed to open file %s for writing.\n", output_file);
                // return;
        }
	fprintf(ofile, " \n");
	fprintf(ofile, "====== ====== ====== ====== \n");
	fprintf(ofile, "   Starting the query ... \n");
	fprintf(ofile, "====== ====== ====== ====== \n");
	fclose(ofile);
	
	FILE *file = fopen(filename, "r");
	if (file == NULL) {
		fprintf(stderr, "Error: Could not open file %s\n", filename);
		exit(1);
	}

	// Allocate initial memory for storing words
	int capacity = 10;  // initial capacity
	*word_count = 0;     // counter to keep track of the number of words
	char **words = malloc(capacity * sizeof(char*));
	if (words == NULL) {
		fprintf(stderr, "Error: Memory allocation failed\n");
		exit(1);
	}
	char ch;
	char word[64]; // Temporary buffer to hold the current word
	int index = 0;
	int is_word_in_list(char **words, int word_count, const char *word_to_check) {
		for (int i = 0; i < word_count; i++) {
			if (strcmp(words[i], word_to_check) == 0) {
				return 1;
			}
		}
		return 0;
	}
	while ((ch = fgetc(file)) != EOF) {
		if (isalnum(ch)) {
			if (index < 63) {
				word[index++] = tolower(ch);
			}
		}
		else {
			if (index > 0) {
				word[index] = '\0';
				
				for (int i = 0; word[i]; i++) {
					word[i] = toupper(word[i]);
				}

				if (!is_word_in_list(words, *word_count, word)) {
					words[*word_count] = strdup(word);
					(*word_count)++;
					if (*word_count >= capacity) {
						capacity *= 2;
						words = realloc(words, capacity * sizeof(char*));
						if (words == NULL) {
							fprintf(stderr, "Error: Memory reallocation failed\n");
							exit(1);
						}
					}
				}
				index = 0;
			}
		}
	}
	if (index > 0) {
		word[index] = '\0';
		for (int i = 0; words[*word_count][i]; i++) {
			words[*word_count][i] = toupper(words[*word_count][i]);
		}
		if (!is_word_in_list(words, *word_count, word)) {
			words[*word_count] = strdup(word);
			(*word_count)++;
		}
	}
	fclose(file);
	return words;	
}

static int poll_rdma_completion(struct conn_context *ctx, int rw)
{
	struct ibv_wc wc;
	struct ibv_recv_wr *bad_wr;
	int ret;
	int flushed = 0;

	while ((ret = ibv_poll_cq(ctx->cq, 1, &wc)) == 1) {
		ret = 0;
		if (wc.status) {
			if (wc.status == IBV_WC_WR_FLUSH_ERR) {
				flushed = 1;
				continue;
			}
			fprintf(stderr, "cq completion failed status %d\n", wc.status);
			ret = -1;
		}
		switch (wc.opcode) {
			case IBV_WC_RDMA_WRITE:
				if (rw == IBV_WC_RDMA_WRITE)
					return 0;
				break;
			case IBV_WC_RDMA_READ:
				if (rw == IBV_WC_RDMA_READ){
					printf("RDMA READ\n");
					return 0;
				}
				break;
			default:
				fprintf(stderr, "unexpected opcode completion\n");
				ret = -1;
			}
	}
	if (ret) {
		fprintf(stderr, "poll error %d\n", ret);
	}
	return flushed;
}


static struct conn_context *cn_init_ctx(struct ibv_device *ib_dev, int port) {
	struct conn_context *ctx;
	int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ;
	ctx = calloc(1, sizeof(*ctx));
	if (!ctx)
		return NULL;
	ctx->size = 4096;
	ctx->send_flags = IBV_SEND_SIGNALED;
	ctx->rx_depth = 500;

	// ctx->buf = memalign(page_size, ctx->size);
	
	if (posix_memalign((void**)&ctx->buf, page_size, ctx->size)) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
	}
	memset(ctx->buf, 0x7b, ctx->size);
	// Step 1: Initialize RDMA device content
	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n", ibv_get_device_name(ib_dev));
	}

	// Step 2: Create a channel
	ctx->channel = ibv_create_comp_channel(ctx->context);
	if (!ctx->channel) {
		fprintf(stderr, "Couldn't create completion channel\n");
	}

	// Step 3: Create protection domain
	ctx->pd = ibv_alloc_pd(ctx->context);
	if (!ctx->pd) {
		fprintf(stderr, "Couldn't allocate PD\n");
	}

	// Step 4: Register Memory Region
	ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, ctx->size, access_flags);
	if (!ctx->mr) {
		fprintf(stderr, "Couldn't register MR\n");
	}

	// Step 5: Create completion queue
	ctx->cq = ibv_create_cq(ctx->context, 500 + 1, NULL, ctx->channel, 0);
	if (!ctx->cq) {
		fprintf(stderr, "Couldn't create CQ\n");
	}

	struct ibv_qp_attr attr;
	struct ibv_qp_init_attr init_attr = {
		.send_cq = (ctx->cq),
		.recv_cq = (ctx->cq),
		.cap = {
			.max_send_wr = 300,
			.max_recv_wr = 300,
			.max_send_sge = 1,
			.max_recv_sge = 1, 
			.max_inline_data = 16},
		.qp_type = IBV_QPT_RC,
		.sq_sig_all = 1
	};

	// Step 6: Create queue pair
	ctx->qp = ibv_create_qp(ctx->pd, &init_attr);
	if (!ctx->qp) {
		fprintf(stderr, "Couldn't create QP\n");
	}

	ibv_query_qp(ctx->qp, &attr, IBV_QP_CAP, &init_attr);
	if (init_attr.cap.max_inline_data >= ctx->size)
		ctx->send_flags |= IBV_SEND_INLINE;

	struct ibv_qp_attr qpattr = {
		.qp_state = IBV_QPS_INIT,
		.pkey_index = 0,
		.port_num = 1,
		.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
	};

	if (ibv_modify_qp(ctx->qp, &qpattr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
		fprintf(stderr, "Failed to modify QP to INIT\n");
	}

	return ctx;
}

static int cn_connect_ctx(struct conn_context *ctx, int port, int my_psn, struct conn_info *dest){
	struct ibv_qp_attr attr = {
		.qp_state = IBV_QPS_RTR,
		.path_mtu = IBV_MTU_1024,
		.dest_qp_num = dest->qpn,
		.rq_psn = dest->psn,
		.max_dest_rd_atomic = 1,
		.min_rnr_timer = 12,
		.ah_attr = {
			.is_global = 0,
			.dlid = dest->lid,
			.sl = 0,
			.src_path_bits = 0,
			.port_num = port }
	};
	
	if (ibv_modify_qp(ctx->qp, &attr,
				IBV_QP_STATE |
				IBV_QP_AV |
				IBV_QP_PATH_MTU |
				IBV_QP_DEST_QPN |
				IBV_QP_RQ_PSN |
				IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
		fprintf(stderr, "Failed to modify QP to RTR\n");
		return 1;
	}
	
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 14;
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;
	attr.sq_psn = my_psn;
	attr.max_rd_atomic = 1;
	if (ibv_modify_qp(ctx->qp, &attr,
				IBV_QP_STATE |
				IBV_QP_TIMEOUT |
				IBV_QP_RETRY_CNT |
				IBV_QP_RNR_RETRY |
				IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC)) {
		fprintf(stderr, "Failed to modify QP to RTS\n");
		return 1;
	}
	
	return 0;	
}

// static int cn_post_recv(struct conn_context *ctx, int n){
static int cn_post_recv(struct conn_context *ctx, int n){
	struct ibv_sge list = {
		.addr = (uintptr_t) ctx->buf,
		.length = ctx->size,
		.lkey = ctx->mr->lkey
	};

	struct ibv_recv_wr wr = {
		.wr_id = 1,
		.sg_list = &list,
		.num_sge = 1,
	};

	struct ibv_recv_wr *bad_wr;
	int i;

	for (i = 0; i < n; ++i)
		if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
			break;
	return i;
}

int cn_get_port_info(struct ibv_context *context, int port, struct ibv_port_attr *attr)
{
	return ibv_query_port(context, port, attr);
}

void *memory_block;
void contiguous_memory(){
	size_t contiguous_size = NUM_BUCKETS * sizeof(Bucket);
	memory_block = malloc(contiguous_size);
	if (!memory_block) {
		fprintf(stderr, "Memory allocation failed for contiguous memory block.\n");
		exit(EXIT_FAILURE);
	}
	for (int i = 0; i < NUM_BUCKETS; i++) {
		memcpy((char *)memory_block + i * sizeof(Bucket), hash_table[i], sizeof(Bucket));
	}
	/*
	for (int i = 0; i < NUM_BUCKETS; i++) {
		Bucket *current_bucket = (Bucket *)((char *)memory_block + i * sizeof(Bucket));
		printf("Bucket %d:\n", i);
		printf("  Record count: %d\n", current_bucket->record_count);
		for (int j = 0; j < current_bucket->record_count; j++) {
			printf("    Word: %s, Frequency: %ld, Locations: ", current_bucket->records[j].word, current_bucket->records[j].frequency);
			for (int k = 0; k < 7; k++) {
				printf("%ld ", current_bucket->records[j].locations[k]);
			}
			printf("\n");
		}
		printf("\n");
	}
	*/
}

void setup_rdma_resources(const char *query_file) {
	struct conn_context *ctx[size];
	struct conn_info my_info[size];

	struct ibv_mr *mr[size];
	struct ibv_device **dev_list;
	struct ibv_device *ib_dev;

	page_size = sysconf(_SC_PAGESIZE);
	dev_list = ibv_get_device_list(NULL);

	if (!dev_list) {
		perror("Failed to get IB devices list");
		return;
	}
	ib_dev = *dev_list;
	
	for(int l = 0; l < size; l ++){
		ctx[l] = cn_init_ctx(ib_dev, 1);
		if (!ctx[l]) {
			return;
		}
		
		int routs = cn_post_recv(ctx[l], ctx[l]->rx_depth);
		if (routs < ctx[l]->rx_depth) {
			fprintf(stderr, "Couldn't post receive (%d)\n", routs);
			return;
		}
		
		if (ibv_req_notify_cq(ctx[l]->cq, 0)) {
			fprintf(stderr, "Couldn't request CQ notification\n");
			return;
		}

		if (cn_get_port_info(ctx[l]->context, 1, &ctx[l]->portinfo)) {
			fprintf(stderr, "Couldn't get port info\n");
			return;
		}

		my_info[l].lid = ctx[l]->portinfo.lid;
		if (ctx[l]->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET && !my_info[l].lid) {
			fprintf(stderr, "Couldn't get local LID\n");
			return;
		}
		my_info[l].qpn = ctx[l]->qp->qp_num;
		my_info[l].psn = lrand48() & 0xffffff;
		contiguous_memory();
		// size_t total_size = NUM_BUCKETS * sizeof(Bucket);
		mr[l] = ibv_reg_mr(ctx[l]->pd, memory_block,NUM_BUCKETS * sizeof(Bucket), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
		my_info[l].rkey = mr[l]->rkey;
		my_info[l].addr = (uint64_t) mr[l]->addr;
		// printf("Rank %d Memory Region Address: %p Rkey: %u with l %d \n", rank, mr[l]->addr, mr[l]->rkey, l);
	}
	
	
	struct conn_info *all_info = malloc(size * size * sizeof(struct conn_info));
	if (!all_info) {
		fprintf(stderr, "Memory allocation failed for all_info.\n");
		MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
	}
	MPI_Allgather(my_info, size * sizeof(struct conn_info), MPI_BYTE, all_info, size * sizeof(struct conn_info), MPI_BYTE, MPI_COMM_WORLD);
	for (int l = 0; l < size; l++) {
		// printf("Rank %d Memory Region Address: %p Rkey: %u with %d\n", l, all_info[l * size + rank].addr, all_info[l * size + rank].rkey, rank);
		int ret = cn_connect_ctx(ctx[l], 1, my_info[l].psn,  &all_info[l * size + rank]);
		if (ret == 1){
			printf("Failed for rank %d, l=%d\n", rank, l);
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);

	if (rank == 1){
		int word_count = 0;
		char **words = query(query_file, &word_count);
		for (int i = 0; i < word_count; i++) {
			unsigned int hash = compute_hash(words[i]);
			int process_id = (hash >> 8) & 0x7;
			int bucket_id = hash & 0xFF;
			// if(strcmp(words[i], "FSU") == 0){

				
			struct ibv_send_wr wr, *bad_wr;
			struct ibv_sge sge;
			
			// uint32_t remote_key = all_info[process_id].rkey;
			// uint64_t remote_addr = all_info[process_id].addr + bucket_id * BUCKET_SIZE;
			uint32_t remote_key = all_info[process_id * size + rank].rkey;
			// uint64_t remote_addr = all_info[process_id * size + rank].addr + bucket_id * BUCKET_SIZE;
			uint64_t remote_addr = all_info[process_id * size + rank].addr + bucket_id * sizeof(Bucket);
			Bucket *local_bucket = (Bucket*) malloc(sizeof(Bucket));
			if (local_bucket == NULL) {
				fprintf(stderr, "Failed to allocate memory for local_bucket.\n");
				exit(1);
			}
			
			/*
			local_bucket->records = (Record *)malloc(sizeof(Record) * 32767);
			if (local_bucket->records == NULL) {
				fprintf(stderr, "Failed to allocate memory for records in local_bucket.\n");
				free(local_bucket); // Free previously allocated memory
				exit(1);
			}
			*/
			// memset(local_bucket, 0, BUCKET_SIZE);
			// local_bucket = aligned_alloc(64, BUCKET_SIZE);
			for(int i=0; i<MAX_RECORDS; i++){
				if (posix_memalign((void*)local_bucket->records[i].word, 64, 64)) {
					fprintf(stderr, "Couldn't allocate aligned memory for word.\n");
				}
				memset(local_bucket->records[i].word, '\0', sizeof(local_bucket->records[i].word));
				local_bucket->records[i].frequency = 0;
				for(int k=0 ; k<7; k++)
					local_bucket->records[i].locations[k]=0;
			}
			
			struct ibv_mr *mr1 = ibv_reg_mr(ctx[process_id]->pd, local_bucket, sizeof(Bucket), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
			if (!mr1) {
				fprintf(stderr, "Memory registration failed.\n");
				exit(1);
			}
			sge.addr = (uint64_t)local_bucket;
			sge.length = sizeof(Bucket);
			sge.lkey = mr1->lkey;

			memset(&wr, 0, sizeof(wr));
			wr.opcode = IBV_WR_RDMA_READ;
			wr.wr.rdma.remote_addr = remote_addr;
			wr.wr.rdma.rkey = remote_key;
			wr.sg_list = &sge;
			wr.num_sge = 1;
			if(ibv_post_send(ctx[process_id]->qp, &wr, &bad_wr)){
				fprintf(stderr, "RDMA read request failed.\n");
				exit(1);
			}
			int ret = poll_rdma_completion(ctx[process_id], IBV_WR_RDMA_READ);
			for (int j = 0; j < 32767; j++) {
				local_bucket->records[j].word[sizeof(local_bucket->records[j].word) - 1] = '\0';
			}
			
			if(local_bucket->records[0].word[0] != '0'){
			printf("Found  %s with ", local_bucket->records[0].word);
			
			unsigned char *raw_data = (unsigned char *)local_bucket->records[0].word;
			for (int i = 0; i < 64; i++) {
				printf("%c", raw_data[i]);
			}
			

			// printf("%s\n", words[i]);
			// printf("\n");
			// printf("Found  %s with ", words[i]);
			printf(" Frequency %lu\n", local_bucket->records[0].frequency);
			}
			for (int j = 0; j < 32767; j++) {

				if (strcmp(local_bucket->records[j].word, words[i]) == 0) {
				
					FILE *ofile = fopen(output_file, "a");
					if (ofile == NULL) {
						perror("Failed to open output file");
						return;
					}
					fprintf(ofile, "%s - Freq: %lu", local_bucket->records[j].word, local_bucket->records[j].frequency);
					int loc_count = 0;
					qsort(local_bucket->records[j].locations, 7, sizeof(uint64_t), compare);
					for (int k = 0; k < 7; k++) {
						if (local_bucket->records[j].locations[k] != 0) {
							if (loc_count == 0) {
								fprintf(ofile, "; Loc (<= 7): ");
							} 
							else {
								fprintf(ofile, " ");
							}
							fprintf(ofile, "%lu", local_bucket->records[j].locations[k]);
							loc_count++;
						}
					}
					if (loc_count == 0) {
						fprintf(ofile, "\n");
					} else {
						fprintf(ofile, " \n");
					}
					fclose(ofile);
					break;
				}
				FILE *ofile = fopen(output_file, "a");
				if (ofile == NULL) {
					perror("Failed to open output file");
					return;
				}
				fprintf(ofile, "%s - Freq: %d ", words[i], 0);
				fprintf(ofile, "\n");
				fclose(ofile);
				break;		
			}
			printf("After query\n");
			
		}
	}
	
}


int get_file_size(const char *filename) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        perror("Unable to open file");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
    fseek(fp, 0, SEEK_END);
    int size = ftell(fp);
    fclose(fp);
    return size;
}

SizesAndOffsets calculate_offsets(int file_size, int size) {
	SizesAndOffsets result;
	result.portion_sizes = malloc(size * sizeof(int));
	result.offsets = malloc(size * sizeof(int));
	if (result.portion_sizes == NULL || result.offsets == NULL) {
		perror("Failed to allocate memory");
		exit(EXIT_FAILURE);
	}
	int base_size = file_size / size;
	int remainder = file_size % size;
	int offset = 0;

	for (int i = 0; i < size; i++) {
        	result.portion_sizes[i] = base_size + (i < remainder ? 1 : 0);
        	result.offsets[i] = offset;
        	offset += result.portion_sizes[i];
    	}

	return result;
}
char *read_portion(const char *filename, int my_portion_size, int my_offset, int rank) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        perror("Unable to open file for reading");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }

    char *buffer = (char *)malloc((my_portion_size + 1) * sizeof(char));
    if (buffer == NULL) {
         perror("Unable to allocate buffer");
	 MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
    fseek(fp, my_offset, SEEK_SET);
    fread(buffer, 1, my_portion_size, fp);
    buffer[my_portion_size] = '\0';
    fclose(fp);

    return buffer;
}

int find_last_alphanumeric_index(const char *buffer, int size) {
    int index = size - 1;
    // while (index >= 0 && buffer[index] != ' ') {
    while (index >= 0 && (isalnum(buffer[index]))){
        index--;
    }
    return index;
}

void adjust_for_word_split(int rank, int size, char **my_buffer, int *my_portion_size, int *my_offset) {
	if (rank > 0) {
		int chars_to_receive;
		MPI_Recv(&chars_to_receive, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		// printf("rank %d, my_portion_size %d, my_offset %d, my_buffer: \n %s\n", rank, *my_portion_size, *my_offset, *my_buffer);
		// printf("rank %d, chars to receive %d\n", rank, chars_to_receive);
		if (chars_to_receive > 0) {
			char *received_buffer = (char *)malloc((chars_to_receive + 1) * sizeof(char));
			MPI_Recv(received_buffer, chars_to_receive + 1, MPI_CHAR, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			// printf("rank %d, receive chars %s\n", rank, received_buffer);
			*my_offset -= chars_to_receive;
			*my_buffer = (char *)realloc(*my_buffer, (*my_portion_size + chars_to_receive + 1) * sizeof(char));
			memmove(*my_buffer + chars_to_receive, *my_buffer, *my_portion_size);
			memcpy(*my_buffer, received_buffer, chars_to_receive);
			*my_portion_size += chars_to_receive;
			(*my_buffer)[*my_portion_size] = '\0';
			free(received_buffer);
		}
		// printf("rank %d, my_portion_size %d, my_offset %d, my_buffer: \n %s\n", rank, *my_portion_size, *my_offset, *my_buffer);
	}
	if (rank < size - 1) {
		int last_index = find_last_alphanumeric_index(*my_buffer, *my_portion_size);
		int send_start_index = last_index + 1;
		int chars_to_send = *my_portion_size - send_start_index;
		// printf("rank %d, my_portion_size %d, my_offset %d, my_buffer: \n %s\n", rank, *my_portion_size, *my_offset, *my_buffer);
		// printf("rank %d, chars to send %d\n", rank, chars_to_send);
		MPI_Send(&chars_to_send, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
		if (chars_to_send > 0) {
			MPI_Send(*my_buffer + send_start_index, chars_to_send + 1, MPI_CHAR, rank + 1, 0, MPI_COMM_WORLD);
			// printf("rank %d, sent chars %s\n", rank, *my_buffer + send_start_index);
		}
		*my_portion_size -= chars_to_send;
		(*my_buffer)[*my_portion_size] = '\0';
		// printf("rank %d, my_portion_size %d, my_offset %d, my_buffer: \n %s\n", rank, *my_portion_size, *my_offset, *my_buffer);
	}
}

void process_buffer(char *buffer, int portion_size) {
	int i = 0;
	int packet_counts[8] = {0};  // Packet counts for each process
	// Temporary arrays to hold the packets
	Packet **packets = malloc(8 * sizeof(Packet *));

	// Initialize each process's packet list
	for (int i = 0; i < 8; i++) {
		packets[i] = malloc(packet_counts[i] * sizeof(Packet));
	}
	while (i < portion_size) {
		// Skip any non-alphanumeric characters
		while (i < portion_size && !isalnum(buffer[i])) i++;

		// Identify a word starting here
		int word_start = i;
		while (i < portion_size && isalnum(buffer[i])) i++;
		int word_end = i;

		// If a word is identified within length constraints (1 to 62 characters)
		if (word_end - word_start >= 1 && word_end - word_start <= 62) {
			char word[63] = {0};
			strncpy(word, buffer + word_start, word_end - word_start);
			for (int j = 0; j < word_end - word_start; j++) {
				word[j] = toupper(word[j]);
			}
			// printf("word %s\n", word);
			unsigned int hash = compute_hash(word);
			int process_id = (hash >> 8) & 0x7;
			int bucket_id = hash & 0xFF;
			// printf("process_id %d, bucket_id %d\n", process_id, bucket_id);
			uint64_t location = my_offset + word_start;
			// printf("Location of word in file: %lu\n", location);

			// Increment the count of packets for the corresponding process
			packet_counts[process_id]++;
			packets[process_id] = realloc(packets[process_id], sizeof(Packet) * packet_counts[process_id]);
			if (packets[process_id] == NULL) {
				fprintf(stderr, "Memory allocation failed for packets in process %d\n", process_id);
				MPI_Abort(MPI_COMM_WORLD, 1);
			}
	
			Packet packet;
			strncpy(packet.word, word, sizeof(packet.word) - 1 );
			packet.word[sizeof(packet.word) - 1 ] = '\0';
			packet.bucket_id = bucket_id;
			packet.location = location;
			packets[process_id][packet_counts[process_id] - 1] = packet;

		}
	}

	MPI_Barrier(MPI_COMM_WORLD);

	/*
	for (int p = 0; p < packet_counts[rank]; p++) {
		hash_table = update_hash_table(packets[rank][p]);
	}
	*/
	MPI_Request request;

	for (int i = 0; i < size; i++) {	
		int target_rank = ((rank + i) % size);
		// if (rank == 3) printf("target rank %d\n", target_rank);
		MPI_Isend(&packet_counts[target_rank], 1, MPI_INT, target_rank, 0, MPI_COMM_WORLD, &request);
	}

	
	int recv_counts[8];
	for (int i = 0; i < size; i++) {
		int source_rank = (rank - i + size) % size;
		MPI_Recv(&recv_counts[source_rank], 1, MPI_INT, source_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		int target_rank = (rank + i + 1) % size;
		MPI_Isend(packets[target_rank], sizeof(Packet) * packet_counts[target_rank], MPI_BYTE, target_rank, 0, MPI_COMM_WORLD, &request);
	}
	
	for (int i = 0; i < size; i++) {
		int source_rank = (rank - i + size) % size;
		if (recv_counts[source_rank] > 0) {
			// if (rank == 4)
		        // printf("Rank %d receiving packets from Rank %d\n", rank, source_rank);
			Packet *recv_packets = malloc(sizeof(Packet) * recv_counts[source_rank]);
			MPI_Recv(recv_packets, sizeof(Packet) * recv_counts[source_rank], MPI_BYTE, source_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			for (int j = 0; j < recv_counts[source_rank]; j++) {
				// if(rank == 3)
                        update_hash_table(recv_packets[j]);

			}
			free(recv_packets);
		}
	}

	free(packets);
}

/*
void allocate_hash_table() {
	// Step 1: Calculate the total size for the hash table and all buckets
	size_t pointer_array_size = NUM_BUCKETS * sizeof(Bucket*);
	// size_t buckets_size = NUM_BUCKETS * BUCKET_SIZE;
	size_t buckets_size = NUM_BUCKETS * sizeof(Bucket);
	size_t total_size = pointer_array_size + buckets_size;

	// Step 2: Allocate a single contiguous block of memory
	memory_block = malloc(total_size);
	if (!memory_block) {
		fprintf(stderr, "Memory allocation failed for hash table.\n");
		exit(EXIT_FAILURE);
	}

	// Step 3: Assign hash_table to the beginning of the memory block
	hash_table = (Bucket**)memory_block;

	// Step 4: Set up each bucket within the contiguous block
	for (int i = 0; i < NUM_BUCKETS; i++) {
		hash_table[i] = (Bucket*)((char*)memory_block + i * sizeof(Bucket*) + i * sizeof(Bucket));
		// hash_table[i]->records = (Record *)malloc(MAX_RECORDS * sizeof(Record));
		hash_table[i]->record_count = 0;
		hash_table[i]->header[0] = '\0';
		
		for (int j = 0; j < MAX_RECORDS; j++) {
			// memset(hash_table[i]->records[j].word, '0', sizeof(hash_table[i]->records[j].word) - 1);
			
			hash_table[i]->records[j].word[63] = '\0';
			for (int l; l < 62; l++){
				hash_table[i]->records[j].word[l] = 'A';
			}
			
			// strncpy(hash_table[i]->records[j].word, "\0", sizeof(hash_table[i]->records[j].word) - 1 );
			hash_table[i]->records[j].frequency = 0;
			for (int k = 0; k < 7; k++) {
				hash_table[i]->records[j].locations[k] = 0;
			}
		}
	}
}
*/

void allocate_hash_table() {
	// Step 1: Allocate memory for the hash table (array of pointers to buckets)
	hash_table = (Bucket **)malloc(NUM_BUCKETS * sizeof(Bucket *));
	if (!hash_table) {
		fprintf(stderr, "Memory allocation failed for hash_table.\n");
		exit(EXIT_FAILURE);
	}

	// Step 2: Allocate memory for each bucket separately
	for (int i = 0; i < NUM_BUCKETS; i++) {
		hash_table[i] = (Bucket *)malloc(sizeof(Bucket));
		if (!hash_table[i]) {
			fprintf(stderr, "Memory allocation failed for bucket %d.\n", i);
			exit(EXIT_FAILURE);
		}
		// Initialize bucket metadata and records
		hash_table[i]->record_count = 0;
		hash_table[i]->header[0] = '\0'; // Initialize the header

		// Initialize each record in the current bucket
		for (int j = 0; j < MAX_RECORDS; j++) {
			memset(hash_table[i]->records[j].word, '0', sizeof(hash_table[i]->records[j].word) - 1);
			hash_table[i]->records[j].word[63] = '\0'; // Null terminate the wor
			hash_table[i]->records[j].frequency = 0; // Initialize frequency
			for (int k = 0; k < 7; k++) {
				hash_table[i]->records[j].locations[k] = 0;
			}
		}
	}
}

// Function to free the allocated hash table
void free_hash_table(Bucket **table) {
    for (int i = 0; i < NUM_BUCKETS; i++) {
        free(table[i]); // Free each bucket
    }
    free(table); // Free the hash table array itself
}

// Function to update the hash table with the received word
void update_hash_table(Packet packet) {
	Bucket *bucket = hash_table[packet.bucket_id]; // Get the corresponding bucket
	// bucket->records = (Record *)malloc(MAX_RECORDS * sizeof(Record));
	if (!bucket->records) {
		fprintf(stderr, "Error: Memory allocation failed for records in bucket %d.\n", packet.bucket_id);
		    return;
	}
	
	for (int i = 0; i < bucket->record_count; i++) {
		if (strcmp(bucket->records[i].word, packet.word) == 0) {
			bucket->records[i].frequency++;
			if (bucket->records[i].locations[6] != '\0') {
                                int largest_index = 0;
                                for (int j = 1; j < 7; j++) {
                                        if (bucket->records[i].locations[j] > bucket->records[i].locations[largest_index]) {
                                                largest_index = j;
                                        }
                                }
                                if (packet.location < bucket->records[i].locations[largest_index]) {
                                        bucket->records[i].locations[largest_index] = packet.location;
                                }
                        } else {
                                for (int j = 0; j < 7; j++) {
                                        if (bucket->records[i].locations[j] == '\0') {
                                                bucket->records[i].locations[j] = packet.location;
                                                break;
                                        }
                                }
                        }
			return;
		}
	}

	if (bucket->record_count < MAX_RECORDS) {
		 // Record *new_record = malloc(sizeof(Record));
		 // Record *new_record = &bucket->records[bucket->record_count];
		 bucket->records[bucket->record_count].frequency = 1;
		 bucket->records[bucket->record_count].locations[0] = packet.location;
		 strncpy(bucket->records[bucket->record_count].word, packet.word, sizeof(bucket->records[bucket->record_count].word) - 1 );
		 bucket->records[bucket->record_count].word[sizeof(bucket->records[bucket->record_count].word) - 1] = '\0';
		 // printf("new %s\n", bucket->records[bucket->record_count].word);
		 bucket->record_count++;
	}
	else{
		fprintf(stderr, "Error: Bucket %d is full, cannot add new record for '%s'.\n", packet.bucket_id, packet.word);
	}

}


// Function to print the contents of the hash table
void print_hash_table() {
	for (int i = 0; i < NUM_BUCKETS; i++) {
		Bucket *bucket = hash_table[i];
		if (bucket->record_count > 0) {
			printf("Bucket %d:\n", i);
			printf("Record count %d\n", bucket->record_count);
			for (int j = 0; j < bucket->record_count; j++) {
				Record *record = &bucket->records[j];
				printf("  Word: %s, Frequency: %lu, Locations: ", record->word, record->frequency);
				for (int k = 0; k < 7; k++) {
					printf("%lu ", record->locations[k]);
				}
				printf("\n");
			}
		}
	}
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    char hostname[20];
    gethostname(hostname, 20);
    printf("Rank %d on %s\n", rank, hostname);

    if (argc < 3) {
        if (rank == 0) {
            fprintf(stderr, "Usage: %s <filename> <query_file>\n", argv[0]);
        }
        MPI_Finalize();
        exit(EXIT_FAILURE);
    }
    const char *filename = argv[1];
    portion_sizes = (int *)malloc(size * sizeof(int));
    offsets = (int *)malloc(size * sizeof(int));

    if (rank == 0) {
        int file_size = get_file_size(filename);

        SizesAndOffsets sizes_offsets = calculate_offsets(file_size, size);
	for (int i = 0; i < size; i++) {
		portion_sizes[i] = sizes_offsets.portion_sizes[i];
		offsets[i] = sizes_offsets.offsets[i];
	}

    }

    MPI_Bcast(portion_sizes, size, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(offsets, size, MPI_INT, 0, MPI_COMM_WORLD);
    
    my_portion_size = portion_sizes[rank];
    my_offset = offsets[rank];

    my_buffer = read_portion(filename, my_portion_size, my_offset, rank);

    adjust_for_word_split(rank, size, &my_buffer, &my_portion_size, &my_offset);

    allocate_hash_table();

    process_buffer(my_buffer, my_portion_size);
 
    MPI_Barrier(MPI_COMM_WORLD);
    // print_hash_table();
    
    // const char *output_file = "results.txt";

    
    remove(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 0) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 1) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 2) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 3) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 4) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 5) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 6) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 7) find_top_frequency_record(output_file);
    MPI_Barrier(MPI_COMM_WORLD);

    setup_rdma_resources(argv[2]);

    free_hash_table(hash_table);
    
    if (rank == 0) {
	free(portion_sizes);
	free(offsets);
    }
    MPI_Finalize();
    return 0;
}
