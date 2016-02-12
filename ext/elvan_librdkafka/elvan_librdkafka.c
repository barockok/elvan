#include "elvan_librdkafka.h"

static VALUE elvan_module;
static int running;
static rd_kafka_t *rd_kafka_inst;
static rd_kafka_conf_t* rd_kafka_conf;
static rd_kafka_topic_partition_list_t *topicPartitionList;
#define ELVAN_MAX_ERRSTR_LEN 512

// Should we expect rb_thread_blocking_region to be present?
// #define RB_THREAD_BLOCKING_REGION
#undef RB_THREAD_BLOCKING_REGION

typedef struct {
    char *topic;
    char errstr[512];
    char *group_id;
    char *client_id;
    char *brokers;
    char **initialTopics;

    int exit_eof;
    int wait_eof;
    int silent;
    int isInitialized;
    int isConnected;
    int subscribed;

    char *error;
} Elvan_Config_t;




// Utility Methods
static void stop (int sig) {
	running = 0;
}

static void logger(const rd_kafka_t *rk,
                   int level,
                   const char *fac,
                   const char *buf) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
            (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
            level, fac, rd_kafka_name(rk), buf);
}

char** str_split(char* a_str, const char a_delim){
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }

    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
     knows where the list of returned strings ends. */
    count++;

    result = malloc(sizeof(char*) * count);

    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            assert(idx < count);
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }

    return result;
}

// Helper Methods
static void helper__wait_kafka_destroy(rd_kafka_t *rk,
                                       int wait_to,
                                       int max_attempt){
   
    while (max_attempt-- > 0 && rd_kafka_wait_destroyed(wait_to) == -1)
        fprintf(stderr, "Waiting for librdkafka to decommission\n");
    
    if (max_attempt <= 0)
        fprintf(stderr, "librdkafka to decommissioned\n");
}

static void helper__print_partition_list (FILE *fp, int is_assigned,
                                          const rd_kafka_topic_partition_list_t
                                          *partitions, Elvan_Config_t* conf) {
    int i;
    for (i = 0 ; i < partitions->cnt ; i++) {
        fprintf(stderr, "%s %s parition: [%"PRId32"] offset: [%"PRId32"]",
                i > 0 ? ",":"",
                partitions->elems[i].topic,
                partitions->elems[i].partition,
                partitions->elems[i].offset
                );

        if (is_assigned)
            conf->wait_eof++ ;
        else {
            if (conf->exit_eof && --conf->wait_eof == 0)
                running = 0;
        }
    }
    fprintf(stderr, "\n");
}

static void helper__retrive_initial_partitionList_position(Elvan_Config_t* conf){
    rd_kafka_resp_err_t err;

    err = rd_kafka_position(rd_kafka_inst, topicPartitionList, 1000);

    if (err) {
        rb_raise(rb_eRuntimeError, "%% Failed to fetch offsets: %s\n", rd_kafka_err2str(err));
    }else{
        helper__print_partition_list(stderr, 1, topicPartitionList, conf);
    }
}

static void helper__rebalance_cb (rd_kafka_t *rk,
                                  rd_kafka_resp_err_t err,
                                  rd_kafka_topic_partition_list_t *partitions,
                                  void *opaque) {

    Elvan_Config_t* conf = (Elvan_Config_t *)rd_kafka_opaque(rk);

    fprintf(stderr, "%% Consumer group rebalanced: ");
    switch (err)
    {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            fprintf(stderr, "assigned:\n");
            helper__print_partition_list(stderr, 1, partitions, conf);
            rd_kafka_assign(rk, partitions);
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            fprintf(stderr, "revoked:\n");
            helper__print_partition_list(stderr, 0, partitions, conf);
            rd_kafka_assign(rk, NULL);
            break;

        default:
            fprintf(stderr, "failed: %s\n",
                    rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
    }
}

static void *helper__consumer_recv_msg(void *ptr){
    rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rd_kafka_inst, 500);
    if ( rkmessage == NULL ) {
        if ( errno != ETIMEDOUT )
            fprintf(stderr, "%% Error: %s\n", rd_kafka_err2str( rd_kafka_errno2err(errno)));
    }
    return (void *) rkmessage;
}


static void helper__parse_initialTopic_to_partitionList(Elvan_Config_t* conf){
    if(!conf->initialTopics)
        return ;

    topicPartitionList = rd_kafka_topic_partition_list_new(1);

    for (int i = 0; *(conf->initialTopics + i); i++)
    {
        char *topic_name = strdup(*(conf->initialTopics + i));
        rd_kafka_topic_partition_list_add(topicPartitionList, topic_name, -1);
    }

}

static void helper__init_kafka_conf(Elvan_Config_t *conf){
    rd_kafka_conf = rd_kafka_conf_new();

    rd_kafka_conf_set_opaque(rd_kafka_conf, (void*)conf);

    if (rd_kafka_conf_set(rd_kafka_conf, "group.id", conf->group_id,
                          conf->errstr, sizeof(conf->errstr)) != RD_KAFKA_CONF_OK) {
        rb_raise(rb_eRuntimeError, "%% %s\n", conf->errstr);

    }

    if (rd_kafka_conf_set(rd_kafka_conf, "bootstrap.servers", conf->brokers,
                          conf->errstr, sizeof(conf->errstr)) != RD_KAFKA_CONF_OK) {
        rb_raise(rb_eRuntimeError, "%% %s\n", conf->errstr);

    }

    /* Set logger */
    rd_kafka_conf_set_log_cb(rd_kafka_conf, logger);
    rd_kafka_conf_set_rebalance_cb(rd_kafka_conf, helper__rebalance_cb);

}

static void helper__init_kafka_consumer(Elvan_Config_t* conf){
    if (!(rd_kafka_inst = rd_kafka_new(RD_KAFKA_CONSUMER, rd_kafka_conf,
                                             conf->errstr, sizeof(conf->errstr)))) {
        rb_raise(rb_eRuntimeError, "%% Failed to create new kafka consumer: %s\n", conf->errstr);
    }

    rd_kafka_set_log_level(rd_kafka_inst, LOG_DEBUG);

    helper__parse_initialTopic_to_partitionList(conf);
    helper__retrive_initial_partitionList_position(conf);

    if (topicPartitionList == NULL){
        rb_raise(rb_eRuntimeError, "TopicPartitionList can't be empty\n");
    }
}

static void helper__msg_consume(rd_kafka_message_t *rkmessage, Elvan_Config_t *conf){
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            if (conf->exit_eof) {
                fprintf(stderr,
                        "%% Consumer reached end of %s [%"PRId32"] "
                        "message queue at offset %"PRId64"\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        rkmessage->partition, rkmessage->offset);

                //                running = 0;
            }

            return;
        }

        fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
                "offset %"PRId64": %s\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset,
                rd_kafka_message_errstr(rkmessage));
        return;
    }

    // Yield the data to the Consumer's block
    if (rb_block_given_p()) {
        VALUE key, data, offset;

        data = rb_str_new((char *)rkmessage->payload, rkmessage->len);
        offset = rb_ll2inum(rkmessage->offset);

        if ( rkmessage->key_len > 0 ) {
            key = rb_str_new((char*) rkmessage->key, (int)rkmessage->key_len);
        } else {
            key = Qnil;
        }

        rd_kafka_message_destroy(rkmessage);
        rb_yield_values(3, data, key, offset);
    }
    else {
        rb_raise(rb_eArgError, "no block given" );
    }

}

static VALUE helper__consumer_loop_stop(VALUE self){
    Elvan_Config_t* conf;
    rd_kafka_resp_err_t rd_err;

    Data_Get_Struct(self, Elvan_Config_t, conf);

    rd_err = rd_kafka_unsubscribe(rd_kafka_inst);
    if (rd_err){
        rb_raise(rb_eRuntimeError, "%% Failed to close unsubscribe: %s\n", rd_kafka_err2str(rd_err));
        return Qnil;
    }else
        fprintf(stderr, "%% Success unsubscribe\n");
    
    rd_err = rd_kafka_consumer_close(rd_kafka_inst);
    if(rd_err){
        rb_raise(rb_eRuntimeError, "%% Failed to close consumer: %s\n", rd_kafka_err2str(rd_err));
        return Qnil;
    }else
        fprintf(stderr, "%% Success closed\n");
    
    running = 0;
    conf->subscribed = 0;
  
    return Qnil;
}

static void helper__elvan_consumer_stop_cb(void *ptr) {
    running = 0;
}

static VALUE helper__consumer_loop(VALUE self){

    Elvan_Config_t* conf;
    rd_kafka_message_t *rkmessage;
    Data_Get_Struct(self, Elvan_Config_t, conf);

    while (running) {
#if HAVE_RB_THREAD_BLOCKING_REGION && RUBY_API_VERSION_MAJOR < 2
        rkmessage = (rd_kafka_message_t *) rb_thread_blocking_region((rb_blocking_function_t *) helper__consumer_recv_msg,
                                                                     conf,
                                                                     helper__elvan_consumer_stop_cb,
                                                                     conf);
#elif HAVE_RB_THREAD_CALL_WITHOUT_GVL
        rkmessage = rb_thread_call_without_gvl(helper__consumer_recv_msg,
                                               conf,
                                               helper__elvan_consumer_stop_cb,
                                               conf);
#else
        rkmessage = helper__consumer_recv_msg(conf);
#endif
        if (rkmessage)
            helper__msg_consume(rkmessage, conf);

    }
    return Qnil;
}

// Elvan::RdKafka::Consumer istance Methods
static void elvan_consumer_free(void *p) {

    Elvan_Config_t *conf = (Elvan_Config_t *)p;

    if (topicPartitionList != NULL) {
        rd_kafka_topic_partition_list_destroy(topicPartitionList);
    }

    if (rd_kafka_inst != NULL) {
        rd_kafka_destroy(rd_kafka_inst);
        helper__wait_kafka_destroy(rd_kafka_inst, 1000, 5);
    }

    free(conf->group_id);
    free(conf->brokers);
    free(conf);
}
static VALUE elvan_consumer_allocate(VALUE klass) {

    VALUE obj;
    Elvan_Config_t *conf;

    conf                     = ALLOC(Elvan_Config_t);
    conf->group_id           = NULL;
    conf->brokers            = NULL;
    conf->initialTopics      = NULL;
    conf->silent             = 1;
    conf->errstr[0]          = 0;
    conf->exit_eof           = 0;
    conf->wait_eof           = 0;
    conf->subscribed         = 0;

    obj = Data_Wrap_Struct(klass, 0, elvan_consumer_free, conf);

    signal(SIGINT, stop);
    return obj;
}
static VALUE elvan_initialize(VALUE self,
                              VALUE brokers,
                              VALUE initialTopic,
                              VALUE group_id,
                              VALUE exit_eof,
                              VALUE is_silent){

    Elvan_Config_t *conf;
    char *brokersPtr, *group_idPtr, *initialTopicPtr;
    int exit_eof_int, is_silentInt;

    brokersPtr      = StringValuePtr(brokers);
    group_idPtr     = StringValuePtr(group_id);
    initialTopicPtr = StringValuePtr(initialTopic);
    is_silentInt    = FIX2INT(is_silent);
    exit_eof_int    = FIX2INT(exit_eof);

    Data_Get_Struct(self, Elvan_Config_t, conf);

    conf->brokers       = strdup(brokersPtr);
    conf->group_id      = strdup(group_idPtr);
    conf->silent        = is_silentInt;
    conf->initialTopics = str_split(initialTopicPtr, ',');
    conf->exit_eof      = exit_eof_int || 0;
    conf->wait_eof      = 0;
    return self;
}

static VALUE elvan_consume(VALUE self){

    Elvan_Config_t* conf;
    rd_kafka_resp_err_t rd_err;

    Data_Get_Struct(self, Elvan_Config_t, conf);

    helper__init_kafka_conf(conf);
    helper__init_kafka_consumer(conf);

    if ((rd_err = rd_kafka_subscribe(rd_kafka_inst, topicPartitionList))) {
        fprintf(stderr,
                "%% Failed to start consuming topics: %s\n",
                rd_kafka_err2str(rd_err));
        rb_raise(rb_eRuntimeError, "Failed to start consuming topic %s", conf->group_id);
        return Qnil;
    }
    conf->subscribed = 1;
    running = 1;

    return rb_ensure(helper__consumer_loop, self, helper__consumer_loop_stop, self);
}
static VALUE elvan_consume_stop(VALUE self){
    Elvan_Config_t* conf;
    Data_Get_Struct(self, Elvan_Config_t, conf);
    running = 0;
    return self;
}

void Init_elvan_librdkafka() {
    VALUE consumer_klass, rd_kafka_module;
    elvan_module = rb_define_module("Elvan");
    rd_kafka_module = rb_define_module_under(elvan_module, "RdKafka");
    consumer_klass = rb_define_class_under(rd_kafka_module, "Consumer", rb_cObject);

    rb_define_alloc_func(consumer_klass, elvan_consumer_allocate);
    rb_define_method(consumer_klass, "initialize", elvan_initialize, 5);
    rb_define_method(consumer_klass, "consume", elvan_consume, 0);
    rb_define_method(consumer_klass, "consume_stop!", elvan_consume_stop, 0);
}