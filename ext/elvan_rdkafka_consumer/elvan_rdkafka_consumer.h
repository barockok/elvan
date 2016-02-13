#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <ctype.h>
#include <errno.h>
#include <syslog.h>
#include <assert.h>
#include "ruby.h"
#include "librdkafka/rdkafka.h"

typedef struct {
    VALUE consumer_config_hash;
    VALUE initialTopics;
    char errstr[512];
    int exit_eof;
    int subscribed;
    
} Elvan_Config_t;