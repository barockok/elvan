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
    char *topic;
    char errstr[512];
    char *client_id;
    char **initialTopics;

    int exit_eof;
    int isInitialized;
    int isConnected;
    int subscribed;

    char *error;
} Elvan_Config_t;