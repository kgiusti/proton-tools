/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <assert.h>
#include <glib.h>

static int log = 0;

////////////////////////////////////////////////////////////////////////////////
//
void enable_logging()
{
    log = 1;
}


////////////////////////////////////////////////////////////////////////////////
//
void LOG( const char *fmt, ... )
{
    if (log) {
        va_list ap;
        va_start(ap, fmt);
        vfprintf( stdout, fmt, ap );
        va_end(ap);
    }
}


////////////////////////////////////////////////////////////////////////////////
// fatal error: print debug info and terminate application
//
void DIE(const char *file, int line, const char *fmt, ...)
{
    fprintf(stderr, "%s:%i:", file, line );
    va_list ap;
    va_start( ap, fmt );
    vfprintf( stderr, fmt, ap );
    va_end( ap );
    exit(1);
}


////////////////////////////////////////////////////////////////////////////////
// "sigh" - would be nice if proton exported pn_strdup(), since
// strdup() is not present in old C compilers (*COUGH* uSoft *COUGH*)
//
char *_strdup( const char *src )
{
  char *r = NULL;
  if (src) {
    r = (char *) malloc(sizeof(char) * (strlen(src) + 1));
    if (r) strcpy(r,src);
  }
  return r;
}


////////////////////////////////////////////////////////////////////////////////
// "sigh" part deux - would be nice if proton exported pn_i_now(), too
//
pn_timestamp_t _now()
{
  struct timeval now;
  if (gettimeofday(&now, NULL)) abort();
  return ((pn_timestamp_t)now.tv_sec) * 1000 + (now.tv_usec / 1000);
}


////////////////////////////////////////////////////////////////////////////////
// Send a message and confirm receipt by remote.
//
pn_status_t deliver_message( pn_messenger_t *messenger,
                             pn_message_t *message )
{
    pn_status_t result = PN_STATUS_UNKNOWN;
    bool done = false;

    pn_messenger_put( messenger, message );
    pn_tracker_t request_tracker = pn_messenger_outgoing_tracker( messenger );

    LOG("sending message...\n");
    int rc = pn_messenger_send( messenger );
    if (rc) LOG( "pn_messenger_send() failed: error=%d\n", rc );

    result = pn_messenger_status( messenger, request_tracker );
    switch (result) {
    case PN_STATUS_REJECTED:
        LOG( "Sent message rejected by remote!\n" );
        break;
    case PN_STATUS_ACCEPTED:
        LOG( "Sent message accepted by remote.\n" );
        break;
    default:
        LOG( "Unexpected outcome for send received from peer: %d\n", (int) result );
        break;
    }
    LOG( "Settling the delivery...\n" );
    rc = pn_messenger_settle( messenger, request_tracker, 0 );
    if (rc) LOG( "pn_messenger_settle() failed: error=%d\n", rc );
    return result;
}




typedef struct DeduplicationDb_s {
    GHashTable *hTable;
    DeduplicationDeleter_t *deleter;
    void *handle;
} DeduplicationDb_t;

typedef struct {
    char *key;
    void *data;
    pn_timestamp_t expire;
} DeduplicationNode_t;


DeduplicationDb_t *DeduplicationDbNew( DeduplicationDeleter_t *deleter,
                                       void *handle)
{
    DeduplicationDb_t *db = malloc( sizeof(DeduplicationDb_t) );
    check( db, "Out of Memory.");
    db->hTable = g_hash_table_new( g_str_hash, g_str_equal );
    check( db->hTable, "Failed to initialize de-duplication hashtable." );
    db->deleter = deleter;
    db->handle = handle;
    return db;
}

void DeduplicationDbDelete( DeduplicationDb_t *db )
{
    if (db) {
        assert(g_hash_table_size( db->hTable ) == 0);
        g_hash_table_unref( db->hTable );
        free( db );
    }
}

// remember the received message until "expire" time
//
void DeduplicationRemember( DeduplicationDb_t *db,
                            const char *key,
                            void *data,
                            pn_timestamp_t expire )
{
    DeduplicationNode_t *n;
    n = (DeduplicationNode_t *)g_hash_table_lookup( db->hTable, key );
    LOG("Adding new entry to deduplication database, key=%s\n", key );
    if ( n ) {
        LOG("... already present, updating expire time to %lu\n", (unsigned long) expire );
        n->expire = expire;
        n->data = data;
    } else {
        n = (DeduplicationNode_t *)malloc( sizeof(DeduplicationNode_t) + strlen( key ) + 1 );
        check( n, "Out of memory." );
        n->key = (char *)&n[1];
        strcpy( n->key, key );
        n->expire = expire;
        n->data = data;
        g_hash_table_insert( db->hTable, n->key, n );
    }
}


// remove the message from the deduplication database
//
void DeduplicationForget( DeduplicationDb_t *db,
                          const char *key )
{
    DeduplicationNode_t *n;
    n = (DeduplicationNode_t *)g_hash_table_lookup( db->hTable, key );
    if ( n ) {
        g_hash_table_remove( db->hTable, key );
        free( n );
    }
}


// has message already been seen?
//
bool DeduplicationIsDuplicate( DeduplicationDb_t *db,
                               const char *key,
                               void **data)
{
    DeduplicationNode_t *n = (DeduplicationNode_t *) g_hash_table_lookup( db->hTable, key );
    if (n) {
        if (n->expire <= _now()) {
            LOG( "expiring old message from deduplication database: %s\n", key );
            g_hash_table_remove( db->hTable, key );
            if (db->deleter) db->deleter( db->handle, n->key, n->data );
            free( n );
            n = NULL;
        } else {
            if (data) *data = n->data;
        }
    }

    return n != NULL;
}

pn_timestamp_t DeduplicationPurgeExpired( DeduplicationDb_t *db )
{
    const char *key;
    DeduplicationNode_t *n;
    GHashTableIter i;
    pn_timestamp_t now = _now();
    pn_timestamp_t next_call = 0;
    g_hash_table_iter_init( &i, db->hTable );
    while (g_hash_table_iter_next( &i, (gpointer *)&key, (gpointer *)&n )) {
        if (n->expire <= now) {
            LOG( "purging old message from deduplication database: %s\n", key );
            g_hash_table_iter_remove( &i );
            if (db->deleter) {
                db->deleter( db->handle, n->key, n->data );
            }
            free( n );
        } else if (next_call == 0 || n->expire < next_call) {
            next_call = n->expire;
        }
    }

    return next_call;
}

