#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

typedef struct {  // Struct for helper functions
  mapper_t mapper;
  kvlist_t *input;
  reducer_t reducer;
  kvlist_t *output;
} t_struct;

void *map_helper(void *val) {
  t_struct *vals = (t_struct *)val;
  kvlist_iterator_t *rep = kvlist_iterator_new(vals->input);
  kvpair_t *p;
  p = kvlist_iterator_next(rep);
  while (p != NULL) {
    vals->mapper(p, vals->output);
    p = kvlist_iterator_next(rep);
  }

  kvlist_iterator_free(&rep);
  return NULL;
}

void *reduce_helper(void *val) {
  t_struct *vals = (t_struct *)val;
  kvlist_sort(vals->input);

  kvlist_iterator_t *rep = kvlist_iterator_new(vals->input);
  kvpair_t *p2, *last = NULL;
  kvlist_t *result = kvlist_new();

  while ((p2 = kvlist_iterator_next(rep)) != NULL) {
    if (last == NULL || strcmp(last->key, p2->key) != 0) {
      if (last != NULL) {
        vals->reducer(last->key, result, vals->output);
        kvlist_free(&result);
        result = kvlist_new();
      }
      last = p2;
    }
    kvlist_append(result, kvpair_clone(p2));
  }

  if (last != NULL) {
    vals->reducer(last->key, result, vals->output);
  }

  kvlist_free(&result);
  kvlist_iterator_free(&rep);
  return NULL;
}
void init(t_struct *vals, mapper_t mapper, reducer_t reducer) {
  vals->mapper = mapper;
  vals->reducer = reducer;
  vals->input = kvlist_new();
  vals->output = kvlist_new();
}

void dist_map(kvlist_t *input, t_struct *mv, size_t num_mapper) {
  kvlist_iterator_t *rep = kvlist_iterator_new(input);
  kvpair_t *p3;
  size_t w = 0;

  while ((p3 = kvlist_iterator_next(rep)) != NULL) {
    kvlist_append(mv[w % num_mapper].input, kvpair_clone(p3));
    w++;
  }
  kvlist_iterator_free(&rep);
}

void do_map(pthread_t *mt, t_struct *mv, size_t num_mapper) {
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_create(&mt[i], NULL, map_helper, &mv[i]);
  }
}

void connect_map(pthread_t *mt, size_t num_mapper) {
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(mt[i], NULL);
  }
}

void dist_red(kvlist_t *c_list, kvlist_t **red_in, size_t num_reducer) {
  kvpair_t *p4;
  kvlist_iterator_t *rep = kvlist_iterator_new(c_list);

  while ((p4 = kvlist_iterator_next(rep)) != NULL) {
    unsigned long hashval = hash(p4->key);
    size_t red_x = hashval % num_reducer;
    kvlist_append(red_in[red_x], kvpair_clone(p4));
  }
  kvlist_iterator_free(&rep);
  kvlist_free(&c_list);
}

void do_red(pthread_t *red_t, t_struct *red_val, size_t num_reducer) {
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_create(&red_t[i], NULL, reduce_helper, &red_val[i]);
  }
}

void connect_red(pthread_t *red_t, t_struct *red_val, kvlist_t *output,
                 size_t num_reducer) {
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_join(red_t[i], NULL);

    kvlist_extend(output, red_val[i].output);
    kvlist_free(&red_val[i].output);
  }
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t *input, kvlist_t *output) {
  pthread_t *mt = malloc(num_mapper * sizeof(pthread_t));
  t_struct *mv = malloc(num_mapper * sizeof(t_struct));

  for (size_t i = 0; i < num_mapper; i++) {
    init(&mv[i], mapper, reducer);
  }

  dist_map(input, mv, num_mapper);
  do_map(mt, mv, num_mapper);
  connect_map(mt, num_mapper);
  free(mt);

  kvlist_t *c_list = kvlist_new();
  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_extend(c_list, mv[i].output);
    kvlist_free(&mv[i].input);
    kvlist_free(&mv[i].output);
  }
  free(mv);

  kvlist_t **red_in = malloc(num_reducer * sizeof(kvlist_t *));
  for (size_t i = 0; i < num_reducer; i++) {
    red_in[i] = kvlist_new();
  }
  dist_red(c_list, red_in, num_reducer);
  pthread_t *red_t = malloc(num_reducer * sizeof(pthread_t));
  t_struct *red_val = malloc(num_reducer * sizeof(t_struct));
  for (size_t i = 0; i < num_reducer; i++) {
    red_val[i].input = red_in[i];
    red_val[i].output = kvlist_new();
    red_val[i].reducer = reducer;
  }

  do_red(red_t, red_val, num_reducer);
  connect_red(red_t, red_val, output, num_reducer);

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_free(&red_in[i]);
  }
  free(red_in);
  free(red_t);
  free(red_val);
}
