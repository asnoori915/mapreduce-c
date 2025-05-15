# MapReduce in C

This project implements a basic MapReduce-style pipeline in C to simulate distributed data processing. 

## Overview

The pipeline follows the core stages of:
- **Map:** Process input data and generate key-value pairs.
- **Shuffle:** Group key-value pairs by key (simulated in-memory).
- **Reduce:** Aggregate results to generate final output.

## Features

- Custom hash table for intermediate key-value storage
- Dynamic memory-safe data structures
- Simulates parallel input processing
- Includes test cases and sample input files

## Usage

To compile:

```bash
make
```

To run:

```bash
./mapreduce input.txt
```

## Files

- `main.c`: Entry point for the pipeline
- `mapreduce.c/.h`: Core Map, Shuffle, Reduce logic
- `kv_store.c/.h`: In-memory key-value store implementation
- `Makefile`: Build instructions

## Author

Amir Noori  
UC Santa Cruz, CSE 130
