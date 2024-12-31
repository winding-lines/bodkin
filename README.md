# Bodkin

A Rust proc-macro to allow easier integration of Apache Arrow in rust projects.

One of meanings of bodkin is "arrow tip", bodkin is interfacing your application to the rest of arrow.

<img src="640px-Bodkin1.jpg" alt="Courtesy of Wikipedia.">

## Overview

Apache Arrow is a high performance in memory data representation. Arrow is
columnar based and code is frequently row based. This project assumes that the
user defines Rust structs for the row representation. It will provide 
proc macros to derive the batch representation as a struct of arrow arrays.

Bodkin will generate the pyarrow schema from the row definitions and assume the parquet/lancedb files will use the generated schema. Each `Row Struct` will define its own schema and be saved in one file.
