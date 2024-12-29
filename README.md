= Bodkin

A Rust proc-macro to allow easier integration of Apache Arrow in rust projects.

== Overview

Apache Arrow is a high performance in memory data representation. Arrow is
columnar based and code is frequently row based. This project assumes that the
user defines Rust structs for the row representation. It will provide 
proc macros to derive the batch representation as a struct of arrow arrays.
