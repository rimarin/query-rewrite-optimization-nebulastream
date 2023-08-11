# Query Rewrite Optimization for NebulaStream

In database systems, the query optimizer can drastically improve the query execution time. Through the transformation of the input queries, it produces a faster, optimal query plan. A fundamental part of this component is the query rewrite: it applies several rules to change, add, delete operators from the query plan and gener-
ate another one, which is optimized and semantically equivalent. Such techniques are also applicable in stream processing systems. Namely, this project focuses on NebulaStream, a data management platform for IoT. We implement rules that change the structure of queries in the NebulaStream system, before they get compiled and executed. By using these rewrite rules, the size of intermediate results during the query execution is reduced. In particular, our rules push down, reorder, split and merge filter operators and eliminate possible duplicates and redundancies. We conduct benchmarks with the application the new rules. Our evaluation shows that - de-
pending on the rule - the throughput of the system is increased and therefore NebulaStream can benefit from the rewrite optimizations.

Implemented rules:
- Filter Split Up
- Filter Push Down
- Filter Merge
- Filter Reordering
- Redundancy Elimination

Complete access to NebulaStream source code can be requested here: https://docs.google.com/forms/d/e/1FAIpQLSfPMLql4SXN_Y8B-jJUjlvyuiLkOBcXLPKn1BUqSnkAcY51rg/viewform

