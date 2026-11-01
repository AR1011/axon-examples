# kv bench: 2026-10-13-build-1-full

**Indicative.** full (indicative: nothing else; first full load after the PSU swap)

- CPU: AMD Ryzen 9 9900X 12-Core Processor; governor performance; 0,1,2,3,4,5,12,13,14,15,16,17 (12 threads), client cpus: 6,7,8,9,10,11,18,19,20,21,22,23, split by L3 cache (one CCD each)
- Work dir: /data/ubuntu/kv-work on /dev/md4 ext4   rw,noatime,stripe=256
- Client: go-ycsb, 64 threads, recordcount 1000000, fieldcount 10, fieldlength 100, writeallfields, readallfields, redis.datatype string, redis.max_retries 0
- Phases: 60s each; rounds: 3
- Box load (1-min average at each phase's end, 24 threads, other work included), highest per target: basic 12, stub 13, etcd-check 10, redis 14, redis-io-threads 15, kv-memory-1 24, redis-cluster-3 18, kv-memory-3 28, redis-aof 15, etcd-1 13, kv-durable-1 19, etcd-3 12, kv-durable-3 30, kv-durable-3-lease 27

Saturation rows are closed-loop throughput with 64 requests outstanding: their percentiles are not a latency comparison. Workload F counts each read and each write of a read-modify-write as one operation.

## Ceilings

| target | A ops/s | C ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|
| basic | 2,018,877 [2,019,159, 2,018,877, 2,013,657] | 2,918,670 [3,130,081, 2,890,605, 2,918,670] |  | 0.0 |  |
| stub | 465,593 [465,593, 467,910, 464,889] | 469,398 [470,335, 469,398, 467,974] | 159,186 [159,950, 159,055, 159,186] | 1.0 |  |
| etcd-check |  | 178,955 [178,955, 178,639, 179,184] |  | 1.0 |  |

## M: acked from memory

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| redis | 202,749 [203,971, 202,502, 202,749] | 205,859 [205,914, 203,969, 205,859] | 206,540 [206,540, 204,284, 206,916] | 224,116 [224,817, 219,820, 224,116] | 213,074 [213,431, 212,357, 213,074] | 202,784 [203,937, 202,469, 202,784] | 1.0 |  |
| redis-io-threads | 392,661 [390,841, 392,661, 393,028] | 404,548 [404,548, 402,310, 404,573] | 408,631 [409,430, 408,631, 408,234] | 446,111 [446,766, 446,111, 444,161] | 416,318 [416,318, 414,976, 416,563] | 98,619 [98,140, 98,619, 98,722] | 1.0 |  |
| kv-memory-1 | 307,894 [307,894, 307,255, 308,122] | 310,573 [310,103, 310,573, 311,316] | 314,694 [313,334, 314,694, 315,257] | 313,585 [313,433, 313,928, 313,585] | 310,844 [310,844, 310,388, 311,274] | 30,750 [30,712, 30,750, 30,772] | 1.0 |  |
| redis-cluster-3 | 446,878 [449,699, 446,347, 446,878] | 452,564 [451,134, 453,872, 452,564] | 455,648 [455,648, 457,503, 454,835] | 457,327 [455,664, 457,691, 457,327] | 451,266 [452,759, 442,872, 451,266] | 179,856 [180,231, 179,234, 179,856] | 1.0 |  |
| kv-memory-3 | 391,588 [392,389, 391,588, 391,282] | 393,224 [393,870, 393,087, 393,224] | 397,962 [398,429, 397,880, 397,962] | 390,076 [390,699, 390,076, 389,830] | 400,440 [400,750, 400,440, 400,377] | 36,696 [36,743, 36,696, 36,666] | 1.0 | front-end-bound |

## S1: one local fsync

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| redis-aof | 12,791 [12,728, 12,791, 12,793] | 14,247 [14,061, 14,247, 14,275] | 206,774 [208,524, 206,774, 202,810] | 14,335 [14,288, 14,338, 14,335] | 12,944 [12,947, 12,930, 12,944] | 159,602 [160,097, 159,571, 159,602] | 1.0 |  |
| etcd-1 | 7,356 [7,304, 7,382, 7,356] | 11,841 [11,809, 11,948, 11,841] | 156,429 [155,683, 156,429, 156,742] | 12,238 [12,235, 12,293, 12,238] | 7,931 [7,919, 7,931, 7,945] | 16,982 [16,982, 16,907, 17,009] | 1.0 | etcd-driver-bound |
| kv-durable-1 | 26,638 [26,463, 26,788, 26,638] | 138,634 [137,240, 138,634, 138,990] | 248,536 [248,536, 248,796, 248,135] | 134,844 [134,652, 134,844, 135,363] | 39,580 [39,580, 39,571, 39,909] | 10,216 [10,216, 10,287, 10,136] | 1.0 |  |

## R: a majority fsyncs (3 processes, one disk, loopback)

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| etcd-3 | 1,473 [1,454, 1,473, 1,488] | 4,533 [4,533, 4,527, 4,535] | 130,314 [130,867, 130,314, 129,740] | 4,978 [4,921, 5,012, 4,978] | 1,640 [1,630, 1,640, 1,644] | 5,972 [5,972, 5,951, 6,707] (range > 10 %) | 1.0 |  |
| kv-durable-3 | 15,552 [15,288, 15,555, 15,552] | 76,339 [76,339, 75,468, 77,011] | 126,457 [126,372, 126,641, 126,457] | 74,009 [74,009, 73,793, 74,889] | 23,234 [23,225, 23,234, 23,508] | 3,541 [3,524, 3,541, 3,548] | 1.0 |  |

## R, a weaker read guarantee: kv's lease reads (alone, not compared)

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| kv-durable-3-lease | 16,782 [16,724, 16,782, 16,862] | 146,856 [146,210, 146,856, 148,631] | 391,699 [391,699, 392,046, 391,324] | 152,474 [151,159, 152,474, 155,593] | 25,372 [25,372, 25,349, 25,876] | 4,832 [4,832, 4,844, 4,831] | 1.0 | front-end-bound |

## Fixed rate

Coordinated omission is reduced here, not removed.

| target | workload | rate | ops/s | p50 us | p99 us | p99.9 us |
|---|---|---|---|---|---|---|
| redis | A 50 % | 101,985 | 101,984 | 198 | 512 | 566 |
| redis | A 80 % | 163,176 | 163,173 | 46 | 362 | 584 |
| redis | C 50 % | 103,270 | 103,269 | 215 | 532 | 584 |
| redis | C 80 % | 165,232 | 165,226 | 254 | 579 | 610 |
| redis-io-threads | A 50 % | 101,985 | 101,983 | 104 | 304 | 414 |
| redis-io-threads | A 80 % | 163,176 | 163,174 | 34 | 121 | 224 |
| redis-io-threads | C 50 % | 103,270 | 103,268 | 115 | 300 | 430 |
| redis-io-threads | C 80 % | 165,232 | 165,228 | 31 | 107 | 197 |
| kv-memory-1 | A 50 % | 101,985 | 101,981 | 103 | 401 | 524 |
| kv-memory-1 | A 80 % | 163,176 | 163,173 | 44 | 214 | 399 |
| kv-memory-1 | C 50 % | 103,270 | 103,269 | 89 | 391 | 508 |
| kv-memory-1 | C 80 % | 165,232 | 165,229 | 43 | 190 | 380 |
| redis-cluster-3 | A 50 % | 196,194 | 196,190 | 35 | 345 | 603 |
| redis-cluster-3 | A 80 % | 313,911 | 313,907 | 30 | 311 | 537 |
| redis-cluster-3 | C 50 % | 199,214 | 199,211 | 76 | 376 | 615 |
| redis-cluster-3 | C 80 % | 318,743 | 318,737 | 30 | 271 | 473 |
| kv-memory-3 | A 50 % | 196,194 | 196,190 | 43 | 376 | 581 |
| kv-memory-3 | A 80 % | 313,911 | 313,903 | 80 | 551 | 833 |
| kv-memory-3 | C 50 % | 199,214 | 199,208 | 71 | 465 | 674 |
| kv-memory-3 | C 80 % | 318,743 | 318,742 | 81 | 546 | 826 |
| redis-aof | A 50 % | 3,652 | 3,651 | 6019 | 8791 | 13823 |
| redis-aof | A 80 % | 5,843 | 5,842 | 5775 | 8687 | 11703 |
| redis-aof | C 50 % | 77,841 | 77,840 | 195 | 463 | 547 |
| redis-aof | C 80 % | 124,545 | 124,542 | 194 | 556 | 600 |
| etcd-1 | A 50 % | 3,652 | 3,651 | 7163 | 20047 | 27471 |
| etcd-1 | A 80 % | 5,843 | 5,842 | 7239 | 19615 | 28079 |
| etcd-1 | C 50 % | 77,841 | 77,835 | 312 | 1338 | 3969 |
| etcd-1 | C 80 % | 124,545 | 124,544 | 313 | 3333 | 5287 |
| kv-durable-1 | A 50 % | 3,652 | 3,651 | 1456 | 4971 | 8887 |
| kv-durable-1 | A 80 % | 5,843 | 5,842 | 1464 | 5675 | 9367 |
| kv-durable-1 | C 50 % | 77,841 | 77,839 | 131 | 458 | 614 |
| kv-durable-1 | C 80 % | 124,545 | 124,543 | 160 | 531 | 912 |
| etcd-3 | A 50 % | 726 | 725 | 29695 | 72255 | 83135 |
| etcd-3 | A 80 % | 1,162 | 1,161 | 39455 | 86335 | 100159 |
| etcd-3 | C 50 % | 63,185 | 63,184 | 271 | 907 | 3605 |
| etcd-3 | C 80 % | 101,096 | 101,095 | 406 | 1162 | 2805 |
| kv-durable-3 | A 50 % | 726 | 725 | 1701 | 7315 | 16495 |
| kv-durable-3 | A 80 % | 1,162 | 1,161 | 1766 | 7379 | 13023 |
| kv-durable-3 | C 50 % | 63,185 | 63,177 | 226 | 676 | 928 |
| kv-durable-3 | C 80 % | 101,096 | 101,094 | 388 | 970 | 1247 |
| kv-durable-3-lease | A 50 % | 726 | 725 | 1700 | 7143 | 12287 |
| kv-durable-3-lease | A 80 % | 1,162 | 1,161 | 91 | 7515 | 19839 |
| kv-durable-3-lease | C 50 % | 63,185 | 63,183 | 63 | 294 | 396 |
| kv-durable-3-lease | C 80 % | 101,096 | 101,096 | 80 | 362 | 508 |
| redis | A 50 % | 101,250 | 101,248 | 214 | 514 | 573 |
| redis | A 80 % | 162,000 | 161,997 | 55 | 535 | 606 |
| redis | C 50 % | 102,142 | 102,139 | 206 | 525 | 574 |
| redis | C 80 % | 163,427 | 163,423 | 217 | 574 | 602 |
| redis-io-threads | A 50 % | 101,250 | 101,249 | 110 | 311 | 419 |
| redis-io-threads | A 80 % | 162,000 | 161,996 | 119 | 349 | 496 |
| redis-io-threads | C 50 % | 102,142 | 102,140 | 116 | 309 | 434 |
| redis-io-threads | C 80 % | 163,427 | 163,391 | 37 | 127 | 224 |
| kv-memory-1 | A 50 % | 101,250 | 101,247 | 113 | 402 | 517 |
| kv-memory-1 | A 80 % | 162,000 | 161,998 | 42 | 198 | 380 |
| kv-memory-1 | C 50 % | 102,142 | 102,141 | 106 | 389 | 512 |
| kv-memory-1 | C 80 % | 163,427 | 163,409 | 124 | 422 | 595 |
| redis-cluster-3 | A 50 % | 195,793 | 195,789 | 38 | 341 | 591 |
| redis-cluster-3 | A 80 % | 313,269 | 313,264 | 28 | 293 | 511 |
| redis-cluster-3 | C 50 % | 198,940 | 198,936 | 73 | 354 | 586 |
| redis-cluster-3 | C 80 % | 318,304 | 318,299 | 32 | 280 | 487 |
| kv-memory-3 | A 50 % | 195,793 | 195,756 | 43 | 387 | 614 |
| kv-memory-3 | A 80 % | 313,269 | 313,260 | 79 | 555 | 841 |
| kv-memory-3 | C 50 % | 198,940 | 198,936 | 64 | 457 | 672 |
| kv-memory-3 | C 80 % | 318,304 | 318,302 | 81 | 547 | 822 |
| redis-aof | A 50 % | 3,691 | 3,690 | 5635 | 8727 | 13839 |
| redis-aof | A 80 % | 5,905 | 5,904 | 5743 | 8727 | 11543 |
| redis-aof | C 50 % | 78,214 | 78,212 | 201 | 480 | 550 |
| redis-aof | C 80 % | 125,143 | 125,140 | 253 | 570 | 600 |
| etcd-1 | A 50 % | 3,691 | 3,690 | 7199 | 19519 | 25439 |
| etcd-1 | A 80 % | 5,905 | 5,904 | 7279 | 19503 | 26863 |
| etcd-1 | C 50 % | 78,214 | 78,214 | 314 | 1356 | 4013 |
| etcd-1 | C 80 % | 125,143 | 125,142 | 314 | 3335 | 5195 |
| kv-durable-1 | A 50 % | 3,691 | 3,690 | 1448 | 4803 | 7779 |
| kv-durable-1 | A 80 % | 5,905 | 5,904 | 1455 | 5647 | 9871 |
| kv-durable-1 | C 50 % | 78,214 | 78,212 | 128 | 454 | 614 |
| kv-durable-1 | C 80 % | 125,143 | 125,141 | 115 | 509 | 888 |
| etcd-3 | A 50 % | 736 | 735 | 30799 | 71935 | 83967 |
| etcd-3 | A 80 % | 1,178 | 1,177 | 38591 | 84671 | 98495 |
| etcd-3 | C 50 % | 63,320 | 63,319 | 271 | 1005 | 3319 |
| etcd-3 | C 80 % | 101,312 | 101,311 | 411 | 1210 | 3311 |
| kv-durable-3 | A 50 % | 736 | 735 | 599 | 7115 | 11391 |
| kv-durable-3 | A 80 % | 1,178 | 1,177 | 1806 | 7431 | 15183 |
| kv-durable-3 | C 50 % | 63,320 | 63,314 | 220 | 689 | 929 |
| kv-durable-3 | C 80 % | 101,312 | 101,310 | 382 | 971 | 1245 |
| kv-durable-3-lease | A 50 % | 736 | 735 | 1684 | 7055 | 14839 |
| kv-durable-3-lease | A 80 % | 1,178 | 1,177 | 1999 | 6835 | 12143 |
| kv-durable-3-lease | C 50 % | 63,320 | 63,318 | 68 | 296 | 397 |
| kv-durable-3-lease | C 80 % | 101,312 | 101,309 | 69 | 359 | 498 |
| redis | A 50 % | 101,374 | 101,372 | 205 | 506 | 563 |
| redis | A 80 % | 162,199 | 162,195 | 250 | 575 | 610 |
| redis | C 50 % | 103,457 | 103,455 | 203 | 523 | 566 |
| redis | C 80 % | 165,532 | 165,530 | 268 | 578 | 611 |
| redis-io-threads | A 50 % | 101,374 | 101,372 | 96 | 310 | 423 |
| redis-io-threads | A 80 % | 162,199 | 162,196 | 103 | 357 | 510 |
| redis-io-threads | C 50 % | 103,457 | 103,455 | 43 | 262 | 422 |
| redis-io-threads | C 80 % | 165,532 | 165,531 | 35 | 110 | 191 |
| kv-memory-1 | A 50 % | 101,374 | 101,373 | 96 | 396 | 508 |
| kv-memory-1 | A 80 % | 162,199 | 162,196 | 113 | 432 | 605 |
| kv-memory-1 | C 50 % | 103,457 | 103,456 | 93 | 391 | 513 |
| kv-memory-1 | C 80 % | 165,532 | 165,529 | 42 | 179 | 365 |
| redis-cluster-3 | A 50 % | 195,641 | 195,638 | 23 | 107 | 223 |
| redis-cluster-3 | A 80 % | 313,025 | 313,016 | 29 | 305 | 527 |
| redis-cluster-3 | C 50 % | 198,980 | 198,976 | 30 | 285 | 534 |
| redis-cluster-3 | C 80 % | 318,368 | 318,362 | 30 | 272 | 482 |
| kv-memory-3 | A 50 % | 195,641 | 195,638 | 42 | 361 | 580 |
| kv-memory-3 | A 80 % | 313,025 | 313,017 | 79 | 551 | 829 |
| kv-memory-3 | C 50 % | 198,980 | 198,979 | 45 | 386 | 602 |
| kv-memory-3 | C 80 % | 318,368 | 318,366 | 80 | 544 | 825 |
| redis-aof | A 50 % | 3,678 | 3,677 | 5711 | 8775 | 12735 |
| redis-aof | A 80 % | 5,884 | 5,883 | 5743 | 8679 | 11543 |
| redis-aof | C 50 % | 78,371 | 78,369 | 192 | 457 | 532 |
| redis-aof | C 80 % | 125,393 | 125,391 | 234 | 563 | 640 |
| etcd-1 | A 50 % | 3,678 | 3,677 | 7163 | 19903 | 27247 |
| etcd-1 | A 80 % | 5,884 | 5,883 | 7247 | 19439 | 26671 |
| etcd-1 | C 50 % | 78,371 | 78,368 | 311 | 1297 | 4081 |
| etcd-1 | C 80 % | 125,393 | 125,394 | 314 | 3417 | 5355 |
| kv-durable-1 | A 50 % | 3,678 | 3,677 | 1432 | 4823 | 8335 |
| kv-durable-1 | A 80 % | 5,884 | 5,883 | 1460 | 5651 | 10151 |
| kv-durable-1 | C 50 % | 78,371 | 78,369 | 118 | 451 | 614 |
| kv-durable-1 | C 80 % | 125,393 | 125,392 | 111 | 498 | 884 |
| etcd-3 | A 50 % | 743 | 742 | 31183 | 73855 | 85247 |
| etcd-3 | A 80 % | 1,189 | 1,188 | 38815 | 84735 | 97343 |
| etcd-3 | C 50 % | 63,228 | 63,227 | 271 | 860 | 2711 |
| etcd-3 | C 80 % | 101,165 | 101,157 | 408 | 1193 | 2907 |
| kv-durable-3 | A 50 % | 743 | 742 | 297 | 7075 | 12439 |
| kv-durable-3 | A 80 % | 1,189 | 1,188 | 2209 | 7575 | 13695 |
| kv-durable-3 | C 50 % | 63,228 | 63,227 | 201 | 674 | 936 |
| kv-durable-3 | C 80 % | 101,165 | 101,165 | 390 | 982 | 1269 |
| kv-durable-3-lease | A 50 % | 743 | 742 | 229 | 7183 | 13751 |
| kv-durable-3-lease | A 80 % | 1,189 | 1,188 | 1700 | 7739 | 20111 |
| kv-durable-3-lease | C 50 % | 63,228 | 63,226 | 70 | 302 | 393 |
| kv-durable-3-lease | C 80 % | 101,165 | 101,164 | 74 | 356 | 491 |

## Large values

Values stored as one go-ycsb field; B is 95 % reads, A 50 % writes. etcd runs with --max-request-bytes=33554432, compaction every 5 s, and its client patched to send 64 MiB (bench/go-ycsb.patch). go-ycsb makes and encodes every value on the client cpus: where the memory targets of one size agree, they may all be bound by the client, not the server.

| target | value | mix | ops/s | MB/s | p50 us | p99 us | errors |
|---|---|---|---|---|---|---|---|
| redis | 1 MB | read-heavy (B) | 1,919 | 1,919 | 12567 | 232319 | 0 |
| redis | 1 MB | write-heavy (A) | 1,928 | 1,928 | 16031 | 197119 | 0 |
| redis | 5 MB | read-heavy (B) | 393 | 1,964 | 147071 | 429055 | 0 |
| redis | 5 MB | write-heavy (A) | 396 | 1,978 | 109695 | 304895 | 0 |
| redis | 10 MB | read-heavy (B) | 199 | 1,988 | 307711 | 521471 | 0 |
| redis | 10 MB | write-heavy (A) | 200 | 2,004 | 227967 | 471551 | 0 |
| redis-io-threads | 1 MB | read-heavy (B) | 1,919 | 1,919 | 12463 | 235775 | 0 |
| redis-io-threads | 1 MB | write-heavy (A) | 1,928 | 1,928 | 16415 | 186367 | 0 |
| redis-io-threads | 5 MB | read-heavy (B) | 393 | 1,966 | 147583 | 415231 | 0 |
| redis-io-threads | 5 MB | write-heavy (A) | 396 | 1,978 | 110527 | 297983 | 0 |
| redis-io-threads | 10 MB | read-heavy (B) | 199 | 1,989 | 309247 | 526335 | 0 |
| redis-io-threads | 10 MB | write-heavy (A) | 200 | 2,004 | 223359 | 469759 | 0 |
| kv-memory-1 | 1 MB | read-heavy (B) | 1,908 | 1,908 | 12311 | 236927 | 0 |
| kv-memory-1 | 1 MB | write-heavy (A) | 1,904 | 1,904 | 16151 | 196863 | 0 |
| kv-memory-1 | 5 MB | read-heavy (B) | 379 | 1,894 | 153471 | 413183 | 0 |
| kv-memory-1 | 5 MB | write-heavy (A) | 380 | 1,898 | 115071 | 300543 | 0 |
| kv-memory-1 | 10 MB | read-heavy (B) | 185 | 1,848 | 333823 | 549887 | 0 |
| kv-memory-1 | 10 MB | write-heavy (A) | 188 | 1,878 | 237951 | 516095 | 0 |
| redis-cluster-3 | 1 MB | read-heavy (B) | 1,915 | 1,915 | 12591 | 237567 | 0 |
| redis-cluster-3 | 1 MB | write-heavy (A) | 1,919 | 1,919 | 16183 | 190463 | 0 |
| redis-cluster-3 | 5 MB | read-heavy (B) | 392 | 1,960 | 149631 | 388863 | 0 |
| redis-cluster-3 | 5 MB | write-heavy (A) | 394 | 1,970 | 108863 | 313855 | 0 |
| redis-cluster-3 | 10 MB | read-heavy (B) | 197 | 1,974 | 311807 | 521215 | 0 |
| redis-cluster-3 | 10 MB | write-heavy (A) | 200 | 1,995 | 219519 | 497663 | 0 |
| kv-memory-3 | 1 MB | read-heavy (B) | 1,906 | 1,906 | 12671 | 232063 | 0 |
| kv-memory-3 | 1 MB | write-heavy (A) | 1,901 | 1,901 | 17183 | 182911 | 0 |
| kv-memory-3 | 5 MB | read-heavy (B) | 380 | 1,900 | 155135 | 375039 | 0 |
| kv-memory-3 | 5 MB | write-heavy (A) | 382 | 1,908 | 113471 | 310783 | 0 |
| kv-memory-3 | 10 MB | read-heavy (B) | 187 | 1,868 | 329983 | 560127 | 0 |
| kv-memory-3 | 10 MB | write-heavy (A) | 189 | 1,890 | 230655 | 525823 | 0 |
| redis-aof | 1 MB | read-heavy (B) | 1,908 | 1,908 | 18047 | 157567 | 0 |
| redis-aof | 1 MB | write-heavy (A) | 1,370 | 1,370 | 42783 | 82623 | 0 |
| redis-aof | 5 MB | read-heavy (B) | 391 | 1,956 | 148735 | 396031 | 0 |
| redis-aof | 5 MB | write-heavy (A) | 193 | 967 | 301055 | 607231 | 0 |
| redis-aof | 10 MB | read-heavy (B) | 196 | 1,957 | 311551 | 565247 | 0 |
| redis-aof | 10 MB | write-heavy (A) | 117 | 1,171 | 486911 | 1008127 | 0 |
| etcd-1 | 1 MB | read-heavy (B) | 1,380 | 1,380 | 42559 | 107007 | 0 |
| etcd-1 | 1 MB | write-heavy (A) | 733 | 733 | 74623 | 514815 | 0 |
| etcd-1 | 5 MB | read-heavy (B) | 266 | 1,329 | 229631 | 554495 | 0 |
| etcd-1 | 5 MB | write-heavy (A) | 112 | 559 | 350207 | 3522559 | 55 |
| etcd-1 | 10 MB | read-heavy (B) | 109 | 1,094 | 467711 | 4788223 | 0 |
| etcd-1 | 10 MB | write-heavy (A) | 16 | 159 | 655359 | 20381695 | 161 |
| kv-durable-1 | 1 MB | read-heavy (B) | 1,897 | 1,897 | 13975 | 216319 | 0 |
| kv-durable-1 | 1 MB | write-heavy (A) | 1,025 | 1,025 | 54911 | 177535 | 0 |
| kv-durable-1 | 5 MB | read-heavy (B) | 371 | 1,855 | 157055 | 412415 | 0 |
| kv-durable-1 | 5 MB | write-heavy (A) | 184 | 920 | 290815 | 1092607 | 0 |
| kv-durable-1 | 10 MB | read-heavy (B) | 180 | 1,802 | 339199 | 587775 | 0 |
| kv-durable-1 | 10 MB | write-heavy (A) | 88 | 881 | 601087 | 2367487 | 5 |
| etcd-3 | 1 MB | read-heavy (B) | 1,250 | 1,250 | 43455 | 169599 | 0 |
| etcd-3 | 1 MB | write-heavy (A) | 190 | 190 | 203647 | 3334143 | 0 |
| etcd-3 | 5 MB | read-heavy (B) | 174 | 870 | 240895 | 3315711 | 0 |
| etcd-3 | 5 MB | write-heavy (A) | 10 | 50 | 2193407 | 19808255 | 137 |
| etcd-3 | 10 MB | read-heavy (B) | 2 | 20 | 32260095 | 65667071 | 6 |
| etcd-3 | 10 MB | write-heavy (A) | 2 | 20 | 8437759 | 85393407 | 116 |
| kv-durable-3 | 1 MB | read-heavy (B) | 1,865 | 1,865 | 15231 | 207487 | 0 |
| kv-durable-3 | 1 MB | write-heavy (A) | 697 | 697 | 87423 | 305919 | 0 |
| kv-durable-3 | 5 MB | read-heavy (B) | 352 | 1,760 | 163839 | 445439 | 0 |
| kv-durable-3 | 5 MB | write-heavy (A) | 142 | 708 | 392191 | 1504255 | 0 |
| kv-durable-3 | 10 MB | read-heavy (B) | 170 | 1,697 | 355583 | 714239 | 0 |
| kv-durable-3 | 10 MB | write-heavy (A) | 65 | 654 | 688639 | 4198399 | 131 |
| kv-durable-3-lease | 1 MB | read-heavy (B) | 1,866 | 1,866 | 14791 | 217471 | 0 |
| kv-durable-3-lease | 1 MB | write-heavy (A) | 616 | 616 | 82687 | 409087 | 0 |
| kv-durable-3-lease | 5 MB | read-heavy (B) | 357 | 1,787 | 163455 | 391167 | 0 |
| kv-durable-3-lease | 5 MB | write-heavy (A) | 116 | 579 | 202751 | 2607103 | 331 |
| kv-durable-3-lease | 10 MB | read-heavy (B) | 171 | 1,711 | 345087 | 913919 | 0 |
| kv-durable-3-lease | 10 MB | write-heavy (A) | 46 | 464 | 529919 | 4706303 | 712 |

## Sync parity

| target | sync calls | acked writes | syncs per acked write | acked writes per sync |
|---|---|---|---|---|
| redis-aof | 4,566 | 138,296 | 0.03 | 30.29 |
| etcd-1 | 5,876 | 95,065 | 0.06 | 16.18 |
| kv-durable-1 | 11,685 | 260,702 | 0.04 | 22.31 |
| etcd-3 | 13,651 | 34,595 | 0.39 | 2.53 |
| kv-durable-3 | 20,360 | 160,051 | 0.13 | 7.86 |

## Predicted against measured

Saturation ops/s from T / (u t_w + (1 - u) t_r): T the threads, u the update share (A 0.5, B 0.05), f the fio fsync p50, t_r the target's read p50 at half load (fixed-rate C 50 %), t_w = k f + t_r with k = 1, up to 2 f + h + t_r; h kv's Append to another node's Log until its answer (its hop and that Log's sync), the p50 since the node started, read at the end of A's saturation phase; 0 where not measured. Capped by the same nodes' memory target. redis-aof's reads wait for its sync. "in range" allows 10 %.

| target | workload | f us | t_r us | h us | predicted ops/s | measured ops/s | |
|---|---|---|---|---|---|---|---|
| redis-aof | A | 4,227 | 195 | 0 | 7,400 to 14,473 | 12,791 | in range |
| redis-aof | B | 4,227 | 195 | 0 | 7,400 to 14,473 | 14,247 | in range |
| etcd-1 | A | 4,227 | 312 | 0 | 14,100 to 26,386 | 7,356 | below |
| etcd-1 | B | 4,227 | 312 | 0 | 87,109 to 122,288 | 11,841 | below |
| kv-durable-1 | A | 4,227 | 128 | 0 | 14,695 to 28,552 | 26,638 | in range |
| kv-durable-1 | B | 4,227 | 128 | 0 | 116,214 to 188,593 | 138,634 | in range |
| etcd-3 | A | 4,227 | 271 | 0 | 14,228 to 26,839 | 1,473 | below |
| etcd-3 | B | 4,227 | 271 | 0 | 92,258 to 132,682 | 4,533 | below |
| kv-durable-3 | A | 4,227 | 220 | 425 | 13,735 to 27,426 | 15,552 | in range |
| kv-durable-3 | B | 4,227 | 220 | 425 | 96,391 to 148,370 | 76,339 | below |
| kv-durable-3-lease | A | 4,227 | 68 | 3,932 | 10,222 to 29,337 | 16,782 | in range |
| kv-durable-3-lease | B | 4,227 | 68 | 3,932 | 93,117 to 229,099 | 146,856 | in range |

## Rows with errors (left out above)

| target | workload | phase | ops_ok | errors |
|---|---|---|---|---|
| etcd-1 | workloada | large5mb | 6711 | 55 |
| etcd-1 | workloada | large10mb | 998 | 161 |
| kv-durable-1 | workloada | large10mb | 5307 | 5 |
| etcd-3 | workloada | large5mb | 691 | 137 |
| etcd-3 | workloada | load | 0 | 64 |
| etcd-3 | workloadb | large10mb | 180 | 6 |
| etcd-3 | workloada | large10mb | 241 | 116 |
| kv-durable-3 | workloada | large10mb | 4046 | 131 |
| kv-durable-3-lease | workloada | large5mb | 7439 | 331 |
| kv-durable-3-lease | workloada | large10mb | 3004 | 712 |
