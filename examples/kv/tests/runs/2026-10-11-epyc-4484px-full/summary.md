# kv bench: 2026-10-11-epyc-4484px-full

**Indicative.** full (indicative: epyc-4484px is shared with builds and other work)

- CPU: AMD EPYC 4484PX 12-Core Processor; governor powersave; 0,1,2,3,4,5,12,13,14,15,16,17 (12 threads), client cpus: 6,7,8,9,10,11,18,19,20,21,22,23, split by L3 cache (one CCD each)
- Work dir: ~/axon/kv-work on /dev/nvme1n1p2 ext4   rw,relatime,errors=remount-ro,stripe=32
- Client: go-ycsb, 64 threads, recordcount 1000000, fieldcount 10, fieldlength 100, writeallfields, readallfields, redis.datatype string, redis.max_retries 0
- Phases: 30s each; rounds: 2
- Box load (1-min average at each phase's end, 24 threads, other work included), highest per target: basic 28, stub 28, etcd-check 24, redis 19, kv-memory-1 28, redis-cluster-3 23, kv-memory-3 31, redis-aof 25, etcd-1 16, kv-durable-1 23, etcd-3 20, kv-durable-3 31, kv-durable-3-lease 27

Saturation rows are closed-loop throughput with 64 requests outstanding: their percentiles are not a latency comparison. Workload F counts each read and each write of a read-modify-write as one operation.

## Ceilings

| target | A ops/s | C ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|
| basic | 1,539,291 [1,085,096, 1,993,487] (range > 10 %) | 2,829,958 [2,483,544, 3,176,371] (range > 10 %) |  | 0.0 |  |
| stub | 208,618 [79,454, 337,781] (range > 10 %) | 212,526 [81,210, 343,842] (range > 10 %) | 90,388 [91,125, 89,652] | 1.0 |  |
| etcd-check |  | 167,905 [172,580, 163,231] |  | 1.0 |  |

## M: acked from memory

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| redis | 118,442 [120,253, 116,631] | 120,428 [121,856, 119,000] | 118,922 [122,585, 115,258] | 120,825 [117,547, 124,103] | 118,062 [114,359, 121,765] | 118,607 [120,340, 116,874] | 1.0 | stub-bound, not client-bound |
| kv-memory-1 | 224,914 [220,417, 229,410] | 213,684 [214,958, 212,411] | 219,493 [202,389, 236,597] (range > 10 %) | 202,162 [159,685, 244,640] (range > 10 %) | 230,985 [218,463, 243,508] (range > 10 %) | 23,418 [23,283, 23,554] | 1.0 | front-end-bound |
| redis-cluster-3 | 301,400 [286,064, 316,737] (range > 10 %) | 296,602 [272,212, 320,991] (range > 10 %) | 319,285 [316,817, 321,754] | 323,284 [321,213, 325,355] | 315,134 [307,066, 323,202] | 111,061 [108,075, 114,047] | 1.0 | stub-bound, not client-bound |
| kv-memory-3 | 262,353 [242,867, 281,839] (range > 10 %) | 264,921 [253,201, 276,640] | 254,388 [291,424, 217,353] (range > 10 %) | 270,195 [290,382, 250,007] (range > 10 %) | 264,936 [293,754, 236,118] (range > 10 %) | 26,029 [25,360, 26,698] | 1.0 | front-end-bound |

## S1: one local fsync

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| redis-aof | 73,941 [68,852, 79,029] (range > 10 %) | 65,022 [41,095, 88,949] (range > 10 %) | 118,545 [119,870, 117,221] | 87,230 [83,270, 91,189] | 82,244 [78,983, 85,504] | 91,447 [90,004, 92,891] | 1.0 | stub-bound, not client-bound |
| etcd-1 | 67,684 [66,960, 68,409] | 120,564 [121,932, 119,195] | 163,038 [163,701, 162,375] | 123,616 | 68,303 [58,910, 77,696] (range > 10 %) | 11,723 [11,692, 11,754] | 1.0 | etcd-driver-bound |
| kv-durable-1 | 84,418 [73,793, 95,044] (range > 10 %) | 138,550 [127,211, 149,888] (range > 10 %) | 194,809 [193,150, 196,469] | 161,549 [152,981, 170,118] (range > 10 %) | 109,105 [107,497, 110,714] | 11,503 [11,266, 11,739] | 1.0 | front-end-bound |

## R: a majority fsyncs (3 processes, one disk, loopback)

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| etcd-3 | 37,913 [36,282, 39,544] | 78,807 [78,046, 79,568] | 109,461 [103,272, 115,650] (range > 10 %) | 80,660 [80,300, 81,020] | 44,359 [43,678, 45,040] | 5,568 [5,469, 5,668] | 1.0 |  |
| kv-durable-3 | 50,345 [48,176, 52,514] | 85,761 [76,327, 95,194] (range > 10 %) | 96,705 [93,620, 99,790] | 92,523 [89,699, 95,346] | 55,136 [46,462, 63,809] (range > 10 %) | 5,620 [5,540, 5,699] | 1.0 | front-end-bound |

## R, a weaker read guarantee: kv's lease reads (alone, not compared)

| target | A ops/s | B ops/s | C ops/s | D ops/s | F ops/s | A ops per CPU-s | cmds/op | bound |
|---|---|---|---|---|---|---|---|---|
| kv-durable-3-lease | 56,570 [50,395, 62,744] (range > 10 %) | 199,570 [187,759, 211,381] (range > 10 %) | 281,552 [274,162, 288,942] | 212,095 [210,550, 213,640] | 83,560 [82,502, 84,618] | 7,035 [6,831, 7,239] | 1.0 | front-end-bound |

## Fixed rate

Coordinated omission is reduced here, not removed.

| target | workload | rate | ops/s | p50 us | p99 us | p99.9 us |
|---|---|---|---|---|---|---|
| redis | A 50 % | 60,126 | 60,121 | 278 | 2913 | 7207 |
| redis | A 80 % | 96,201 | 96,195 | 401 | 1016 | 3715 |
| redis | C 50 % | 61,292 | 61,287 | 285 | 975 | 2505 |
| redis | C 80 % | 98,068 | 98,065 | 495 | 1526 | 3441 |
| kv-memory-1 | A 50 % | 60,126 | 60,122 | 91 | 1510 | 3409 |
| kv-memory-1 | A 80 % | 96,201 | 96,166 | 159 | 1405 | 3713 |
| kv-memory-1 | C 50 % | 61,292 | 61,288 | 87 | 964 | 4127 |
| kv-memory-1 | C 80 % | 98,068 | 98,061 | 130 | 659 | 818 |
| redis-cluster-3 | A 50 % | 121,433 | 121,429 | 59 | 3771 | 6819 |
| redis-cluster-3 | A 80 % | 194,292 | 194,282 | 43 | 224 | 447 |
| redis-cluster-3 | C 50 % | 145,712 | 145,706 | 40 | 172 | 592 |
| redis-cluster-3 | C 80 % | 233,139 | 233,130 | 49 | 315 | 541 |
| kv-memory-3 | A 50 % | 121,433 | 113,734 | 204 | 4319 | 8423 |
| kv-memory-3 | A 80 % | 194,292 | 194,280 | 82 | 599 | 1183 |
| kv-memory-3 | C 50 % | 145,712 | 145,707 | 60 | 419 | 795 |
| kv-memory-3 | C 80 % | 233,139 | 233,132 | 170 | 1180 | 2883 |
| redis-aof | A 50 % | 33,480 | 33,478 | 563 | 906 | 1337 |
| redis-aof | A 80 % | 53,568 | 53,565 | 348 | 1546 | 2745 |
| redis-aof | C 50 % | 59,935 | 59,932 | 268 | 640 | 822 |
| redis-aof | C 80 % | 95,896 | 95,847 | 395 | 928 | 1131 |
| etcd-1 | A 50 % | 33,480 | 33,478 | 365 | 5847 | 9591 |
| etcd-1 | A 80 % | 53,568 | 53,564 | 477 | 7811 | 14655 |
| etcd-1 | C 50 % | 59,935 | 59,933 | 170 | 1555 | 4155 |
| etcd-1 | C 80 % | 95,896 | 95,898 | 327 | 1029 | 1896 |
| kv-durable-1 | A 50 % | 33,480 | 33,478 | 184 | 2043 | 24079 |
| kv-durable-1 | A 80 % | 53,568 | 53,565 | 210 | 2107 | 27167 |
| kv-durable-1 | C 50 % | 59,935 | 59,926 | 99 | 838 | 2623 |
| kv-durable-1 | C 80 % | 95,896 | 95,849 | 67 | 435 | 767 |
| etcd-3 | A 50 % | 18,140 | 18,138 | 619 | 4819 | 7331 |
| etcd-3 | A 80 % | 29,024 | 29,016 | 1126 | 8147 | 12567 |
| etcd-3 | C 50 % | 46,809 | 46,808 | 360 | 3025 | 7735 |
| etcd-3 | C 80 % | 74,895 | 74,894 | 458 | 2627 | 6611 |
| kv-durable-3 | A 50 % | 18,140 | 18,136 | 626 | 6459 | 38111 |
| kv-durable-3 | A 80 % | 29,024 | 29,023 | 652 | 4483 | 45343 |
| kv-durable-3 | C 50 % | 46,809 | 46,817 | 228 | 712 | 969 |
| kv-durable-3 | C 80 % | 74,895 | 74,892 | 514 | 1771 | 3969 |
| kv-durable-3-lease | A 50 % | 18,140 | 18,138 | 342 | 5119 | 37023 |
| kv-durable-3-lease | A 80 % | 29,024 | 29,022 | 302 | 5995 | 48767 |
| kv-durable-3-lease | C 50 % | 46,809 | 46,807 | 100 | 354 | 670 |
| kv-durable-3-lease | C 80 % | 74,895 | 74,892 | 102 | 491 | 1333 |
| redis | A 50 % | 58,315 | 58,313 | 229 | 584 | 696 |
| redis | A 80 % | 93,304 | 89,354 | 395 | 1882 | 5083 |
| redis | C 50 % | 57,628 | 57,625 | 338 | 3493 | 5767 |
| redis | C 80 % | 92,205 | 90,882 | 61 | 1738 | 4287 |
| kv-memory-1 | A 50 % | 58,315 | 58,309 | 144 | 4411 | 9207 |
| kv-memory-1 | A 80 % | 93,304 | 88,790 | 144 | 2889 | 6115 |
| kv-memory-1 | C 50 % | 57,628 | 57,624 | 100 | 1738 | 5043 |
| kv-memory-1 | C 80 % | 92,205 | 92,041 | 154 | 1492 | 3997 |
| redis-cluster-3 | A 50 % | 140,919 | 136,584 | 40 | 1661 | 4271 |
| redis-cluster-3 | A 80 % | 225,470 | 222,046 | 159 | 1819 | 4115 |
| redis-cluster-3 | C 50 % | 108,676 | 108,672 | 111 | 888 | 2369 |
| redis-cluster-3 | C 80 % | 173,881 | 173,876 | 43 | 941 | 4243 |
| kv-memory-3 | A 50 % | 140,919 | 140,911 | 54 | 372 | 716 |
| kv-memory-3 | A 80 % | 225,470 | 158,508 | 163 | 3833 | 6487 |
| kv-memory-3 | C 50 % | 108,676 | 108,676 | 115 | 498 | 725 |
| kv-memory-3 | C 80 % | 173,881 | 173,878 | 70 | 562 | 1175 |
| redis-aof | A 50 % | 34,204 | 34,200 | 320 | 986 | 3587 |
| redis-aof | A 80 % | 54,726 | 54,724 | 798 | 4019 | 6183 |
| redis-aof | C 50 % | 58,610 | 58,607 | 254 | 651 | 837 |
| redis-aof | C 80 % | 93,776 | 93,772 | 64 | 740 | 1483 |
| etcd-1 | A 50 % | 34,204 | 34,202 | 447 | 7395 | 15471 |
| etcd-1 | A 80 % | 54,726 | 54,724 | 486 | 8147 | 14327 |
| etcd-1 | C 50 % | 58,610 | 58,609 | 163 | 646 | 2581 |
| etcd-1 | C 80 % | 93,776 | 93,775 | 265 | 914 | 1504 |
| kv-durable-1 | A 50 % | 34,204 | 34,176 | 191 | 1492 | 24943 |
| kv-durable-1 | A 80 % | 54,726 | 54,723 | 301 | 1940 | 24719 |
| kv-durable-1 | C 50 % | 58,610 | 58,607 | 108 | 509 | 697 |
| kv-durable-1 | C 80 % | 93,776 | 93,774 | 160 | 648 | 906 |
| kv-durable-3 | A 50 % | 19,771 | 19,769 | 431 | 4267 | 35295 |
| kv-durable-3 | A 80 % | 31,634 | 31,452 | 512 | 5979 | 43263 |
| kv-durable-3 | C 50 % | 49,894 | 49,892 | 233 | 723 | 1041 |
| kv-durable-3 | C 80 % | 79,831 | 79,828 | 488 | 1173 | 1500 |
| kv-durable-3-lease | A 50 % | 19,771 | 19,768 | 253 | 3067 | 30319 |
| kv-durable-3-lease | A 80 % | 31,634 | 31,628 | 320 | 5379 | 39679 |
| kv-durable-3-lease | C 50 % | 49,894 | 49,890 | 76 | 318 | 438 |
| kv-durable-3-lease | C 80 % | 79,831 | 79,828 | 68 | 390 | 576 |
| etcd-3 | A 50 % | 19,771 | 19,769 | 746 | 4611 | 6399 |
| etcd-3 | A 80 % | 31,634 | 31,611 | 1048 | 7983 | 12295 |
| etcd-3 | C 50 % | 49,894 | 49,893 | 326 | 926 | 2491 |
| etcd-3 | C 80 % | 79,831 | 79,832 | 456 | 1277 | 3175 |

## Large values

Values stored as one go-ycsb field; B is 95 % reads, A 50 % writes. etcd runs with --max-request-bytes=33554432, compaction every 5 s, and its client patched to send 64 MiB (bench/go-ycsb.patch). go-ycsb makes and encodes every value on the client cpus: where the memory targets of one size agree, they may all be bound by the client, not the server.

| target | value | mix | ops/s | MB/s | p50 us | p99 us | errors |
|---|---|---|---|---|---|---|---|
| redis | 1 MB | read-heavy (B) | 1,637 | 1,637 | 15607 | 247935 | 0 |
| redis | 1 MB | write-heavy (A) | 1,642 | 1,642 | 20431 | 197759 | 0 |
| redis | 5 MB | read-heavy (B) | 332 | 1,659 | 177151 | 454143 | 0 |
| redis | 5 MB | write-heavy (A) | 339 | 1,696 | 129791 | 327167 | 0 |
| redis | 10 MB | read-heavy (B) | 147 | 1,468 | 428031 | 763903 | 0 |
| redis | 10 MB | write-heavy (A) | 170 | 1,705 | 288255 | 574975 | 0 |
| kv-memory-1 | 1 MB | read-heavy (B) | 1,638 | 1,638 | 15279 | 257791 | 0 |
| kv-memory-1 | 1 MB | write-heavy (A) | 1,630 | 1,630 | 20639 | 194047 | 0 |
| kv-memory-1 | 5 MB | read-heavy (B) | 324 | 1,621 | 183935 | 421119 | 0 |
| kv-memory-1 | 5 MB | write-heavy (A) | 330 | 1,651 | 129919 | 340223 | 0 |
| kv-memory-1 | 10 MB | read-heavy (B) | 160 | 1,604 | 386815 | 628223 | 0 |
| kv-memory-1 | 10 MB | write-heavy (A) | 164 | 1,642 | 267775 | 595967 | 0 |
| redis-cluster-3 | 1 MB | read-heavy (B) | 1,636 | 1,636 | 15735 | 258431 | 0 |
| redis-cluster-3 | 1 MB | write-heavy (A) | 1,639 | 1,639 | 19919 | 191231 | 0 |
| redis-cluster-3 | 5 MB | read-heavy (B) | 332 | 1,660 | 178175 | 417535 | 0 |
| redis-cluster-3 | 5 MB | write-heavy (A) | 338 | 1,688 | 127551 | 338687 | 0 |
| redis-cluster-3 | 10 MB | read-heavy (B) | 166 | 1,663 | 369919 | 621567 | 0 |
| redis-cluster-3 | 10 MB | write-heavy (A) | 170 | 1,704 | 259839 | 544767 | 0 |
| kv-memory-3 | 1 MB | read-heavy (B) | 1,633 | 1,633 | 15375 | 257919 | 0 |
| kv-memory-3 | 1 MB | write-heavy (A) | 1,628 | 1,628 | 21759 | 172415 | 0 |
| kv-memory-3 | 5 MB | read-heavy (B) | 324 | 1,620 | 181759 | 436991 | 0 |
| kv-memory-3 | 5 MB | write-heavy (A) | 329 | 1,644 | 130815 | 332287 | 0 |
| kv-memory-3 | 10 MB | read-heavy (B) | 162 | 1,615 | 383231 | 623103 | 0 |
| kv-memory-3 | 10 MB | write-heavy (A) | 164 | 1,636 | 257151 | 585215 | 0 |
| redis-aof | 1 MB | read-heavy (B) | 1,618 | 1,618 | 16783 | 234623 | 0 |
| redis-aof | 1 MB | write-heavy (A) | 814 | 814 | 74111 | 128319 | 0 |
| redis-aof | 5 MB | read-heavy (B) | 330 | 1,648 | 178303 | 439039 | 0 |
| redis-aof | 5 MB | write-heavy (A) | 113 | 564 | 531455 | 1057791 | 0 |
| redis-aof | 10 MB | read-heavy (B) | 130 | 1,305 | 480767 | 844287 | 0 |
| redis-aof | 10 MB | write-heavy (A) | 62 | 618 | 968191 | 1864703 | 0 |
| etcd-1 | 1 MB | read-heavy (B) | 1,161 | 1,161 | 49375 | 158719 | 0 |
| etcd-1 | 1 MB | write-heavy (A) | 592 | 592 | 87871 | 587263 | 0 |
| etcd-1 | 5 MB | read-heavy (B) | 204 | 1,018 | 294143 | 823295 | 0 |
| etcd-1 | 5 MB | write-heavy (A) | 121 | 604 | 442623 | 1471487 | 0 |
| etcd-1 | 10 MB | read-heavy (B) | 88 | 880 | 607743 | 3457023 | 0 |
| etcd-1 | 10 MB | write-heavy (A) | 38 | 384 | 1079295 | 7839743 | 25 |
| kv-durable-1 | 1 MB | read-heavy (B) | 1,622 | 1,622 | 17247 | 233599 | 0 |
| kv-durable-1 | 1 MB | write-heavy (A) | 592 | 592 | 78015 | 325887 | 0 |
| kv-durable-1 | 5 MB | read-heavy (B) | 319 | 1,597 | 186111 | 427263 | 0 |
| kv-durable-1 | 5 MB | write-heavy (A) | 146 | 730 | 363519 | 1360895 | 0 |
| kv-durable-1 | 10 MB | read-heavy (B) | 157 | 1,569 | 390655 | 677375 | 0 |
| kv-durable-1 | 10 MB | write-heavy (A) | 54 | 537 | 509439 | 6008831 | 203 |
| etcd-3 | 1 MB | read-heavy (B) | 1,056 | 1,056 | 46847 | 705023 | 0 |
| etcd-3 | 1 MB | write-heavy (A) | 327 | 327 | 171775 | 1225727 | 0 |
| etcd-3 | 5 MB | read-heavy (B) | 162 | 808 | 293631 | 1817599 | 0 |
| etcd-3 | 5 MB | write-heavy (A) | 20 | 101 | 1485823 | 20086783 | 63 |
| etcd-3 | 10 MB | read-heavy (B) | 63 | 632 | 622079 | 8962047 | 10 |
| etcd-3 | 10 MB | write-heavy (A) | 6 | 56 | 3291135 | 42631167 | 74 |
| kv-durable-3 | 1 MB | read-heavy (B) | 1,468 | 1,468 | 16831 | 333567 | 0 |
| kv-durable-3 | 1 MB | write-heavy (A) | 299 | 299 | 143999 | 754687 | 0 |
| kv-durable-3 | 5 MB | read-heavy (B) | 300 | 1,502 | 171775 | 765951 | 0 |
| kv-durable-3 | 5 MB | write-heavy (A) | 78 | 390 | 604671 | 3655679 | 49 |
| kv-durable-3 | 10 MB | read-heavy (B) | 146 | 1,465 | 390911 | 1172479 | 0 |
| kv-durable-3 | 10 MB | write-heavy (A) | 27 | 267 | 1161215 | 6086655 | 184 |
| kv-durable-3-lease | 1 MB | read-heavy (B) | 1,594 | 1,594 | 17487 | 309247 | 0 |
| kv-durable-3-lease | 1 MB | write-heavy (A) | 305 | 305 | 74815 | 859135 | 0 |
| kv-durable-3-lease | 5 MB | read-heavy (B) | 311 | 1,554 | 182911 | 614399 | 0 |
| kv-durable-3-lease | 5 MB | write-heavy (A) | 55 | 276 | 701439 | 4546559 | 148 |
| kv-durable-3-lease | 10 MB | read-heavy (B) | 130 | 1,300 | 294911 | 2414591 | 89 |
| kv-durable-3-lease | 10 MB | write-heavy (A) | 12 | 115 | 184831 | 7237631 | 822 |

## Sync parity

| target | sync calls | acked writes | syncs per acked write | acked writes per sync |
|---|---|---|---|---|
| redis-aof | 27,694 | 824,064 | 0.03 | 29.76 |
| etcd-1 | 74,015 | 744,830 | 0.10 | 10.06 |
| kv-durable-1 | 36,750 | 364,251 | 0.10 | 9.91 |
| etcd-3 | 262,749 | 388,641 | 0.68 | 1.48 |
| kv-durable-3 | 73,002 | 254,948 | 0.29 | 3.49 |

## Predicted against measured

Saturation ops/s from T / (u t_w + (1 - u) t_r): T the threads, u the update share (A 0.5, B 0.05), f the fio fsync p50, t_r the target's read p50 at half load (fixed-rate C 50 %), t_w = k f + t_r with k = 1, up to 2 f + h + t_r; h kv's Append to another node's Log until its answer (its hop and that Log's sync), the p50 since the node started, read at the end of A's saturation phase; 0 where not measured. Capped by the same nodes' memory target. redis-aof's reads wait for its sync. "in range" allows 10 %.

| target | workload | f us | t_r us | h us | predicted ops/s | measured ops/s | |
|---|---|---|---|---|---|---|---|
| redis-aof | A | 17 | 261 | 0 | 118,442 to 118,442 | 73,941 | below |
| redis-aof | B | 17 | 261 | 0 | 120,428 to 120,428 | 65,022 | below |
| etcd-1 | A | 17 | 166 | 0 | 349,154 to 365,923 | 67,684 | below |
| etcd-1 | B | 17 | 166 | 0 | 380,545 to 382,455 | 120,564 | below |
| kv-durable-1 | A | 17 | 104 | 0 | 224,914 to 224,914 | 84,418 | below |
| kv-durable-1 | B | 17 | 104 | 0 | 213,684 to 213,684 | 138,550 | below |
| etcd-3 | A | 16 | 343 | 0 | 178,025 to 182,206 | 37,913 | below |
| etcd-3 | B | 16 | 343 | 0 | 185,696 to 186,141 | 78,807 | below |
| kv-durable-3 | A | 17 | 230 | 524 | 125,613 to 262,353 | 50,345 | below |
| kv-durable-3 | B | 17 | 230 | 524 | 247,678 to 264,921 | 85,761 | below |
| kv-durable-3-lease | A | 16 | 88 | 655 | 148,148 to 262,353 | 56,570 | below |
| kv-durable-3-lease | B | 16 | 88 | 655 | 264,921 to 264,921 | 199,570 | below |

## Faults

fault.sh on 3 durable nodes (memory-control on memory nodes), `kv check` with 64 writers, 16 readers and a probe write to every shard each 10 ms. Windows: how long a shard took no write, p50 / max in ms, for the moves away and back; each must end within T + 3 s. Unknown: writes answered `ERR outcome unknown`, never counted lost.

| phase | verdict | acked writes | failed | unknown | lost | stale | away p50 / max | back p50 / max | note |
|---|---|---|---|---|---|---|---|---|---|
| memory-control | PASS | 1,277,846 | 172 | 23 | 0 | 214 | 4,004 / 4,017 (138) |  |  |
| kill | PASS | 218,069 | 194 | 45 | 0 | 0 | 4,028 / 4,039 (147) | 16 / 22 (9) | n2; T 6000 ms |
| stop | PASS | 267,806 | 171 | 150 | 0 | 0 | 4,117 / 4,143 (147) | 10 / 11 (5) | n2; T 6000 ms |
| stop-lease | PASS | 338,763 | 176 | 155 | 0 | 0 | 4,384 / 4,394 (147) | 18 / 104 (11) | n2; T 6000 ms |
| follower | PASS | 473,373 | 21 | 0 | 0 | 0 | 26 / 32 (57) |  | 0 written shards moved |
| restart-all | PASS | 311,307 | 0 | 0 | 0 | 0 |  |  | read back 1024 keys: lost 0; every shard answered 7 s after the starts |
| quorum-loss | PASS | 322,668 | 0 | 0 | 0 | 0 |  |  | read back 1024 keys: lost 0; two down: 0 acked of 23 writes, answers CLUSTERDOWN shard no majority of acceptors |
| memory-control | PASS | 2,192,270 | 190 | 41 | 0 | 158 | 4,003 / 4,014 (138) |  |  |
| kill | PASS | 527,213 | 194 | 45 | 0 | 0 | 4,018 / 4,035 (147) | 10 / 10 (4) | n2; T 6000 ms |
| stop | PASS | 736,518 | 173 | 152 | 0 | 0 | 4,314 / 4,316 (147) | 90 / 139 (12) | n2; T 6000 ms |
| stop-lease | PASS | 493,419 | 172 | 151 | 0 | 0 | 4,772 / 4,778 (147) | 10 / 10 (3) | n2; T 6000 ms |
| follower | PASS | 861,441 | 21 | 0 | 0 | 0 | 23 / 27 (57) |  | 0 written shards moved |
| restart-all | PASS | 320,704 | 0 | 0 | 0 | 0 |  |  | read back 1024 keys: lost 0; every shard answered 7 s after the starts |
| quorum-loss | PASS | 336,949 | 0 | 0 | 0 | 0 |  |  | read back 1024 keys: lost 0; two down: 0 acked of 23 writes, answers CLUSTERDOWN shard no majority of acceptors |
| soak | PASS | 48,123,730 | 1,092 |  | 0 | 0 |  |  | 60 min at 7,775 ops/s; 16 of 16 checks passed; WAL at most 1,536 MiB; RSS most 18,467 then 19,379 MiB |

## Rows with errors (left out above)

| target | workload | phase | ops_ok | errors |
|---|---|---|---|---|
| etcd-1 | workloadd | sat | 2423314 | 1836017 |
| etcd-1 | workloada | large10mb | 1155 | 25 |
| kv-durable-1 | workloada | large10mb | 1809 | 203 |
| etcd-3 | workloada | large5mb | 785 | 63 |
| etcd-3 | workloadb | large10mb | 1912 | 10 |
| etcd-3 | workloada | large10mb | 326 | 74 |
| kv-durable-3 | workloada | large5mb | 2472 | 49 |
| kv-durable-3 | workloada | large10mb | 905 | 184 |
| kv-durable-3-lease | workloada | large5mb | 1801 | 148 |
| kv-durable-3-lease | workloada | load | 61 | 3 |
| kv-durable-3-lease | workloadb | large10mb | 4108 | 89 |
| kv-durable-3-lease | workloada | large10mb | 398 | 822 |
