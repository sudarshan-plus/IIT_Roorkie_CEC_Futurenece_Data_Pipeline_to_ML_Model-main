### Q1a: Daily net revenue by ZONE (delivered orders only, after discount)

```sql
SELECT order_date, zone, sum(delivered_orders) AS orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY order_date, zone
ORDER BY order_date, zone;
```

| order_date | zone | orders | net_revenue |
|---|---|---|---|
| 2026-09-01 | East | 19 | 10192.85 |
| 2026-09-01 | North | 16 | 9217.15 |
| 2026-09-01 | South | 9 | 4718.30 |
| 2026-09-01 | UNKNOWN | 9 | 5844.75 |
| 2026-09-01 | West | 14 | 6563.65 |
| 2026-09-02 | East | 17 | 8737.90 |
| 2026-09-02 | North | 9 | 3573.80 |
| 2026-09-02 | South | 18 | 14038.00 |
| 2026-09-02 | UNKNOWN | 3 | 824.10 |
| 2026-09-02 | West | 17 | 10797.65 |
| 2026-09-03 | East | 19 | 12058.40 |
| 2026-09-03 | North | 19 | 13959.15 |
| 2026-09-03 | South | 10 | 7478.40 |
| 2026-09-03 | UNKNOWN | 3 | 1014.20 |
| 2026-09-03 | West | 18 | 11261.90 |
| 2026-09-04 | East | 18 | 9881.00 |
| 2026-09-04 | North | 9 | 4193.90 |
| 2026-09-04 | South | 9 | 6012.05 |
| 2026-09-04 | UNKNOWN | 4 | 1614.40 |
| 2026-09-04 | West | 13 | 9157.40 |
| 2026-09-05 | East | 12 | 5589.25 |
| 2026-09-05 | North | 8 | 3117.30 |
| 2026-09-05 | South | 13 | 5951.80 |
| 2026-09-05 | UNKNOWN | 4 | 2053.80 |
| 2026-09-05 | West | 21 | 8706.20 |
| 2026-09-06 | East | 15 | 12857.55 |
| 2026-09-06 | North | 16 | 10269.90 |
| 2026-09-06 | South | 18 | 11721.45 |
| 2026-09-06 | UNKNOWN | 3 | 1701.80 |
| 2026-09-06 | West | 17 | 13105.05 |
| 2026-09-07 | East | 10 | 5633.05 |
| 2026-09-07 | North | 11 | 7927.75 |
| 2026-09-07 | South | 14 | 10691.20 |
| 2026-09-07 | UNKNOWN | 5 | 3999.90 |
| 2026-09-07 | West | 21 | 11126.85 |
| 2026-09-08 | East | 15 | 7026.10 |
| 2026-09-08 | North | 18 | 7447.95 |
| 2026-09-08 | South | 14 | 8673.80 |
| 2026-09-08 | UNKNOWN | 6 | 5860.85 |
| 2026-09-08 | West | 21 | 7549.00 |
| 2026-09-09 | East | 23 | 8810.75 |
| 2026-09-09 | North | 11 | 6723.75 |
| 2026-09-09 | South | 16 | 7823.95 |
| 2026-09-09 | UNKNOWN | 4 | 1211.30 |
| 2026-09-09 | West | 17 | 8433.00 |
| 2026-09-10 | East | 21 | 10783.05 |
| 2026-09-10 | North | 24 | 14057.00 |
| 2026-09-10 | South | 12 | 4897.40 |
| 2026-09-10 | UNKNOWN | 6 | 2333.15 |
| 2026-09-10 | West | 9 | 10202.15 |
| 2026-09-11 | East | 14 | 12694.95 |
| 2026-09-11 | North | 15 | 6669.65 |
| 2026-09-11 | South | 10 | 4133.75 |
| 2026-09-11 | UNKNOWN | 3 | 2229.55 |
| 2026-09-11 | West | 16 | 12850.60 |
| 2026-09-12 | East | 20 | 8846.75 |
| 2026-09-12 | North | 15 | 5780.30 |
| 2026-09-12 | South | 12 | 5669.05 |
| 2026-09-12 | UNKNOWN | 4 | 1895.80 |
| 2026-09-12 | West | 24 | 16220.45 |
| 2026-09-13 | East | 24 | 13290.95 |
| 2026-09-13 | North | 11 | 6310.90 |
| 2026-09-13 | South | 13 | 8098.15 |
| 2026-09-13 | UNKNOWN | 4 | 4559.90 |
| 2026-09-13 | West | 24 | 8293.35 |
| 2026-09-14 | East | 21 | 13851.45 |
| 2026-09-14 | North | 16 | 7994.15 |
| 2026-09-14 | South | 11 | 4274.45 |
| 2026-09-14 | UNKNOWN | 5 | 4447.40 |
| 2026-09-14 | West | 21 | 7404.50 |
| 2026-09-15 | East | 19 | 8582.60 |
| 2026-09-15 | North | 14 | 9051.15 |
| 2026-09-15 | South | 8 | 3763.20 |
| 2026-09-15 | UNKNOWN | 6 | 3031.60 |
| 2026-09-15 | West | 21 | 12648.60 |
| 2026-09-16 | East | 22 | 11436.60 |
| 2026-09-16 | North | 14 | 7177.90 |
| 2026-09-16 | South | 14 | 10746.05 |
| 2026-09-16 | UNKNOWN | 9 | 4248.10 |
| 2026-09-16 | West | 14 | 5143.00 |
| 2026-09-17 | East | 19 | 7804.25 |
| 2026-09-17 | North | 14 | 7661.55 |
| 2026-09-17 | South | 16 | 10943.25 |
| 2026-09-17 | UNKNOWN | 3 | 1034.00 |
| 2026-09-17 | West | 22 | 13046.15 |
| 2026-09-18 | East | 16 | 11885.40 |
| 2026-09-18 | North | 9 | 4010.30 |
| 2026-09-18 | South | 10 | 7463.25 |
| 2026-09-18 | UNKNOWN | 4 | 1028.80 |
| 2026-09-18 | West | 21 | 10375.10 |
| 2026-09-19 | East | 24 | 8930.15 |
| 2026-09-19 | North | 27 | 18476.45 |
| 2026-09-19 | South | 17 | 7791.10 |
| 2026-09-19 | UNKNOWN | 3 | 1123.90 |
| 2026-09-19 | West | 23 | 18613.20 |
| 2026-09-20 | East | 22 | 11111.25 |
| 2026-09-20 | North | 15 | 7516.40 |
| 2026-09-20 | South | 15 | 8005.30 |
| 2026-09-20 | UNKNOWN | 6 | 4478.30 |
| 2026-09-20 | West | 20 | 7113.35 |
| 2026-09-21 | East | 19 | 9942.25 |
| 2026-09-21 | North | 11 | 3538.15 |
| 2026-09-21 | South | 10 | 6184.60 |
| 2026-09-21 | UNKNOWN | 8 | 4459.05 |
| 2026-09-21 | West | 22 | 11445.35 |
| 2026-09-22 | East | 19 | 9105.45 |
| 2026-09-22 | North | 7 | 4602.00 |
| 2026-09-22 | South | 10 | 5782.40 |
| 2026-09-22 | UNKNOWN | 7 | 3499.45 |
| 2026-09-22 | West | 14 | 6473.00 |
| 2026-09-23 | East | 19 | 6257.25 |
| 2026-09-23 | North | 10 | 5059.65 |
| 2026-09-23 | South | 19 | 8955.10 |
| 2026-09-23 | UNKNOWN | 7 | 5610.25 |
| 2026-09-23 | West | 25 | 11461.90 |
| 2026-09-24 | East | 15 | 6253.95 |
| 2026-09-24 | North | 16 | 8408.75 |
| 2026-09-24 | South | 14 | 8242.90 |
| 2026-09-24 | UNKNOWN | 8 | 3674.75 |
| 2026-09-24 | West | 19 | 7427.35 |
| 2026-09-25 | East | 20 | 7089.45 |
| 2026-09-25 | North | 13 | 5009.60 |
| 2026-09-25 | South | 13 | 10822.70 |
| 2026-09-25 | UNKNOWN | 6 | 2241.90 |
| 2026-09-25 | West | 21 | 10007.65 |
| 2026-09-26 | East | 25 | 13671.80 |
| 2026-09-26 | North | 13 | 10178.85 |
| 2026-09-26 | South | 27 | 17109.70 |
| 2026-09-26 | UNKNOWN | 1 | 109.00 |
| 2026-09-26 | West | 11 | 4615.55 |
| 2026-09-27 | East | 16 | 10585.20 |
| 2026-09-27 | North | 5 | 3054.75 |
| 2026-09-27 | South | 13 | 7264.90 |
| 2026-09-27 | UNKNOWN | 3 | 2311.80 |
| 2026-09-27 | West | 18 | 8917.30 |
| 2026-09-28 | East | 16 | 7360.95 |
| 2026-09-28 | North | 13 | 7259.05 |
| 2026-09-28 | South | 11 | 3377.10 |
| 2026-09-28 | UNKNOWN | 4 | 4376.00 |
| 2026-09-28 | West | 16 | 7833.05 |
| 2026-09-29 | East | 18 | 9885.15 |
| 2026-09-29 | North | 24 | 12924.35 |
| 2026-09-29 | South | 16 | 11095.55 |
| 2026-09-29 | UNKNOWN | 1 | 205.70 |
| 2026-09-29 | West | 18 | 9425.95 |
| 2026-09-30 | East | 19 | 9750.55 |
| 2026-09-30 | North | 9 | 4469.55 |
| 2026-09-30 | South | 17 | 8638.95 |
| 2026-09-30 | UNKNOWN | 6 | 1702.10 |
| 2026-09-30 | West | 16 | 5910.10 |
| 2026-10-01 | East | 54 | 23771.95 |
| 2026-10-01 | North | 30 | 17380.45 |
| 2026-10-01 | South | 24 | 15846.75 |
| 2026-10-01 | UNKNOWN | 5 | 2656.40 |
| 2026-10-01 | West | 37 | 19996.65 |

### Q1b: Daily net revenue by STORE

```sql
SELECT order_date, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, store_id;
```

| order_date | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|
| 2026-09-01 | S01 | NammaMart Hebbal | 4 | 2247.80 |
| 2026-09-01 | S02 | NammaMart Yelahanka | 6 | 2385.55 |
| 2026-09-01 | S03 | NammaMart RT Nagar | 6 | 4583.80 |
| 2026-09-01 | S04 | NammaMart Jayanagar | 1 | 1103.30 |
| 2026-09-01 | S05 | NammaMart JP Nagar | 2 | 803.00 |
| 2026-09-01 | S06 | NammaMart Banashankari | 6 | 2812.00 |
| 2026-09-01 | S07 | NammaMart BTM Layout | 9 | 5844.75 |
| 2026-09-01 | S08 | NammaMart Whitefield | 4 | 2984.70 |
| 2026-09-01 | S09 | NammaMart Indiranagar | 6 | 2945.40 |
| 2026-09-01 | S10 | NammaMart Marathahalli | 3 | 2141.60 |
| 2026-09-01 | S11 | NammaMart HSR Layout | 6 | 2121.15 |
| 2026-09-01 | S12 | NammaMart Rajajinagar | 1 | 300.00 |
| 2026-09-01 | S13 | NammaMart Malleshwaram | 3 | 1469.10 |
| 2026-09-01 | S14 | NammaMart Vijayanagar | 7 | 3095.85 |
| 2026-09-01 | S15 | NammaMart Basaveshwaranagar | 3 | 1698.70 |
| 2026-09-02 | S01 | NammaMart Hebbal | 3 | 1125.00 |
| 2026-09-02 | S02 | NammaMart Yelahanka | 2 | 889.20 |
| 2026-09-02 | S03 | NammaMart RT Nagar | 4 | 1559.60 |
| 2026-09-02 | S04 | NammaMart Jayanagar | 7 | 4511.90 |
| 2026-09-02 | S05 | NammaMart JP Nagar | 5 | 5154.50 |
| 2026-09-02 | S06 | NammaMart Banashankari | 6 | 4371.60 |
| 2026-09-02 | S07 | NammaMart BTM Layout | 3 | 824.10 |
| 2026-09-02 | S08 | NammaMart Whitefield | 4 | 1183.60 |
| 2026-09-02 | S09 | NammaMart Indiranagar | 7 | 3698.20 |
| 2026-09-02 | S10 | NammaMart Marathahalli | 4 | 2996.90 |
| 2026-09-02 | S11 | NammaMart HSR Layout | 2 | 859.20 |
| 2026-09-02 | S12 | NammaMart Rajajinagar | 1 | 412.00 |
| 2026-09-02 | S13 | NammaMart Malleshwaram | 4 | 1252.15 |
| 2026-09-02 | S14 | NammaMart Vijayanagar | 4 | 5177.90 |
| 2026-09-02 | S15 | NammaMart Basaveshwaranagar | 8 | 3955.60 |
| 2026-09-03 | S01 | NammaMart Hebbal | 6 | 3033.00 |
| 2026-09-03 | S02 | NammaMart Yelahanka | 6 | 6597.85 |
| 2026-09-03 | S03 | NammaMart RT Nagar | 7 | 4328.30 |
| 2026-09-03 | S04 | NammaMart Jayanagar | 1 | 138.60 |
| 2026-09-03 | S05 | NammaMart JP Nagar | 3 | 2693.00 |
| 2026-09-03 | S06 | NammaMart Banashankari | 6 | 4646.80 |
| 2026-09-03 | S07 | NammaMart BTM Layout | 3 | 1014.20 |
| 2026-09-03 | S08 | NammaMart Whitefield | 3 | 1660.45 |
| 2026-09-03 | S09 | NammaMart Indiranagar | 4 | 2858.75 |
| 2026-09-03 | S10 | NammaMart Marathahalli | 3 | 2344.90 |
| 2026-09-03 | S11 | NammaMart HSR Layout | 9 | 5194.30 |
| 2026-09-03 | S12 | NammaMart Rajajinagar | 3 | 935.65 |
| 2026-09-03 | S13 | NammaMart Malleshwaram | 4 | 2314.30 |
| 2026-09-03 | S14 | NammaMart Vijayanagar | 7 | 4695.30 |
| 2026-09-03 | S15 | NammaMart Basaveshwaranagar | 4 | 3316.65 |
| 2026-09-04 | S01 | NammaMart Hebbal | 2 | 427.00 |
| 2026-09-04 | S02 | NammaMart Yelahanka | 4 | 2672.50 |
| 2026-09-04 | S03 | NammaMart RT Nagar | 3 | 1094.40 |
| 2026-09-04 | S04 | NammaMart Jayanagar | 4 | 3555.30 |
| 2026-09-04 | S05 | NammaMart JP Nagar | 4 | 2157.95 |
| 2026-09-04 | S06 | NammaMart Banashankari | 1 | 298.80 |
| 2026-09-04 | S07 | NammaMart BTM Layout | 4 | 1614.40 |
| 2026-09-04 | S08 | NammaMart Whitefield | 4 | 1772.10 |
| 2026-09-04 | S09 | NammaMart Indiranagar | 4 | 1724.70 |
| 2026-09-04 | S10 | NammaMart Marathahalli | 6 | 2793.80 |
| 2026-09-04 | S11 | NammaMart HSR Layout | 4 | 3590.40 |
| 2026-09-04 | S12 | NammaMart Rajajinagar | 2 | 537.50 |
| 2026-09-04 | S13 | NammaMart Malleshwaram | 2 | 3238.20 |
| 2026-09-04 | S14 | NammaMart Vijayanagar | 5 | 4294.30 |
| 2026-09-04 | S15 | NammaMart Basaveshwaranagar | 4 | 1087.40 |
| 2026-09-05 | S01 | NammaMart Hebbal | 3 | 1419.55 |
| 2026-09-05 | S02 | NammaMart Yelahanka | 2 | 249.90 |
| 2026-09-05 | S03 | NammaMart RT Nagar | 3 | 1447.85 |
| 2026-09-05 | S04 | NammaMart Jayanagar | 4 | 1286.40 |
| 2026-09-05 | S05 | NammaMart JP Nagar | 5 | 3454.40 |
| 2026-09-05 | S06 | NammaMart Banashankari | 4 | 1211.00 |
| 2026-09-05 | S07 | NammaMart BTM Layout | 4 | 2053.80 |
| 2026-09-05 | S08 | NammaMart Whitefield | 4 | 1460.70 |
| 2026-09-05 | S09 | NammaMart Indiranagar | 6 | 3484.55 |
| 2026-09-05 | S10 | NammaMart Marathahalli | 1 | 416.00 |
| 2026-09-05 | S11 | NammaMart HSR Layout | 1 | 228.00 |
| 2026-09-05 | S12 | NammaMart Rajajinagar | 7 | 2976.05 |
| 2026-09-05 | S13 | NammaMart Malleshwaram | 4 | 1973.20 |
| 2026-09-05 | S14 | NammaMart Vijayanagar | 4 | 1239.10 |
| 2026-09-05 | S15 | NammaMart Basaveshwaranagar | 6 | 2517.85 |
| 2026-09-06 | S01 | NammaMart Hebbal | 5 | 3443.80 |
| 2026-09-06 | S02 | NammaMart Yelahanka | 3 | 1013.70 |
| 2026-09-06 | S03 | NammaMart RT Nagar | 8 | 5812.40 |
| 2026-09-06 | S04 | NammaMart Jayanagar | 8 | 5279.65 |
| 2026-09-06 | S05 | NammaMart JP Nagar | 3 | 3585.00 |
| 2026-09-06 | S06 | NammaMart Banashankari | 7 | 2856.80 |
| 2026-09-06 | S07 | NammaMart BTM Layout | 3 | 1701.80 |
| 2026-09-06 | S08 | NammaMart Whitefield | 4 | 3398.60 |
| 2026-09-06 | S09 | NammaMart Indiranagar | 3 | 3148.40 |
| 2026-09-06 | S10 | NammaMart Marathahalli | 3 | 1938.15 |
| 2026-09-06 | S11 | NammaMart HSR Layout | 5 | 4372.40 |
| 2026-09-06 | S12 | NammaMart Rajajinagar | 2 | 924.90 |
| 2026-09-06 | S13 | NammaMart Malleshwaram | 3 | 3087.00 |
| 2026-09-06 | S14 | NammaMart Vijayanagar | 2 | 307.45 |
| 2026-09-06 | S15 | NammaMart Basaveshwaranagar | 10 | 8785.70 |
| 2026-09-07 | S01 | NammaMart Hebbal | 4 | 2574.25 |
| 2026-09-07 | S02 | NammaMart Yelahanka | 2 | 973.10 |
| 2026-09-07 | S03 | NammaMart RT Nagar | 5 | 4380.40 |
| 2026-09-07 | S04 | NammaMart Jayanagar | 4 | 2488.40 |
| 2026-09-07 | S05 | NammaMart JP Nagar | 6 | 3645.15 |
| 2026-09-07 | S06 | NammaMart Banashankari | 4 | 4557.65 |
| 2026-09-07 | S07 | NammaMart BTM Layout | 5 | 3999.90 |
| 2026-09-07 | S08 | NammaMart Whitefield | 5 | 2039.00 |
| 2026-09-07 | S10 | NammaMart Marathahalli | 3 | 2433.45 |
| 2026-09-07 | S11 | NammaMart HSR Layout | 2 | 1160.60 |
| 2026-09-07 | S12 | NammaMart Rajajinagar | 2 | 1455.00 |
| 2026-09-07 | S13 | NammaMart Malleshwaram | 9 | 5120.20 |
| 2026-09-07 | S14 | NammaMart Vijayanagar | 4 | 1632.55 |
| 2026-09-07 | S15 | NammaMart Basaveshwaranagar | 6 | 2919.10 |
| 2026-09-08 | S01 | NammaMart Hebbal | 8 | 3028.20 |
| 2026-09-08 | S02 | NammaMart Yelahanka | 6 | 2906.65 |
| 2026-09-08 | S03 | NammaMart RT Nagar | 4 | 1513.10 |
| 2026-09-08 | S04 | NammaMart Jayanagar | 3 | 1204.00 |
| 2026-09-08 | S05 | NammaMart JP Nagar | 4 | 2877.10 |
| 2026-09-08 | S06 | NammaMart Banashankari | 7 | 4592.70 |
| 2026-09-08 | S07 | NammaMart BTM Layout | 6 | 5860.85 |
| 2026-09-08 | S08 | NammaMart Whitefield | 6 | 2389.45 |
| 2026-09-08 | S09 | NammaMart Indiranagar | 3 | 643.15 |
| 2026-09-08 | S10 | NammaMart Marathahalli | 1 | 358.40 |
| 2026-09-08 | S11 | NammaMart HSR Layout | 5 | 3635.10 |
| 2026-09-08 | S12 | NammaMart Rajajinagar | 2 | 667.80 |
| 2026-09-08 | S13 | NammaMart Malleshwaram | 4 | 825.65 |
| 2026-09-08 | S14 | NammaMart Vijayanagar | 7 | 2558.30 |
| 2026-09-08 | S15 | NammaMart Basaveshwaranagar | 8 | 3497.25 |
| 2026-09-09 | S01 | NammaMart Hebbal | 5 | 2663.80 |
| 2026-09-09 | S02 | NammaMart Yelahanka | 5 | 3922.95 |
| 2026-09-09 | S03 | NammaMart RT Nagar | 1 | 137.00 |
| 2026-09-09 | S04 | NammaMart Jayanagar | 7 | 1483.70 |
| 2026-09-09 | S05 | NammaMart JP Nagar | 4 | 2035.45 |
| 2026-09-09 | S06 | NammaMart Banashankari | 5 | 4304.80 |
| 2026-09-09 | S07 | NammaMart BTM Layout | 4 | 1211.30 |
| 2026-09-09 | S08 | NammaMart Whitefield | 7 | 2912.50 |
| 2026-09-09 | S09 | NammaMart Indiranagar | 3 | 1569.85 |
| 2026-09-09 | S10 | NammaMart Marathahalli | 9 | 2875.95 |
| 2026-09-09 | S11 | NammaMart HSR Layout | 4 | 1452.45 |
| 2026-09-09 | S12 | NammaMart Rajajinagar | 2 | 794.20 |
| 2026-09-09 | S13 | NammaMart Malleshwaram | 2 | 1870.40 |
| 2026-09-09 | S14 | NammaMart Vijayanagar | 6 | 2153.80 |
| 2026-09-09 | S15 | NammaMart Basaveshwaranagar | 7 | 3614.60 |
| 2026-09-10 | S01 | NammaMart Hebbal | 10 | 4023.95 |
| 2026-09-10 | S02 | NammaMart Yelahanka | 8 | 3889.35 |
| 2026-09-10 | S03 | NammaMart RT Nagar | 6 | 6143.70 |
| 2026-09-10 | S04 | NammaMart Jayanagar | 5 | 1564.60 |
| 2026-09-10 | S05 | NammaMart JP Nagar | 5 | 1411.80 |
| 2026-09-10 | S06 | NammaMart Banashankari | 2 | 1921.00 |
| 2026-09-10 | S07 | NammaMart BTM Layout | 6 | 2333.15 |
| 2026-09-10 | S08 | NammaMart Whitefield | 5 | 3589.80 |
| 2026-09-10 | S09 | NammaMart Indiranagar | 3 | 1917.60 |
| 2026-09-10 | S10 | NammaMart Marathahalli | 7 | 2026.05 |
| 2026-09-10 | S11 | NammaMart HSR Layout | 6 | 3249.60 |
| 2026-09-10 | S12 | NammaMart Rajajinagar | 2 | 4040.00 |
| 2026-09-10 | S13 | NammaMart Malleshwaram | 4 | 4405.40 |
| 2026-09-10 | S15 | NammaMart Basaveshwaranagar | 3 | 1756.75 |
| 2026-09-11 | S01 | NammaMart Hebbal | 1 | 688.00 |
| 2026-09-11 | S02 | NammaMart Yelahanka | 5 | 1945.90 |
| 2026-09-11 | S03 | NammaMart RT Nagar | 9 | 4035.75 |
| 2026-09-11 | S04 | NammaMart Jayanagar | 6 | 2741.25 |
| 2026-09-11 | S06 | NammaMart Banashankari | 4 | 1392.50 |
| 2026-09-11 | S07 | NammaMart BTM Layout | 3 | 2229.55 |
| 2026-09-11 | S08 | NammaMart Whitefield | 3 | 3830.95 |
| 2026-09-11 | S09 | NammaMart Indiranagar | 4 | 3679.90 |
| 2026-09-11 | S10 | NammaMart Marathahalli | 3 | 3619.15 |
| 2026-09-11 | S11 | NammaMart HSR Layout | 4 | 1564.95 |
| 2026-09-11 | S12 | NammaMart Rajajinagar | 5 | 6569.20 |
| 2026-09-11 | S13 | NammaMart Malleshwaram | 6 | 3296.05 |
| 2026-09-11 | S14 | NammaMart Vijayanagar | 1 | 67.00 |
| 2026-09-11 | S15 | NammaMart Basaveshwaranagar | 4 | 2918.35 |
| 2026-09-12 | S01 | NammaMart Hebbal | 5 | 2887.10 |
| 2026-09-12 | S02 | NammaMart Yelahanka | 3 | 514.40 |
| 2026-09-12 | S03 | NammaMart RT Nagar | 7 | 2378.80 |
| 2026-09-12 | S04 | NammaMart Jayanagar | 6 | 2780.05 |
| 2026-09-12 | S05 | NammaMart JP Nagar | 3 | 1801.00 |
| 2026-09-12 | S06 | NammaMart Banashankari | 3 | 1088.00 |
| 2026-09-12 | S07 | NammaMart BTM Layout | 4 | 1895.80 |
| 2026-09-12 | S08 | NammaMart Whitefield | 3 | 1137.15 |
| 2026-09-12 | S09 | NammaMart Indiranagar | 6 | 4337.05 |
| 2026-09-12 | S10 | NammaMart Marathahalli | 4 | 488.35 |
| 2026-09-12 | S11 | NammaMart HSR Layout | 7 | 2884.20 |
| 2026-09-12 | S12 | NammaMart Rajajinagar | 3 | 720.70 |
| 2026-09-12 | S13 | NammaMart Malleshwaram | 9 | 6585.30 |
| 2026-09-12 | S14 | NammaMart Vijayanagar | 6 | 2916.95 |
| 2026-09-12 | S15 | NammaMart Basaveshwaranagar | 6 | 5997.50 |
| 2026-09-13 | S01 | NammaMart Hebbal | 1 | 2625.00 |
| 2026-09-13 | S02 | NammaMart Yelahanka | 4 | 1245.40 |
| 2026-09-13 | S03 | NammaMart RT Nagar | 6 | 2440.50 |
| 2026-09-13 | S04 | NammaMart Jayanagar | 4 | 3371.35 |
| 2026-09-13 | S05 | NammaMart JP Nagar | 5 | 3416.80 |
| 2026-09-13 | S06 | NammaMart Banashankari | 4 | 1310.00 |
| 2026-09-13 | S07 | NammaMart BTM Layout | 4 | 4559.90 |
| 2026-09-13 | S08 | NammaMart Whitefield | 9 | 5865.65 |
| 2026-09-13 | S09 | NammaMart Indiranagar | 5 | 2163.80 |
| 2026-09-13 | S10 | NammaMart Marathahalli | 4 | 1084.80 |
| 2026-09-13 | S11 | NammaMart HSR Layout | 6 | 4176.70 |
| 2026-09-13 | S12 | NammaMart Rajajinagar | 7 | 2056.30 |
| 2026-09-13 | S13 | NammaMart Malleshwaram | 6 | 2555.30 |
| 2026-09-13 | S14 | NammaMart Vijayanagar | 8 | 2737.00 |
| 2026-09-13 | S15 | NammaMart Basaveshwaranagar | 3 | 944.75 |
| 2026-09-14 | S01 | NammaMart Hebbal | 7 | 3276.25 |
| 2026-09-14 | S02 | NammaMart Yelahanka | 4 | 2203.60 |
| 2026-09-14 | S03 | NammaMart RT Nagar | 5 | 2514.30 |
| 2026-09-14 | S04 | NammaMart Jayanagar | 3 | 1239.00 |
| 2026-09-14 | S05 | NammaMart JP Nagar | 5 | 1649.50 |
| 2026-09-14 | S06 | NammaMart Banashankari | 3 | 1385.95 |
| 2026-09-14 | S07 | NammaMart BTM Layout | 5 | 4447.40 |
| 2026-09-14 | S08 | NammaMart Whitefield | 3 | 836.40 |

_... 260 more rows (460 total)_

### Q2: Revenue and gross margin by product category (margin = revenue - quantity x cost_price)

```sql
SELECT category, delivered_orders, units_sold, revenue, cost_of_goods, gross_margin, margin_pct
FROM etl_category_margin;
```

| category | delivered_orders | units_sold | revenue | cost_of_goods | gross_margin | margin_pct |
|---|---|---|---|---|---|---|
| Staples | 336 | 624 | 287601.45 | 225423.36 | 62178.09 | 21.62 |
| Personal Care | 347 | 678 | 215345.30 | 168422.72 | 46922.58 | 21.79 |
| Household | 362 | 664 | 211727.05 | 157374.54 | 54352.51 | 25.67 |
| Dairy & Bakery | 405 | 760 | 179568.95 | 131935.95 | 47633.00 | 26.53 |
| Snacks & Beverages | 340 | 682 | 174700.60 | 133329.82 | 41370.78 | 23.68 |
| Fruits & Vegetables | 392 | 751 | 99566.20 | 79054.82 | 20511.38 | 20.6 |
| UNKNOWN | 44 | 88 | 41911.75 | 36968.36 | 4943.39 | 11.79 |

### Q3: Average delivery time per store and % of delivered orders within 15 minutes

```sql
SELECT s.store_id, s.store_name,
       count(*)                                                       AS delivered_orders,
       round(avg(f.delivery_minutes), 2)                              AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min
FROM etl_fact_orders f
JOIN etl_dim_store s ON s.store_id = f.store_id
WHERE f.order_status = 'DELIVERED' AND f.delivery_minutes IS NOT NULL
GROUP BY s.store_id, s.store_name
ORDER BY s.store_id;
```

| store_id | store_name | delivered_orders | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|---|
| S01 | NammaMart Hebbal | 149 | 20.47 | 32.21 |
| S02 | NammaMart Yelahanka | 150 | 20.93 | 31.33 |
| S03 | NammaMart RT Nagar | 143 | 20.65 | 29.37 |
| S04 | NammaMart Jayanagar | 138 | 21.31 | 27.54 |
| S05 | NammaMart JP Nagar | 142 | 21.19 | 29.58 |
| S06 | NammaMart Banashankari | 153 | 22.52 | 21.57 |
| S07 | NammaMart BTM Layout | 150 | 21.13 | 32.67 |
| S08 | NammaMart Whitefield | 164 | 20.61 | 35.37 |
| S09 | NammaMart Indiranagar | 136 | 21.13 | 26.47 |
| S10 | NammaMart Marathahalli | 150 | 22.09 | 26.0 |
| S11 | NammaMart HSR Layout | 160 | 20.88 | 28.13 |
| S12 | NammaMart Rajajinagar | 118 | 21.86 | 24.58 |
| S13 | NammaMart Malleshwaram | 150 | 21.87 | 27.33 |
| S14 | NammaMart Vijayanagar | 166 | 21.51 | 31.33 |
| S15 | NammaMart Basaveshwaranagar | 157 | 22.03 | 22.93 |

### Q4a: Revenue by payment mode

```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY net_revenue DESC;
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| UPI | 1205 | 638752.15 |
| Card | 457 | 251882.30 |
| Cash on Delivery | 318 | 181668.40 |
| Wallet | 246 | 138118.45 |

### Q4b: Amount that does not reconcile with the payment gateway (by reason; feed-pending is a timing difference)

```sql
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM etl_payment_reconciliation
GROUP BY mismatch_reason
UNION ALL
SELECT 'TOTAL (excl. PAYMENT_FEED_PENDING)', count(*), round(sum(unreconciled_amount), 2)
FROM etl_payment_reconciliation WHERE mismatch_reason <> 'PAYMENT_FEED_PENDING'
ORDER BY 3 DESC;
```

| mismatch_reason | rows | unreconciled_amount |
|---|---|---|
| TOTAL (excl. PAYMENT_FEED_PENDING) | 191 | 137545.85 |
| PAYMENT_FOR_REJECTED_ORDER | 90 | 84812.98 |
| PAYMENT_FEED_PENDING | 150 | 79652.20 |
| PAYMENT_FAILED | 42 | 25722.25 |
| MISSING_PAYMENT | 28 | 17476.35 |
| ORPHAN_PAYMENT | 8 | 4580.57 |
| RETURNED_NOT_REFUNDED | 10 | 4193.70 |
| AMOUNT_MISMATCH | 13 | 760.00 |

### Q5: Top 10 customers by spend, with loyalty tier (masked identity only)

```sql
SELECT rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank,
       f.customer_id, c.customer_name_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS net_spend
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.customer_name_masked, c.loyalty_tier
ORDER BY spend_rank, f.customer_id
LIMIT 10;
```

| spend_rank | customer_id | customer_name_masked | loyalty_tier | delivered_orders | net_spend |
|---|---|---|---|---|---|
| 1 | C00136 | S. B. | Bronze | 9 | 8738.10 |
| 2 | C00115 | S. K. | Bronze | 8 | 8037.70 |
| 3 | C00283 | N. R. | Bronze | 8 | 7934.60 |
| 4 | C00236 | S. N. | Platinum | 12 | 7831.80 |
| 5 | C00310 | K. P. | Bronze | 8 | 7258.95 |
| 6 | C00265 | V. H. | Gold | 8 | 7151.30 |
| 7 | C00071 | M. P. | Silver | 11 | 6933.30 |
| 8 | C00338 | B. I. | Silver | 9 | 6848.55 |
| 9 | C00357 | A. N. | Bronze | 11 | 6637.90 |
| 10 | C00209 | K. P. | Platinum | 8 | 6519.40 |

### Q6: Rows rejected in the latest run, by file and by reason

```sql
SELECT source_file, reason AS failure_reason, count(*) AS rows_rejected
FROM (
    SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason
    FROM etl_quarantine
    WHERE run_id = (SELECT max_by(run_id, quarantined_at) FROM etl_quarantine)
)
GROUP BY source_file, reason
ORDER BY source_file, rows_rejected DESC, reason;
```

| source_file | failure_reason | rows_rejected |
|---|---|---|
| customers.csv | CUS_CUSTOMER_ID_UNIQUE | 5 |
| orders.csv | ORD_AMOUNT_MATCHES_FORMULA | 18 |
| orders.csv | ORD_CUSTOMER_ID_NOT_NULL | 18 |
| orders.csv | ORD_ORDER_ID_UNIQUE | 12 |
| orders.csv | ORD_QUANTITY_POSITIVE | 10 |
| orders.csv | ORD_PRODUCT_EXISTS | 9 |
| orders.csv | ORD_DELIVERED_HAS_MINUTES | 8 |
| orders.csv | ORD_ORDER_TS_NOT_FUTURE | 7 |
| orders.csv | ORD_DELIVERY_MINUTES_MAX | 6 |
| orders.csv | ORD_ORDER_TS_VALID | 5 |
| orders.csv | ORD_STORE_EXISTS | 5 |
| orders.csv | ORD_DISCOUNT_RANGE | 3 |
| orders.csv | ORD_STATUS_VALID | 3 |
| orders.csv | ORD_PAYMENT_MODE_VALID | 2 |
| orders.csv | ORD_ORDER_TS_NOT_NULL | 1 |
| payments.csv | PAY_AMOUNT_POSITIVE | 10 |
| payments.csv | PAY_ORDER_EXISTS | 8 |
| payments.csv | PAY_TS_NOT_FUTURE | 7 |
| payments.csv | PAY_PAYMENT_ID_UNIQUE | 4 |
| payments.csv | PAY_MODE_VALID | 2 |
| products.csv | PRD_PRODUCT_ID_UNIQUE | 2 |

