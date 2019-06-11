#!/bin/bash

for priv in 10 20 40 60 80 100 120 140 160 180 200 220 240 260 280 300
do
    for pub in 8
    do
        echo -en "${priv} "
        ./target/release/actix-elray-sim --numprivate=${priv} --numpublic=${pub}
    done
done
