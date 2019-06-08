#!/bin/bash

for priv in 1 21 41 61 81 101 121 141 161 181 200
do
    for pub in 2
    do
        echo -en "${priv} "
        ./target/release/actix-elray-sim --numprivate=${priv} --numpublic=${pub} -r
    done
done
