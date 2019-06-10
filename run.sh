#!/bin/bash

for priv in 420 440 480 500
do
    for pub in 2
    do
        echo -en "${priv} "
        ./target/release/actix-elray-sim --numprivate=${priv} --numpublic=${pub} -r
    done
done
