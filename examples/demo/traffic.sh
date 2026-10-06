#!/bin/sh
# Orders from 500 customers at 1 to 4 a second, the rate changing every 15 minutes.
# About 1 in 50 checkouts fails after the event was recorded (the order and the event
# roll back together), 9 in 10 orders are paid, and 1 in 14 is paid twice, as a double
# click would: the second payment gets a 409 and records nothing.
APP=${APP:-http://app:8000}

rnd() { od -An -N2 -tu2 /dev/urandom | tr -d ' '; }

while :; do
    n=$((1 + $(date +%s) / 900 % 4))
    i=0
    while [ "$i" -lt "$n" ]; do
        i=$((i + 1))
        customer=$(printf 'c%03d' $(($(rnd) % 500 + 1)))
        total=$(($(rnd) % 9900 + 100))
        dice=$(($(rnd) % 100))
        fail=0
        [ "$dice" -lt 2 ] && fail=1
        id=$(curl -s -m 5 -X POST "$APP/orders" -d customer="$customer" -d total="$total" -d fail="$fail" \
            | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
        [ -z "$id" ] && continue
        [ "$dice" -ge 92 ] && continue
        curl -s -m 5 -o /dev/null -X POST "$APP/orders/$id/pay"
        [ "$dice" -lt 7 ] && curl -s -m 5 -o /dev/null -X POST "$APP/orders/$id/pay"
    done
    sleep 1
done
