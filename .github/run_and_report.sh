#!/usr/bin/env bash
# Runs a command and repeats the tail of its output as a job annotation, so
# results show up on the run page without digging through logs.
#   .github/run_and_report.sh <title> <command...>
title="$1"; shift
log=$(mktemp)
"$@" 2>&1 | tee "$log"
rc=${PIPESTATUS[0]}
level=notice
[ "$rc" -ne 0 ] && level=error
body=$(tail -n 40 "$log" | sed ':a;N;$!ba;s/%/%25/g;s/\r//g;s/\n/%0A/g')
echo "::${level} title=${title}::${body}"
exit "$rc"
