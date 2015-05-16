#!/bin/bash
n=0
while read line ; do
  let n=n+line
done
echo $n
