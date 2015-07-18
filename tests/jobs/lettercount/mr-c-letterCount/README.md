# C-Mapper

## Mapper
Receives via STDIN a list of words separated by space and enters and prints every letter of it.

## Reducer
Receives via STDIN the *Mapper* results (or another Reducer results) and adds the amount grouped by letter

## Usage
It can be called from a MapReduce stream job, or for testing purposes via command-line

	cat *.txt | ./mapper | sort | ./reducer

## Installing

Just use Make:

    make

## Example Data

https://www.dropbox.com/sh/geizvpqqz2aiwxv/AADMCmnpeBRBGTzGXpAVB4Nla?dl=0
