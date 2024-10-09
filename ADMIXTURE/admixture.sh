#!/bin/zsh

# Check if the user provided a file name
if [ $# -ne 1 ]; then
  echo "Usage: $0 <input-file.bed>"
  exit 1
fi

# Assign the input file to a variable
input_file=$1

# Loop through values from 2 to 70
for K in {2..70}
do
  admixture --cv "$input_file" $K -j12
done