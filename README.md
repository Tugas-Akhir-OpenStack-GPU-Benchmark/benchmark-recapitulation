## OpenStack GPU Benchmark Recapitulation Script

This script is used to automate calculating descriptive statistics, standard deviation, and student t-test from raw 
benchmark result. This script will automatically output latex codes to declare variables for the calculated values, 
generate boxplot graphics, and generate bell curve graphics from the data. This script will also update a Google Spreadsheet
automatically and update tables and graphs in a Google Slide. 

To update the input data for this script, please go to folder `benchmark_result` 
from the Ansible benchmark script that has been run. After that, run `more * | cat` in the directory, 
copy all the output, and paste it in a single file. Please put the file in folder `./data` of this script. 
The file name can be anything, but please make sure that it contains substring either `physical`, `nova`, `zun`, or `ironic`, 
and each of them should appear in exactly one file.

After that, please make sure all libraries in `requirements.txt` has been satisfied and then
run `main.py` of this script. This script should create a folder called `./graphics` that contains PNG images 
for generated boxplot & bell-curve graph, and tables as well. This script should also 
generate `./latex_command.tex` that contains latex code, which will declare latex variables that will be used
in the paper. If configured properly, this script will update the specified Google Spreadsheet 
and graphs in the specified Google Slide as well.