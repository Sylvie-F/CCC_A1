185875709 rows intotal

code to run with scripts uses name: {A1.py}
command to run scripts:
sbatch 1-1.slurm
sbatch 1-8.slurm
sbatch 2-8.slurm

command to check working status:
squeue --me

upload file:
scp local.dat myusername@spartan.hpc.unimelb.edu.au:/home/{username}/remote.dat
scp {file.dat} zhifeic@spartan.hpc.unimelb.edu.au:/home/zhifeic/assn1

download file:
scp myusername@spartan.hpc.unimelb.edu.au:/home/{username}/remote.dat local.dat 
scp zhifeic@spartan.hpc.unimelb.edu.au:/home/zhifeic/assn1/{file.dat} {local-copy.dat}

handle line break:
sed -i 's/\r$//' 1-1.slurm

output:
fast with timer:
1-1 --- cat slurm-57318351.out
1-8 --- cat slurm-57318363.out
2-8 --- cat slurm-57318368.out

final:
1-1 --- cat slurm-57434675.out
1-8 --- cat slurm-57434745.out
2-8 --- cat slurm-57434678.out


