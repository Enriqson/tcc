# Generate data
Inside a linux vm

## install libs
sudo apt-get update 
sudo apt install gcc-13 make -y
sudo apt install gcc -y
sudo snap install aws-cli --classic
sudo apt install python3-pip python3-venv -y

mkdir -p $HOME/.venvs  
python3 -m venv $HOME/.venvs/MyEnv
source $HOME/.venvs/MyEnv/bin/activate
pip install duckdb

## generate raw data with dbgen

git clone https://github.com/vadimtk/ssb-dbgen.git
cd ssb-dbgen
make
mkdir s1 s10 s100

cp dbgen dists.dss s1
cp dbgen dists.dss s10
cp dbgen dists.dss s100

cd s1
./dbgen -s 1 -T a

cd ../s10
./dbgen -s 10 -T a

cd ../s100
./dbgen -s 100 -T a

## Transform and upload to s3

### upload the script to the vm in the same folder as the dbgen or alter the scripts directories accordingly
### copying the script from the s3 bucket 
aws s3 cp s3://<bucket_name>/python_scripts/parse_data.py parse_data.py

python3 parse_data.py 


### another option is to use the duckdb cli directly
curl https://install.duckdb.org | sh
alias duckdb=/home/ubuntu/.duckdb/cli/latest/duckdb

COPY (SELECT * FROM read_csv('s100/lineorder.tbl')) TO 'output.parquet' (FORMAT PARQUET);

CREATE TABLE t1 AS
    SELECT * FROM read_csv('s100/lineorder.tbl');
