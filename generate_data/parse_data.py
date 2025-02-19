import duckdb as ddb
import logging
import os

BUCKET_NAME='<bucket_name>'
S_FACTORS = ['s1','s10','s100']

INITIAL_DATASET_PERCENTAGE = .5
ADDITIONAL_DATASET_COUNT = 5

logging.basicConfig(
    format="%(asctime)s, %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

columns_per_table = {
    "customer": [
        "C_CUSTKEY",
        "C_NAME",
        "C_ADDRESS",
        "C_CITY",
        "C_NATION",
        "C_REGION",
        "C_PHONE",
        "C_MKTSEGMENT",
    ],
    "part": [
        "P_PARTKEY",
        "P_NAME",
        "P_MFGR",
        "P_CATEGORY",
        "P_BRAND",
        "P_COLOR",
        "P_TYPE",
        "P_SIZE",
        "P_CONTAINER",
    ],
    "supplier": [
        "S_SUPPKEY",
        "S_NAME",
        "S_ADDRESS",
        "S_CITY",
        "S_NATION",
        "S_REGION",
        "S_PHONE",
    ],
    "date": [
        "D_DATEKEY",
        "D_DATE",
        "D_DAYOFWEEK",
        "D_MONTH",
        "D_YEAR",
        "D_YEARMONTHNUM",
        "D_YEARMONTH",
        "D_DAYNUMINWEEK",
        "D_DAYNUMINMONTH",
        "D_DAYNUMINYEAR",
        "D_MONTHNUMINYEAR",
        "D_WEEKNUMINYEAR",
        "D_SELLINGSEASON",
        "D_LASTDAYINWEEKFL",
        "D_LASTDAYINMONTHFL",
        "D_HOLIDAYFL",
        "D_WEEKDAYFL",
    ],
    "lineorder": [
        "LO_ORDERKEY",
        "LO_LINENUMBER",
        "LO_CUSTKEY",
        "LO_PARTKEY",
        "LO_SUPPKEY",
        "LO_ORDERDATE",
        "LO_ORDERPRIORITY",
        "LO_SHIPPRIORITY",
        "LO_QUANTITY",
        "LO_EXTENDEDPRICE",
        "LO_ORDTOTALPRICE",
        "LO_DISCOUNT",
        "LO_REVENUE",
        "LO_SUPPLYCOST",
        "LO_TAX",
        "LO_COMMITDATE",
        "LO_SHIPMODE",
    ],
}

        
def generate_parsed_parquet(input_file, parquet_filename, column_names):
    input_file = f"{s_factor}/{table_name}.tbl"
    columns_select_statement = ','.join(column_names)
    
    logging.info("Creating parsed parquet file...")
    ddb.sql(f"""
                COPY (
                    SELECT {columns_select_statement} FROM 
                    read_csv('{input_file}', delim=',', quote='"', header=false, column_names={column_names})
                )    
                TO '{parquet_filename}' (FORMAT PARQUET);
    """)
    logging.info("Creation successfull")

def get_total_data_count(parquet_filename):
    total_data_count = ddb.sql(f"""
        select count(*) from read_parquet('{parquet_filename}')
        """).fetchone()[0]
    return total_data_count

def output_initial_dataset_to_s3(parquet_filename, s3_base_path, initial_dataset_count):
    logging.info("Saving initial dataset to s3...")
    path = s3_base_path+"/initial/data.parquet"
    ddb.sql(f"""
        COPY (
                select * from
                read_parquet('{parquet_filename}')
                limit {initial_dataset_count}
        ) TO '{path}';
        """)
    logging.info("Saving done!")

def calculate_additional_limits_and_offsets(initial_dataset_count, total_data_count):
    # Calculate the approximate size of each chunk
    remainder_data_size = total_data_count- initial_dataset_count
    additional_chunk_size = remainder_data_size // ADDITIONAL_DATASET_COUNT
    remainder = remainder_data_size % ADDITIONAL_DATASET_COUNT  # Extra elements to distribute

    limits = []
    offsets = []
    offset = initial_dataset_count

    for i in range(ADDITIONAL_DATASET_COUNT):
        # Calculate the size for this chunk (add 1 if within remainder)
        current_chunk_size = additional_chunk_size + (1 if i < remainder else 0)
        limit = current_chunk_size

        # Store the offset and limit
        limits.append(limit)
        offsets.append(offset)

        # Update offset for the next chunk
        offset += limit

    return offsets,limits

def output_additional_datasets_to_s3(parquet_filename, s3_base_path, initial_dataset_count, total_data_count):
    logging.info("Saving additional dataset to s3...")
    offsets, limits = calculate_additional_limits_and_offsets(initial_dataset_count,total_data_count)
    
    for i in range(ADDITIONAL_DATASET_COUNT):
        output_path = s3_base_path+f"/additional_{i}/data.parquet"
        logging.info(f"Processing additional dataset {i}")
        ddb.sql(f"""
            COPY (
                    select * from
                    read_parquet('{parquet_filename}')
                    limit {limits[i]}
                    offset {offsets[i]}
            ) TO '{output_path}';
            """)
    logging.info("Saving done!")

def output_full_dataset_to_s3(parquet_filename, s3_base_path):
    logging.info("Saving full dataset to s3...")
    path = s3_base_path+"/initial/data.parquet"
    ddb.sql(f"""
        COPY (
                select * from
                read_parquet('{parquet_filename}')
        ) TO '{path}';
        """)
    logging.info("Saving done!")


#setup duckdb to upload data to s3
#uses the credentials already in the env
ddb.sql("""
        CREATE SECRET (
        TYPE S3,
        PROVIDER CREDENTIAL_CHAIN,
        REGION 'us-east-2'
    );
    INSTALL HTTPS;
    LOAD HTTPS;
""")


for s_factor in S_FACTORS:
    logging.info(f"Processing factor {s_factor}...")
    for table_name in columns_per_table.keys():
        logging.info(f"Parsing {table_name}")
        input_file = f"{s_factor}/{table_name}.tbl"
        parquet_filename = f"{s_factor}/{table_name}.parquet"
        column_names = columns_per_table[table_name]
        
        s3_base_path =  f"s3://{BUCKET_NAME}/{s_factor}/{table_name}"
        
        
        generate_parsed_parquet(input_file, parquet_filename, column_names)
        total_data_count = get_total_data_count(parquet_filename)
        
        initial_dataset_count = int(total_data_count*INITIAL_DATASET_PERCENTAGE)
        if table_name=='lineorder':
            total_data_count = get_total_data_count(parquet_filename)
            output_initial_dataset_to_s3(parquet_filename,s3_base_path, initial_dataset_count)
            output_additional_datasets_to_s3(parquet_filename, s3_base_path, initial_dataset_count, total_data_count)
        else:    
            output_full_dataset_to_s3(parquet_filename,s3_base_path)
       
        os.remove(parquet_filename)
    
    
    