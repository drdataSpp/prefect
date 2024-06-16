import json
import os
import glob
import collections
import prefect
from prefect import flow, task, get_run_logger

## Defining project dirs variables
#############################################################################################

project_dir = 'file_watcher_prefect_flow'
params_dir = 'flow_params'
param_file = 'PS_flow_one_param.json'
params_path = os.path.join(os.getcwd(), project_dir, params_dir, param_file)

data_dir = ''
data_file_name = ''
file_ext = ''

## Defining functions for the flow
#############################################################################################

@task
def extract_project_params(path):
    try:
        with open(path, 'r') as j:
            FLOW_PARAMS = json.loads(j.read())
        
        return FLOW_PARAMS
        
    except Exception as e:
        print("There was an Error while opening the params file:", str(e))

@task(retries = 3, retry_delay_seconds=2)
def preloadstep_file_watcher(params):
    """ This function is used to check whether the source file exists, if not retries after 15 mins for 3 time """
    
    try:
        ps_file = params

        data_dir = ps_file['DATA_DIR']
        data_file_name = ps_file['DATA_FILE_NAME']
        file_ext = ps_file['FILE_EXTENSION']

        source_data_dir = os.path.join(os.getcwd(), project_dir, data_dir)
        externsion_to_look_for = '*' + file_ext

        matching_files = glob.glob(f'{source_data_dir}/{data_file_name + externsion_to_look_for}')

        if not matching_files:
            raise Exception(f"Source File {data_file_name}* is missing. Retrying...")

        output = matching_files

        file_count = collections.Counter(matching_files)
        total_file_retrieved = sum(file_count.values())

        print(total_file_retrieved)

        return output

    except Exception as e:
        error = "Error in File Watcher Step: " + str(e)
        raise Exception(error)
        
@task
def preloadstep_file_count_check(matched_file_dict):
    
    """ This function is to check whether the source dir has only one file"""

    try:
        file_count = collections.Counter(matched_file_dict)
        total_file_retrieved = sum(file_count.values())

        if total_file_retrieved == 1: ##Once the source file is processed, it will be zipped, so count will be 0
            file_process_flag = True
            file_path = matched_file_dict[0]

            return file_process_flag, file_path
        
        else:
            file_process_flag = False
            file_path = ''
            return file_process_flag, file_path 
    
    except Exception as e:
        print("Error while counting file: ", str(e))

@task
def preloadstep_file_size_check(params, file_process_flag, path):

    file_size_threshold = params['FILE_SIZE_THRESHOLD_BYTES']
    isFileReady = file_process_flag
    filePath = path

    try:
        if isFileReady:
            filesize = os.path.getsize(filePath)

            if filesize > file_size_threshold:
                file_greater_than_thres = True

            else:
                file_greater_than_thres = False

    except Exception as e:
        print(str(e))

    finally:
        return file_greater_than_thres
                

@flow
def filewatcher_prefect_flow(name='filewatcher_prefect_flow', log_prints=True):
    
    prefect_logger = get_run_logger()

    extract_params = extract_project_params(params_path)
    file_exist_check = preloadstep_file_watcher(extract_params, wait_for = extract_params)
    file_process_flag, file_path = preloadstep_file_count_check(file_exist_check, wait_for=file_exist_check)
    output = preloadstep_file_size_check(extract_params, file_process_flag, file_path, wait_for=[file_process_flag, file_path])

    if output:
        prefect_logger.info('Flow will continue as the file is greater than the threshold.')
    else:
        prefect_logger.info('Flow will stop as the source file is lesser than the threshold.')

    ## Add more ETL tasks after checking the file size, example, if the files is greater than the threshold then process else end the flow

if __name__ == "__main__":
    filewatcher_prefect_flow()