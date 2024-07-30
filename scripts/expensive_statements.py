import csv
import os
import json
from datetime import datetime, timedelta
from scripts.models import model_map
from scripts.utils import file_select



SUMMARY_KEYS = ['START_TIME', 'USER', 'STATEMENT_HASH', 'STATEMENT_TYPE', 'APPLICATION_NAME', 'DURATION_S', 'CPU_TIME_S', 'PARALLEL_FACTOR', 'MEMORY_SIZE', 'MODEL_ID', 'MODEL_NAME']
MDS_KEYS = ['START_TIME', 'USER', 'STATEMENT_HASH', 'APPLICATION_NAME', 'DURATION_S', 'CPU_TIME_S', 'PARALLEL_FACTOR', 'MEMORY_SIZE', 'MODEL_ID', 'MODEL_NAME', 'STORY_ID', 'STORY_NAME', 'WIDGET_ID', 'MDS_TYPE', 'DIMENSIONS', 'READ_MODE', 'MEASURES', 'MEASURE_TYPE', 'INPUT_RECORDS']
DA_KEYS = ['START_TIME', 'USER', 'STATEMENT_HASH', 'APPLICATION_NAME', 'CPU_TIME_S', 'DURATION_S', 'PARALLEL_FACTOR', 'MEMORY_SIZE', 'STATEMENT_TYPE', 'MODEL_ID', 'MODEL_NAME', 'ACTION_FOUND', 'VERSION_UUID', 'ACTION_TYPE', 'ACTION_NAME', 'ACTION_STEP', 'ROWS_CHANGED_SEMANTIC', 'ROWS_CHANGED_TECHNICAL', 'VERSION_SIZE']

DATA_ACTION_TYPES = ['DATA_ACTION', 'EPM_ACTION', 'EPM_BACKUP', 'EPM_CLOSE']
EPM_INTERNAL_ACTIONS = ['publish','query_based_copy','populate_single_version', 'close', 'init']

def get_mds_metadata(statement):
    metadata = {}
    if type(statement) == dict:
        stmt_json = statement
    else:
        stmt_json = json.loads(statement[statement.find('{'):])
    if 'ClientInfo' in stmt_json:
        if 'StoryName' in stmt_json['ClientInfo']['Context']:
            metadata['STORY_NAME'] = stmt_json['ClientInfo']['Context']['StoryName']
        if 'StoryId' in stmt_json['ClientInfo']['Context']:
            metadata['STORY_ID'] = stmt_json['ClientInfo']['Context']['StoryId']
        else:
            metadata['STORY_ID'] = "N/A"
        if 'WidgetId' in stmt_json['ClientInfo']['Context']:
            metadata['WIDGET_ID'] = "|".join(stmt_json['ClientInfo']['Context']['WidgetId'])
        else:
            metadata['WIDGET_ID'] = "N/A"
    else:
        metadata['STORY_NAME'] = "N/A"
        metadata['STORY_ID'] = "N/A"
        metadata['WIDGET_ID'] = "N/A"

    if 'Analytics' in stmt_json:
        data_src = stmt_json['Analytics']['DataSource']
        metadata['MODEL_ID'] = data_src['ObjectName'][5:31]
        metadata['MODEL_NAME'] = model_map[metadata['MODEL_ID']] if metadata['MODEL_ID'] in model_map else "N/A"
        definition = stmt_json['Analytics']['Definition']
        dimensions = definition['Dimensions']

        # # mdlz entity filter
        # if "DynamicFilter" in definition:
        #     statement_filter = definition['DynamicFilter']
        #     metadata['MEA_FILTER'] = "[ENTITY].[HIERARCHY].&[MEU]" in str(statement_filter)
        #     metadata['FILTER_VAL'] = str(statement_filter)

        # else:
        #     metadata['MEA_FILTER'] = "None"
        #     metadata['FILTER_VAL'] = "None"

        if len(dimensions) == 1:
            metadata['MDS_TYPE'] = 'Master'
            metadata['DIMENSIONS'] = dimensions[0]['Name']
            if 'ReadMode' in dimensions[0]:
                metadata['READ_MODE'] = dimensions[0]['ReadMode']
            else:
                metadata['READ_MODE'] = "N/A"
            metadata['MEASURES'] = "N/A"
            metadata['MEASURE_TYPE'] = "N/A"
            metadata['INPUT_RECORDS'] = ''
        else:
            measures = []
            measure_type = []
            dim_list = []
            read_mode_list = []
            if "NewValues" in definition:
                metadata['MDS_TYPE'] = 'DataInput'
                metadata['INPUT_RECORDS'] = len(definition['NewValues'])

            else:
                metadata['MDS_TYPE'] = 'Fact Data'
                metadata['INPUT_RECORDS'] = ''
            for dim in dimensions:
                dim_list.append(dim['Name'])
                if dim['Name'] == 'CustomDimension1':
                    read_mode_list.append('MEASURE')
                    for member in dim['Members']:
                        if 'MemberName' in member:
                            measures.append(member['MemberName'])
                            measure_type.append('Stored')
                            continue
                        if 'CurrencyTranslationName' in member:
                            measures.append(member['Name'])
                            measure_type.append('ConversionMeasure')
                            continue
                        if "MemberOperand" in member:
                            measures.append(member['MemberOperand']['Value'])
                            measure_type.append('Other')
                        else:
                            measures.append(f'FORMULA - {member["Name"]}')
                        measure_type.append('Formula')
                    metadata['MEASURES'] = "|".join(measures)
                    metadata['MEASURE_TYPE'] = "|".join(measure_type)
                else:
                    read_mode_list.append(dim['ReadMode'])
            metadata['DIMENSIONS'] = "|".join(dim_list)
            metadata['READ_MODE'] = "|".join(read_mode_list)
    
    elif 'Batch' in stmt_json:
        metadata['MDS_TYPE'] = 'Batch'
        metadata['MEASURES'] = "N/A"
        metadata['MEASURE_TYPE'] = "N/A"
        metadata['INPUT_RECORDS'] = ''
        all_batch_metadata = []
        for batch in stmt_json['Batch']:
            all_batch_metadata.append(get_batch_type(batch))
        metadata['DIMENSIONS'] = '|'.join(all_batch_metadata)
            
    return metadata

def get_batch_type(batch_json):
    batch_metadata = {}
    if batch_json.get('Analytics'):
        dimensions = batch_json['Analytics']['Definition']['Dimensions']
        if len(dimensions) > 1:
            return 'FactRead'
        else:
            return 'MasterRead'

def is_new_row(line):
    if line[-2:] == b'\r\n':
        return True
    else: 
        return False

def fix_file(file_name):
    file = []
    temp = ""
    dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(dir, file_name)
    infile = open(file_path, 'rb')

    for line_bytes in infile.readlines():
        line_str = line_bytes.decode('utf-8')
        if not is_new_row(line_bytes):
            temp += str(line_str[:-1])
            continue
        if temp:
            temp += line_str
            if len(temp.split(';')) > 41:
                temp = fix_statement_fields(temp)
            file.append(temp)
            temp = ""
            continue
        if len(line_str.split(';')) != 41:
            raise ValueError(f"Error Parsing Line: Expected 41 fields, found {len(line_str.split(';'))}") 
        file.append(line_str)
    return file

def fix_statement_fields(string):
    """Function to handle extra ; delimiters usually coming from STATEMENT_STRING field it will consolidate 
    the extra fields back into STATEMENT_STRING and wrap in "" so csvReader can ignore.
    """
    temp = []
    temp2 = []
    fields = string.split(';')
    extra_fields = len(fields) - 41
    temp.append(fields[17])     #Get first component of STATEMENT_STRING expected to be 17th field
    for offset in range(extra_fields):
        #get remaining fields values and delete original unexpected field
        index = 18
        temp.append(fields[index])
        del fields[18]

    for i in range(len(temp)):
        #Need to escape double quote values in the string as we will wrap string in double quotes below
        temp[i] = temp[i].replace('"', '""')    

    temp_str = ';'.join(temp)
    temp_str = '"' + temp_str + '"' #Add quotes so csv.DictReader() will ignore the delimiter values
    fields[17] = temp_str
    return ';'.join(fields)

def write_to_csv(file_name, keys, list):
    with open(file_name, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(list)     

def get_stmt_type(statement):
    if statement.startswith('CALL SYS.EXECUTE_MDS'):
        return 'MDS'
    elif statement.startswith('CALL EXECUTE_MDS'):
        return 'MDS_OTHER'
    elif statement.startswith("CALL EPM_MODEL_COMMAND('actions'"):
        return 'DATA_ACTION'
    elif statement.startswith("CALL EPM_MODEL_COMMAND('action'"):
        return 'EPM_ACTION'
    elif statement.startswith("CALL EPM_MODEL_COMMAND('close'"):
        return "EPM_CLOSE"
    elif 'sap.fpa.services.planningScript::' in statement:
        return 'DA_PROCEDURE'
    elif "sap.fpa.services.dataLocking::LOCKS_INDEX" in statement:
        return 'LOCKING'
    elif "sap.fpa.services.dataLocking" in statement:
        return 'LOCKING_OTHER'
    elif "$MDX//TENANT_B" in statement:
        return "HIERARCHY"
    elif "PDC:0::TEMPORARY" in statement:
        return "EPM_BACKUP"
    else:
        return "OTHER"
    
def get_expensive_statements(file_name):
    dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(dir, file_name)

    with open(file_path, 'rb') as file:
        content = file.read()

    raw_file = []
    file_bytes = content.decode('utf-8').split('\r\n')

    count = 0
    for row in file_bytes:
        # Check if additional new line characters exist
        count += 1
        new_lines = row.split('\n')
        if len(new_lines) > 1:
            row = ' '.join(new_lines)
        field_count = len(row.split(';'))
        if field_count == 41:
            raw_file.append(row)
            continue
        if field_count < 41:
            continue
            # raise ValueError(f"Error Parsing Line: Expected 41 fields, found {field_count}")
        # Extra fields identified, most likely from STATEMENT_STRING
        fixed_row = fix_statement_fields(row)
        raw_file.append(fixed_row)
    return raw_file

def get_model_from_params(params):
    view = params.split(',')[1].strip()
    namespace_model = view.split('/')[1]
    model_id = namespace_model.split('.')[2]
    return model_id

if __name__ == "__main__":

    # selecting statements file
    file_name = file_select()
    # file_name = "C:/Users/frank.naylor@sap.com/OneDrive - SAP SE/2024/Mondelez/April Monitoring/Q2/Q2_4_26_CPU.csv"

    summary_out_file = file_name.split('.csv')[0] + "_summary.csv"
    mds_out_file = file_name.split('.csv')[0] + "_mds_summary.csv"
    da_out_file = file_name.split('.csv')[0] + "_da_summary.csv"

    # selecting actions file
    raw_file = get_expensive_statements(file_name)
    csv.field_size_limit(13107200)
    csv_reader = csv.DictReader(raw_file, delimiter=';')
    file = []
    a = 0
    for row in csv_reader:
        if len(row) != 41:
            raise ValueError(f"Error Parsing Line: Expected 41 fields, found {len(row)}")
        file.append(row)


    statement = {}
    statement_hash_map = {}
    summary_list = []

    for row in file:

        if row['STATEMENT_HASH'] not in statement_hash_map:
            statement_hash_map[row['STATEMENT_HASH']] = row['STATEMENT_STRING']

        summary_row = {}
        summary_row['START_TIME'] = datetime.strptime(row['START_TIME'], "%Y-%m-%d %H:%M:%S.%f000")
        summary_row['USER'] = row['APP_USER']
        summary_row['STATEMENT_HASH'] = row['STATEMENT_HASH']
        # summary_row['PARAMETERS'] = row['PARAMETERS']
        # summary_row['APPLICATION_SOURCE'] = row['APPLICATION_SOURCE']
        summary_row['APPLICATION_NAME'] = row['APPLICATION_NAME']
        summary_row['CPU_TIME_S'] = round(float(row['CPU_TIME'])/1000000,1)
        summary_row['DURATION_S'] = round(float(row['DURATION_MICROSEC'])/1000000,1)
        summary_row['PARALLEL_FACTOR'] = round(float(row['CPU_TIME'])/float(row['DURATION_MICROSEC']),1)
        summary_row['MEMORY_SIZE'] = float(row['MEMORY_SIZE'])
        summary_row['STATEMENT_TYPE'] = get_stmt_type(row['STATEMENT_STRING'])
        
        # Derive Model Name from Model ID Lookup
        if '_ACTION' in summary_row['STATEMENT_TYPE']:
            summary_row['MODEL_ID'] = get_model_from_params(row['PARAMETERS'])
            summary_row['MODEL_NAME'] = model_map[summary_row['MODEL_ID']] if summary_row['MODEL_ID'] in model_map else "NO_MAPPING"

        elif summary_row['STATEMENT_TYPE'] == 'MDS':
            qs_text = row['PARAMETERS'].split(",")[3]
            if 'View' in row['PARAMETERS']:
                summary_row['MODEL_ID'] = 'View'
                summary_row['MODEL_NAME'] = 'View'
            else:
                summary_row['MODEL_ID'] = qs_text.split('/')[2].split('_qs')[0]
                summary_row['MODEL_NAME'] = model_map[summary_row['MODEL_ID']] if summary_row['MODEL_ID'] in model_map else "NO_MAPPING"
        else:
            summary_row['MODEL_ID'] = 'N/A'
            summary_row['MODEL_NAME'] = 'N/A'

        summary_list.append(summary_row)
        

    # Write Summary Output
    write_to_csv(summary_out_file, SUMMARY_KEYS,  summary_list)

    # Step 2 - MDS Summary
    mds_summary = []
    for row in summary_list:
        mds_statement_info = {key: row[key] for key in MDS_KEYS[:10]}
        if row['STATEMENT_TYPE'] != 'MDS':
            continue
        statement_string = statement_hash_map[row['STATEMENT_HASH']]
        metadata = get_mds_metadata(statement_string)
        if not metadata:
            print(f"Could not derive metadata for STATEMENT_HASH: {row['STATEMENT_HASH']} please check manually")
        for key, value in metadata.items():
            mds_statement_info[key] = value
        mds_summary.append(mds_statement_info)

    # Write MDS Summary
    write_to_csv(mds_out_file, MDS_KEYS, mds_summary)




    # Step 3 - DA Summary

    file_name = file_select()
    # file_name = 'C:/Users/frank.naylor@sap.com/OneDrive - SAP SE/2024/Mondelez/April Monitoring/Q2/Action_4_26.csv'
    dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(dir, file_name)

    actions = []
    with open(file_path, newline='',encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile,delimiter=';')
        for row in reader:
            row['START_TIME'] = datetime.strptime(row['START_TIME'], "%Y-%m-%d %H:%M:%S.%f000")
            row['END_TIME'] = datetime.strptime(row['END_TIME'], "%Y-%m-%d %H:%M:%S.%f000")
            actions.append(row)
    sorted_actions = sorted(actions, key=lambda d: d['START_TIME'])
    user_action_mapping = {}
    for action in sorted_actions:
        if action['USER'] in user_action_mapping:
            user_action_mapping[action['USER']].append(action)
        else:
            user_action_mapping[action['USER']] = [action]
    
    da_summary = []
    for statement in summary_list:
        if statement['STATEMENT_TYPE'] not in DATA_ACTION_TYPES:
            continue
        found = False
        action_metadata = statement
        user_actions = user_action_mapping[statement['USER']]
        for action in user_actions:
            if action['USER'] != statement['USER']:
                continue
            if action['INTERACTION_TYPE'] == 'action_sequence':
                continue
            delta = abs(statement['START_TIME'] - action['START_TIME'])
            threshold = timedelta(seconds=0.5)
            
            if not delta < threshold:
                if action['START_TIME'] > statement["START_TIME"]:
                    break
                continue

            found = True
            action_metadata = {key: statement[key] for key in DA_KEYS[:11]}
            action_metadata['ACTION_FOUND'] = True
            action_metadata['VERSION_UUID'] = action['VERSION_UUID']
            action_metadata['ROWS_CHANGED_SEMANTIC'] = int(action['ROWS_CHANGED_SEMANTIC']) if action['ROWS_CHANGED_SEMANTIC'] else 0
            action_metadata['ROWS_CHANGED_TECHNICAL'] = int(action['ROWS_CHANGED_TECHNICAL']) if action['ROWS_CHANGED_TECHNICAL'] else 0
            action_metadata['VERSION_SIZE'] = int(action['VERSION_SIZE']) if action['VERSION_SIZE'] else 0
            action_metadata['ACTION_TYPE'] = action['INTERACTION_TYPE'] 

            if action['INTERACTION_NAME'] in EPM_INTERNAL_ACTIONS:
                action_metadata['ACTION_NAME'] = action['INTERACTION_NAME']
                action_metadata['ACTION_STEP'] = 'N/A'
                break
            elif "PLANNINGSEQUENCE_EXECUTION" in action['DESCRIPTION']:
                da = json.loads(action['DESCRIPTION'])
                action_metadata['ACTION_NAME'] = da['dataAction']
                action_metadata['ACTION_STEP'] = da['step']
                break
            else:
                action_metadata['ACTION_NAME'] = 'N/A'
                action_metadata['ACTION_STEP'] = 'N/A'
                break
        if not found:
            action_metadata['ACTION_FOUND'] = False
            for key in DA_KEYS[11:]:
                if key in action_metadata:
                    continue
                else:
                    action_metadata[key] = 'N/A'
        da_summary.append(action_metadata)

    # Write DA Summary File
    write_to_csv(da_out_file, DA_KEYS, da_summary)