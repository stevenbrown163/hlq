# insert executable line here

import sys
import argparse
import re
LOG=0

#### Utilities
# Multiple a list of lists
def recursivesplat(textlist):
    if len(textlist) == 0:
        return [[]]
    newlist = []
    restlist = recursivesplat(textlist[1:])
    for str in textlist[0]:
        for reststr in restlist:
            newlist.append([str]+reststr)
    return newlist

# Out source the pulling of data fields from the format for multiple use
def data_from_format(format):
    return format[2:-1].split('|')[0].strip()


# Format args processing
def process_format(formattext):
    format_obj = {
        'text': formattext,
        'data': [], 
        'formats': re.findall(r"%{[^}]*}", formattext)
    }
    format_obj['data'] = [data_from_format(f) for f in format_obj['formats']]
    return format_obj

#### Gather the data needed from the msg based on the format
def work_msg(msg_struct, parsed_formats):
    global LOG
    # Aggregate all the data fields we need
    data_sets = [a['data'] for a in parsed_formats]
    data_sets = sum(data_sets, [])
    data_obj = {}
    msg_now = ""

    for data_elem in data_sets:
        seps_list = msg_struct["SEPERATORS"]
        if data_elem not in data_obj.keys():
            data_points = data_elem.split(".")
            msg_now = msg_struct
            # Get list of segments with matching segment name
            if len(data_points) > 0:
                # get first value in list and remove from the list ("MSH" in ["MSH", 9, 0])
                data_seg_name = data_points.pop(0)
                if data_seg_name in msg_now.keys():
                    msg_now = msg_now[data_seg_name]
            while len(data_points) > 0 and len(seps_list) > 0:
                # TODO SAFETY CHECK - is this an integer?
                data_seg_name = int(data_points.pop(0))
                sep = seps_list[0]
                seps_list = seps_list[1:]
                msg_next = []
                if LOG == 1:
                    print('STATE')
                    print(f'sep:{sep} sep_list: {seps_list} data_elem: {data_elem}')
                    print(f'msg_now: {msg_now}')
                for msg in msg_now:
                    #print(f'msg1: {msg}')
                    msg = msg.split(sep)
                    #print(f'msg2: {msg}')
                    if data_seg_name < len(msg):
                        msg_next.append(msg[data_seg_name])
                msg_now = msg_next
            data_obj.update({data_elem: msg_now})
    return data_obj


def apply_formatter(data, formatter):
    if formatter.startswith("reverse"):
            return data[::-1]
    return data


#### For each format text
    #### for each formatter
        #### for each data element in the data source for the format
            #### format the data element string however appropriate
#### Splat all the formatted data elements
#### For each iteration in the formatted data elements
    #### replace the full format text with the corresponding formatted data element
    #### print to stdout
def format_data(all_data_values, parsed_formats):
    global LOG
    output_list_of_lists = []
    for format_list in parsed_formats:
        ordered_list_of_formatted_data = []
        output_sub_list = []
        for format in format_list["formats"]:
            data_elem = data_from_format(format)
            formatters_list = format[2:-1].split("|")[1:]
            data_list = all_data_values[data_elem]
            for formatter in formatters_list:
                formatter = formatter.strip()
                data_list = [apply_formatter(d, formatter) for d in data_list]
            ordered_list_of_formatted_data.append(data_list)
        splatted_list_of_formatted_data = recursivesplat(ordered_list_of_formatted_data)

        for i in splatted_list_of_formatted_data:
            new_text = format_list["text"]
            for format_index in range(len(format_list["formats"])):
                replace_search = format_list["formats"][format_index]
                replace_with = i[format_index]
                new_text = new_text.replace(replace_search, replace_with)
            
            output_sub_list.append(new_text)
        output_list_of_lists.append(output_sub_list)
    return recursivesplat(output_list_of_lists)

#### Parse arguments for what to do
parser = argparse.ArgumentParser()

# args.formats
parser.add_argument('formats', 
                    metavar="FORMATS", 
                    type=str,
                    nargs='*', 
                    help='String formats default behaviour')

#args.delim
parser.add_argument('-d', '--delim', 
                    default=' ',
                    help='output string separator')
args = parser.parse_args()

# Get out early if no formats
if len(args.formats) == 0:
    exit()
parsed_formats = [process_format(a) for a in args.formats]
if LOG == 1:
    print(parsed_formats)

#### Read in the data
for msg in sys.stdin:
    segs = msg.split("\r")
    msg_struct = {}
    for seg in segs:
        seg_name = seg[0:3]
        if seg_name == "MSH":
            msg_struct["SEPERATORS"] = seg[3:8]
        if seg_name in msg_struct.keys():
            msg_struct[seg_name].append(seg)
        else:
            msg_struct[seg_name] = [seg]
    # Object with the lists of data per field name
    all_data_values = work_msg(msg_struct, parsed_formats)
    formatted_data_values = format_data(all_data_values, parsed_formats)
    for output in formatted_data_values:
        print(args.delim.join(output))
