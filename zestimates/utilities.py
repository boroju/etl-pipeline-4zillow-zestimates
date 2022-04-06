import csv
import inflection
import json

from io import TextIOBase, StringIO
from typing import List


class CaseConverters:
    @staticmethod
    def pascal_to_snake(input_str: str):
        return inflection.underscore(input_str)

    @staticmethod
    def snake_to_pascal(input_str: str):
        return inflection.camelize(input_str)


def json_to_csv(fp: TextIOBase, include_header_row: bool = True) -> StringIO:
    """
    Function for converting a file-like object from json to csv.
    :param fp: A file-like object. Specifically a TextIOBase object.
    :param include_header_row: boolean for making the first row a header row or not.
    :return: StringIO containing a csv.
    """
    # workaround until I can figure out why json.load(input) blows up.
    # note: json_dict is actually a list of dicts.
    # thoughts: should I use an object_hook or custom decoder to make sure the deserialized json is always
    # a list that I can iterate through?
    json_list: List = json.loads(fp.getvalue())
    output_buf = StringIO()
    csv_writer = csv.writer(output_buf)

    if not isinstance(json_list, List):
        json_list = [json_list]

    if include_header_row:
        # this doesn't work unless json_list is actually a list and not a single dict
        csv_writer.writerow(json_list[0].keys())

    for json_dict in json_list:
        csv_writer.writerow(json_dict.values())

    return output_buf


def dict_to_csv(input_dict: dict, include_header_row: bool = True) -> StringIO:
    """
    Convert a dictionary to a csv file-like object.

    :param input_dict: The dictionary to be converted to csv.
    :param include_header_row: boolean for making the first row a header row or not.
    :return: StringIO file-like object containing a csv.
    """
    output_buf = StringIO()
    csv_writer = csv.writer(output_buf)

    if include_header_row:
        csv_writer.writerow(input_dict.keys())
    csv_writer.writerow(input_dict.values())

    return output_buf


