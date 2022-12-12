from .main import DFToSQL
date_regexes: list[str] = [r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([+-]\d{2}:\d{2}|Z)?$']
number_regexes: list[str] = [r'^([1-9][0-9]+|[0-9])(\.[0-9]+$|$)']