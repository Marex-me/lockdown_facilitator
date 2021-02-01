import os
import re


def get_dir_files_names(dir_name: str) -> list:
    raw_list = os.listdir(dir_name)
    names_pat = re.compile(r'(.+)\.[^.]*')
    temp_path = re.compile(r'^\.~.+')
    return [names_pat.match(name).group(1) for name in raw_list if not temp_path.match(name)]


def align_multi_words_names(name: str) -> str:
    words_list = list(map(lambda x: x.lower(), name.strip().replace('  ', ' ').split(' ')))
    if len(words_list[0]) == 1:
        return ' '.join([words_list[0][0].upper()] + words_list[1:])
    return ' '.join([words_list[0][0].upper() + words_list[0][1:]] + words_list[1:])
