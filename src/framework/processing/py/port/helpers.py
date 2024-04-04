import pandas as pd
import math
import re
import logging 
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def split_dataframe(df: pd.DataFrame, row_count: int) -> list[pd.DataFrame]:
    """
    Port has trouble putting large tables in memory. 
    Has to be expected. Solution split tables into smaller tables.
    I have tried non-bespoke table soluions they did not perform any better

    I hope you have an idea to make tables faster! Would be nice
    """
    # Calculate the number of splits needed.
    num_splits = int(len(df) / row_count) + (len(df) % row_count > 0)

    # Split the DataFrame into chunks of size row_count.
    df_splits = [df[i*row_count:(i+1)*row_count].reset_index(drop=True) for i in range(num_splits)]

    return df_splits


def convert_unix_timestamp(timestamp: str) -> str:
    out = timestamp
    try:
        out = datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(e)

    return  out



def dict_denester(
    inp: dict[Any, Any] | list[Any],
    new: dict[Any, Any] | None = None,
    name: str = "",
    run_first: bool = True,
) -> dict[Any, Any]:
    """
    Denest a dict or list, returns a new denested dict
    """

    if run_first:
        new = {}

    if isinstance(inp, dict):
        for k, v in inp.items():
            if isinstance(v, (dict, list)):
                dict_denester(v, new, f"{name}-{str(k)}", run_first=False)
            else:
                newname = f"{name}-{k}"
                new.update({newname[1:]: v})  # type: ignore

    elif isinstance(inp, list):
        for i, item in enumerate(inp):
            dict_denester(item, new, f"{name}-{i}", run_first=False)

    else:
        new.update({name[1:]: inp})  # type: ignore

    return new  # type: ignore



def find_item(d: dict[Any, Any],  key_to_match: str) -> str:
    """
    d is a denested dict
    match all keys in d that contain key_to_match

    return the value beloning to that key that is the least nested
    In case of no match return empty string

    example:
    key_to_match = asd

    asd-asd-asd-asd-asd-asd: 1
    asd-asd: 2
    qwe: 3

    returns 2

    This function is needed because your_posts_1.json contains a wide variety of nestedness per post
    """
    out = ""
    pattern = r"{}".format(f"^.*{key_to_match}.*$")
    depth = math.inf

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                depth_current_match = k.count("-")
                if depth_current_match < depth:
                    depth = depth_current_match
                    out = str(v)
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out



def find_items(d: dict[Any, Any],  key_to_match: str) -> list:
    """
    d is a denested dict
    find all items in a denested dict return list
    """
    out = []
    pattern = r"{}".format(f"^.*{key_to_match}.*$")
    depth = math.inf

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                out.append(str(v))
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out



