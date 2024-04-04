"""
DDP extract ChatGPT module
"""
from pathlib import Path
import logging
import zipfile

import pandas as pd

import port.unzipddp as unzipddp
import port.helpers as helpers

from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=["chat.html", "conversations.json", "message_feedback.json", "model_comparisons.json", "user.json"]
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid zip", message="Valid zip"),
    StatusCode(id=1, description="Bad zipfile", message="Bad zipfile"),
]


def validate_zip(zfile: Path) -> ValidateInput:
    """
    Make sure you always set a status code
    """

    validate = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".html", ".json"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        if validate.infer_ddp_category(paths):
            validate.set_status_code_by_id(0)
        else:
            validate.set_status_code_by_id(1)
    except zipfile.BadZipFile:
        validate.set_status_code_by_id(1)

    return validate



def conversations_to_df(chatgpt_zip: str)  -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(chatgpt_zip, "conversations.json")
    conversations = unzipddp.read_json_from_bytes(b)

    datapoints = []
    out = pd.DataFrame()

    try:
        for conversation in conversations:
            title = conversation["title"]
            for _, turn in conversation["mapping"].items():

                denested_d = helpers.dict_denester(turn)
                is_hidden = helpers.find_item(denested_d, "is_visually_hidden_from_conversation")
                if is_hidden != "True":
                    role = helpers.find_item(denested_d, "role")
                    message = "".join(helpers.find_items(denested_d, "part"))
                    model = helpers.find_item(denested_d, "-model_slug")
                    time = helpers.convert_unix_timestamp(helpers.find_item(denested_d, "create_time"))

                    datapoint = {
                        "conversation title": title,
                        "role": role,
                        "message": message,
                        "model": model,
                        "time": time,
                    }
                    datapoints.append(datapoint)

        out = pd.DataFrame(datapoints)

    except Exception as e:
        logger.error("Data extraction error: %s", e)
        
    return out


