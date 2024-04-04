import logging
import json
import io

from port.api.commands import (CommandSystemDonate, CommandSystemExit, CommandUIRender)
import port.api.props as props
import port.chatgpt as chatgpt


LOG_STREAM = io.StringIO()

logging.basicConfig(
    stream=LOG_STREAM,
    level=logging.INFO,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")

# Headers
SUBMIT_FILE_HEADER = props.Translatable({
    "en": "Select your ChatGPT file", 
    "nl": "Selecteer uw ChatGPT bestand"
})

REVIEW_DATA_HEADER = props.Translatable({
    "en": "Your ChatGPT data", 
    "nl": "Uw ChatGPT gegevens"
})

RETRY_HEADER = props.Translatable({
    "en": "Try again", 
    "nl": "Probeer opnieuw"
})


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")


    platform_name = "ChatGPT"
    table_list = None

    while True:
        LOGGER.info("Prompt for file for %s", platform_name)
        yield donate_logs(f"{session_id}-tracking")

        file_prompt = generate_file_prompt("application/zip")
        file_result = yield render_page(SUBMIT_FILE_HEADER, file_prompt)

        if file_result.__type__ == "PayloadString":
            validation = chatgpt.validate_zip(file_result.value)
            print("VALIDATION")
            print(validation)

            # Flow logic
            # Happy flow: Valid DDP
            if validation.status_code.id == 0:
                LOGGER.info("Payload for %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

                extraction_result = extract_chatgpt(file_result.value)
                table_list = extraction_result
                break

            # Enter retry flow, reason: if DDP was not a ChatGPT DDP
            if validation.status_code.id != 0:
                LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_page(RETRY_HEADER, retry_confirmation(platform_name))

                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during retry flow")
                    yield donate_logs(f"{session_id}-tracking")
                    yield donate_status(f"{session_id}-SKIP-RETRY-FLOW", "SKIP_RETRY_FLOW")
                    break

        else:
            LOGGER.info("Skipped at file selection ending flow")
            yield donate_logs(f"{session_id}-tracking")
            yield donate_status(f"{session_id}-SKIP-FILE-SELECTION", "SKIP_FILE_SELECTION")
            break


    if table_list is not None:
        LOGGER.info("Prompt consent; %s", platform_name)
        yield donate_logs(f"{session_id}-tracking")
        prompt = create_consent_form(table_list)
        consent_result = yield render_page(REVIEW_DATA_HEADER, prompt)

        # Data was donated
        if consent_result.__type__ == "PayloadJSON":
            LOGGER.info("Data donated; %s", platform_name)
            yield donate(f"{session_id}-{platform_name}", consent_result.value)
            yield donate_logs(f"{session_id}-tracking")
            yield donate_status(f"{session_id}-DONATED", "DONATED")


    yield exit(0, "Success")
    yield render_end_page()


##################################################################

def create_consent_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, meta_tables=[])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))



def donate_status(filename: str, message: str):
    return donate(filename, json.dumps({"status": message}))



##################################################################
# Extraction function

# The A conditional group gets the visualizations 
def extract_chatgpt(chatgpt_zip: str) -> list[props.PropsUIPromptConsentFormTable]:

    tables_to_render = []
    
    df = chatgpt.conversations_to_df(chatgpt_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Your conversations with ChatGPT", "nl": "Uw gesprekken met ChatGPT"})
        table_description = props.Translatable({
            "en": "Table description", 
            "nl": "Table description"
        })
        wordcloud = {
            "title": {"en": "", "nl": ""},
            "type": "wordcloud",
            "textColumn": "message",
            "tokenize": True,
        }
        table = props.PropsUIPromptConsentFormTable("chatgpt_conversations", table_title, df, table_description, [wordcloud])
        tables_to_render.append(table)

    return tables_to_render



def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)



def render_page(header_text, body):
    header = props.PropsUIHeader(header_text)

    footer = props.PropsUIFooter()
    platform = "ChatGPT"
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)



def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)



##################################################################

def generate_file_prompt(extensions):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

def exit(code, info):
    return CommandSystemExit(code, info)
