import itertools
import port.api.props as props
from port.api.commands import CommandSystemDonate, CommandUIRender

import pandas as pd
import zipfile
import json
import datetime
from collections import defaultdict, namedtuple
from contextlib import suppress

##########################
# TikTok file processing #
##########################

filter_start = datetime.datetime(2021, 1, 1)
filter_end = datetime.datetime(2025, 1, 1)

datetime_format = "%Y-%m-%d %H:%M:%S"


def parse_datetime(value):
    return datetime.datetime.strptime(value, datetime_format)


def get_in(data_dict, *key_path):
    for k in key_path:
        data_dict = data_dict.get(k, None)
        if data_dict is None:
            return None
    return data_dict


def get_list(data_dict, *key_path):
    result = get_in(data_dict, *key_path)
    if result is None:
        return []
    return result


def get_dict(data_dict, *key_path):
    result = get_in(data_dict, *key_path)
    if result is None:
        return {}
    return result


def get_string(data_dict, *key_path):
    result = get_in(data_dict, *key_path)
    if result is None:
        return ""
    return result


def cast_number(data_dict, *key_path):
    value = get_in(data_dict, *key_path)
    if value is None or value == "None":
        return 0
    return value


def get_activity_video_browsing_list_data(data):
    return get_list(data, "Activity", "Video Browsing History", "VideoList")


def get_comment_list_data(data):
    return get_in(data, "Comment", "Comments", "CommentsList")


def get_date_filtered_items(items):
    for item in items:
        timestamp = parse_datetime(item["Date"])
        if timestamp < filter_start or timestamp > filter_end:
            continue
        yield (timestamp, item)


def get_count_by_date_key(timestamps, key_func):
    """Returns a dict of the form (key, count)

    The key is determined by the key_func, which takes a datetime object and
    returns an object suitable for sorting and usage as a dictionary key.

    The returned list is sorted by key.
    """
    item_count = defaultdict(int)
    for timestamp in timestamps:
        item_count[key_func(timestamp)] += 1
    return sorted(item_count.items())


def get_all_first(items):
    return (i[0] for i in items)


def hourly_key(date):
    return date.replace(minute=0, second=0, microsecond=0)


def daily_key(date):
    return date.date()


def get_sessions(timestamps):
    """Returns a list of tuples of the form (start, end, duration)

    The start and end are datetime objects, and the duration is a timedelta
    object.
    """
    timestamps = list(sorted(timestamps))
    if len(timestamps) == 0:
        return []
    if len(timestamps) == 1:
        return [(timestamps[0], timestamps[0], datetime.timedelta(0))]

    sessions = []
    start = timestamps[0]
    end = timestamps[0]
    for prev, cur in zip(timestamps, timestamps[1:]):
        if cur - prev > datetime.timedelta(minutes=5):
            sessions.append((start, end, end - start))
            start = cur
        end = cur
    sessions.append((start, end, end - start))
    return sessions


def load_tiktok_data(json_file):
    data = json.load(json_file)
    if not get_user_name(data):
        raise IOError("Unsupported file type")
    return data


def get_json_data_from_zip(zip_file):
    with zipfile.ZipFile(zip_file, "r") as zip:
        for name in zip.namelist():
            if not name.endswith(".json"):
                continue
            with zip.open(name) as json_file:
                with suppress(IOError, json.JSONDecodeError):
                    return [load_tiktok_data(json_file)]
    return []


def get_json_data_from_file(file_):
    # TikTok exports can be a single JSON file or a zipped JSON file
    try:
        with open(file_) as f:
            return [load_tiktok_data(f)]
    except (json.decoder.JSONDecodeError, UnicodeDecodeError):
        return get_json_data_from_zip(file_)


def filtered_count(data, *key_path):
    items = get_list(data, *key_path)
    filtered_items = get_date_filtered_items(items)
    return len(list(filtered_items))


def get_user_name(data):
    return get_in(data, "Profile", "Profile Information", "ProfileMap", "userName")


def get_chat_history(data):
    return get_dict(data, "Direct Messages", "Chat History", "ChatHistory")


def flatten_chat_history(history):
    return itertools.chain(*history.values())


def filter_by_key(items, key, value):
    return filter(lambda item: item[key] == value, items)


def exclude_by_key(items, key, value):
    """
    Return a filtered list where items that match key & value are excluded.
    """
    return filter(lambda item: item[key] != value, items)


def map_to_timeslot(series):
    return series.map(lambda hour: f"{hour}-{hour+1}")


def extract_summary_data(data):
    user_name = get_user_name(data)
    chat_history = get_chat_history(data)
    flattened = flatten_chat_history(chat_history)
    direct_messages_in_period = list(get_date_filtered_items(flattened))
    sent_count = len(
        list(
            filter(lambda item: item[1]["From"] == user_name, direct_messages_in_period)
        )
    )
    received_count = len(
        list(
            filter(
                lambda item: item[1]["From"] != user_name,
                direct_messages_in_period,
            )
        )
    )

    summary_data = {
        "Description": [
            "Followers",
            "Following",
            "Likes received",
            "Videos posted",
            "Likes given",
            "Comments posted",
            "Messages sent",
            "Messages received",
            "Videos watched",
        ],
        "Number": [
            filtered_count(data, "Activity", "Follower List", "FansList"),
            filtered_count(data, "Activity", "Following List", "Following"),
            cast_number(
                data,
                "Profile",
                "Profile Information",
                "ProfileMap",
                "likesReceived",
            ),
            filtered_count(data, "Video", "Videos", "VideoList"),
            filtered_count(data, "Activity", "Like List", "ItemFavoriteList"),
            filtered_count(data, "Comment", "Comments", "CommentsList"),
            sent_count,
            received_count,
            filtered_count(data, "Activity", "Video Browsing History", "VideoList"),
        ],
    }

    return ExtractionResult(
        "tiktok_summary",
        props.Translatable(
            {"en": "Summary information", "nl": "Samenvatting gegevens"}
        ),
        pd.DataFrame(summary_data),
        props.Translatable(
            {
                "en": "Here we can now add a description per table to better help the user understand what the data is about. The description should be short. Say about one to three sentences",
                "nl": "Hier kunnen we nu een beschrijving per tabel toevoegen om de gebruiker beter te helpen begrijpen waar de gegevens over gaan. De beschrijving moet kort zijn. Zeg ongeveer één tot drie zinnen",
            }
        ),
        None,
    )


def extract_videos_viewed(data):
    videos = get_all_first(
        get_date_filtered_items(get_activity_video_browsing_list_data(data))
    )
    video_counts = get_count_by_date_key(videos, hourly_key)
    if not video_counts:
        return

    df = pd.DataFrame(video_counts, columns=["Date", "Videos"])
    df["Timeslot"] = map_to_timeslot(df["Date"].dt.hour)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d %H:00:00")
    df = df.reindex(columns=["Date", "Timeslot", "Videos"])

    visualizations = [
        props.PropsUIChartVisualization(
            title=props.Translatable(
                {
                    "en": "Average number of videos watched per hour of the day",
                    "nl": "Gemiddeld aantal videos bekeken per uur van de dag",
                }
            ),
            type="bar",
            group=props.PropsUIChartGroup(
                column="Date", label="Hour of the day", dateFormat="hour_cycle"
            ),
            values=[
                props.PropsUIChartValue(
                    column="Videos",
                    label="Average nr. of videos",
                    aggregate="mean",
                    addZeroes=True,
                )
            ],
        )
    ]

    return ExtractionResult(
        "tiktok_videos_viewed",
        props.Translatable({"en": "Video views", "nl": "Videos gezien"}),
        df,
        None,
        visualizations,
    )


def extract_video_posts(data):
    video_list = get_in(data, "Video", "Videos", "VideoList")
    if video_list is None:
        return
    videos = get_date_filtered_items(video_list)
    post_stats = defaultdict(lambda: defaultdict(int))
    for date, video in videos:
        hourly_stats = post_stats[hourly_key(date)]
        hourly_stats["Videos"] += 1
        hourly_stats["Likes received"] += int(video["Likes"])

    df = pd.DataFrame(post_stats).transpose()
    df["Date"] = df.index.strftime("%Y-%m-%d")
    df["Timeslot"] = map_to_timeslot(df.index.hour)
    df = df.reset_index(drop=True)
    df = df.reindex(columns=["Date", "Timeslot", "Videos", "Likes received"])

    return ExtractionResult(
        "tiktok_posts",
        props.Translatable({"en": "Video posts", "nl": "Video posts"}),
        df,
        None,
        None,
    )


def extract_comments_and_likes(data):
    comments = get_all_first(
        get_date_filtered_items(get_list(data, "Comment", "Comments", "CommentsList"))
    )
    comment_counts = get_count_by_date_key(comments, hourly_key)

    likes_given = get_all_first(
        get_date_filtered_items(
            get_list(data, "Activity", "Like List", "ItemFavoriteList")
        )
    )
    likes_given_counts = get_count_by_date_key(likes_given, hourly_key)
    if not likes_given_counts:
        return

    df1 = pd.DataFrame(comment_counts, columns=["Date", "Comment posts"]).set_index(
        "Date"
    )
    df2 = pd.DataFrame(likes_given_counts, columns=["Date", "Likes given"]).set_index(
        "Date"
    )

    df = pd.merge(df1, df2, left_on="Date", right_on="Date", how="outer").sort_index()
    df["Timeslot"] = map_to_timeslot(df.index.hour)
    df["Date"] = df.index.strftime("%Y-%m-%d %H:00:00")
    df = (
        df.reindex(columns=["Date", "Timeslot", "Comment posts", "Likes given"])
        .reset_index(drop=True)
        .fillna(0)
    )
    df["Comment posts"] = df["Comment posts"].astype(int)
    df["Likes given"] = df["Likes given"].astype(int)

    visualizations = [
        props.PropsUIChartVisualization(
            title=props.Translatable(
                {
                    "en": "Average number of comments and likes for every hour of the day",
                    "nl": "Gemiddeld aantal comments en likes per uur van de dag",
                }
            ),
            type="bar",
            group=props.PropsUIChartGroup(
                column="Date", label="Hour of the day", dateFormat="hour_cycle"
            ),
            values=[
                props.PropsUIChartValue(
                    label="Average nr. of comments",
                    column="Comment posts",
                    aggregate="mean",
                    addZeroes=True,
                ),
                props.PropsUIChartValue(
                    label="Average nr. of posts",
                    column="Likes given",
                    aggregate="mean",
                    addZeroes=True,
                ),
            ],
        )
    ]

    return ExtractionResult(
        "tiktok_comments_and_likes",
        props.Translatable({"en": "Comments and likes", "nl": "Comments en likes"}),
        df,
        None,
        visualizations,
    )


def extract_session_info(data):
    session_paths = [
        ("Video", "Videos", "VideoList"),
        ("Activity", "Video Browsing History", "VideoList"),
        ("Comment", "Comments", "CommentsList"),
    ]

    item_lists = [get_list(data, *path) for path in session_paths]
    dates = get_all_first(get_date_filtered_items(itertools.chain(*item_lists)))

    sessions = get_sessions(dates)
    df = pd.DataFrame(sessions, columns=["Start", "End", "Duration"])
    df["Start"] = df["Start"].dt.strftime("%Y-%m-%d %H:%M")
    df["Duration (in minutes)"] = (df["Duration"].dt.total_seconds() / 60).round(2)
    df = df.drop("End", axis=1)
    df = df.drop("Duration", axis=1)

    visualizations = [
        props.PropsUIChartVisualization(
            title=props.Translatable(
                {
                    "en": "Number of minutes spent on TikTok",
                    "nl": "Aantal minuten besteed aan TikTok",
                }
            ),
            type="line",
            group=props.PropsUIChartGroup(
                column="Start", label="Date", dateFormat="auto"
            ),
            values=[
                props.PropsUIChartValue(
                    label="Nr. of minutes",
                    column="Duration (in minutes)",
                    aggregate="sum",
                    addZeroes=True,
                )
            ],
        )
    ]

    return ExtractionResult(
        "tiktok_session_info",
        props.Translatable({"en": "Session information", "nl": "Sessie informatie"}),
        df,
        None,
        visualizations,
    )


def extract_direct_messages(data):
    history = get_in(data, "Direct Messages", "Chat History", "ChatHistory")
    counter = itertools.count(start=1)
    anon_ids = defaultdict(lambda: next(counter))
    # Ensure 1 is the ID of the donating user
    anon_ids[get_user_name(data)]
    table = {"Anonymous ID": [], "Sent": []}
    for item in flatten_chat_history(history):
        table["Anonymous ID"].append(anon_ids[item["From"]])
        table["Sent"].append(parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M"))

    return ExtractionResult(
        "tiktok_direct_messages",
        props.Translatable(
            {"en": "Direct Message Activity", "nl": "Berichten activiteit"}
        ),
        pd.DataFrame(table),
        None,
        None,
    )


def extract_comment_activity(data):
    comments = get_in(data, "Comment", "Comments", "CommentsList")
    if comments is None:
        return
    timestamps = [
        parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M") for item in comments
    ]

    return ExtractionResult(
        "tiktok_comment_activity",
        props.Translatable({"en": "Comment Activity", "nl": "Commentaar activiteit"}),
        pd.DataFrame({"Posted on": timestamps}),
        None,
        None,
    )


def extract_videos_liked(data):
    favorite_videos = get_in(data, "Activity", "Favorite Videos", "FavoriteVideoList")
    if favorite_videos is None:
        return
    table = {"Liked": [], "Link": []}
    for item in favorite_videos:
        table["Liked"].append(parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M"))
        table["Link"].append(item["Link"])

    return ExtractionResult(
        "tiktok_videos_liked",
        props.Translatable({"en": "Videos liked", "nl": "Gelikete videos"}),
        pd.DataFrame(table),
    )


def extract_tiktok_data(zip_file):
    extractors = [
        extract_summary_data,
        extract_video_posts,
        extract_comments_and_likes,
        extract_videos_viewed,
        extract_session_info,
        extract_direct_messages,
        extract_comment_activity,
        extract_videos_liked,
    ]
    for data in get_json_data_from_file(zip_file):
        return [
            table
            for table in (extractor(data) for extractor in extractors)
            if table is not None
        ]


######################
# Data donation flow #
######################

ExtractionResult = namedtuple(
    "ExtractionResult", ["id", "title", "data_frame", "description", "visualizations"]
)


class InvalidFileError(Exception):
    """Indicates that the file does not match expectations."""


class SkipToNextStep(Exception):
    pass


class DataDonationProcessor:
    def __init__(self, platform, mime_types, extractor, session_id):
        self.platform = platform
        self.mime_types = mime_types
        self.extractor = extractor
        self.session_id = session_id
        self.progress = 0
        self.meta_data = []

    def process(self):
        with suppress(SkipToNextStep):
            while True:
                file_result = yield from self.prompt_file()

                self.log(f"extracting file")
                try:
                    extraction_result = self.extract_data(file_result.value)
                except IOError as e:
                    self.log(f"prompt confirmation to retry file selection")
                    yield from self.prompt_retry()
                    return
                except InvalidFileError:
                    self.log(f"invalid file detected, prompting for retry")
                    if (yield from self.prompt_retry()):
                        continue
                    else:
                        return
                else:
                    if extraction_result is None:
                        try_again = yield from self.prompt_retry()
                        if try_again:
                            continue
                        else:
                            return
                    self.log(f"extraction successful, go to consent form")
                    yield from self.prompt_consent(extraction_result)

    def prompt_retry(self):
        retry_result = yield render_donation_page(
            self.platform, retry_confirmation(self.platform), self.progress
        )
        return retry_result.__type__ == "PayloadTrue"

    def prompt_file(self):
        description = props.Translatable(
            {
                "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a {self.platform} file. ",
                "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {self.platform} bestand heeft klik dan op “Overslaan” rechts onder.",
            }
        )
        prompt_file = props.PropsUIPromptFileInput(description, self.mime_types)
        file_result = yield render_donation_page(
            self.platform, prompt_file, self.progress
        )
        if file_result.__type__ != "PayloadString":
            self.log(f"skip to next step")
            raise SkipToNextStep()
        return file_result

    def log(self, message):
        self.meta_data.append(("debug", f"{self.platform}: {message}"))

    def extract_data(self, file):
        return self.extractor(file)

    def prompt_consent(self, data):
        log_title = props.Translatable({"en": "Log messages", "nl": "Log berichten"})

        tables = [
            props.PropsUIPromptConsentFormTable(
                table.id,
                table.title,
                table.data_frame,
                table.description,
                table.visualizations,
            )
            for table in data
        ]
        meta_frame = pd.DataFrame(self.meta_data, columns=["type", "message"])
        meta_table = props.PropsUIPromptConsentFormTable(
            "log_messages", log_title, meta_frame
        )
        self.log(f"prompt consent")
        consent_result = yield render_donation_page(
            self.platform,
            props.PropsUIPromptConsentForm(tables, [meta_table]),
            self.progress,
        )

        if consent_result.__type__ == "PayloadJSON":
            self.log(f"donate consent data")
            yield donate(f"{self.sessionId}-{self.platform}", consent_result.value)


class DataDonation:
    def __init__(self, platform, mime_types, extractor):
        self.platform = platform
        self.mime_types = mime_types
        self.extractor = extractor

    def __call__(self, session_id):
        processor = DataDonationProcessor(
            self.platform, self.mime_types, self.extractor, session_id
        )
        yield from processor.process()


tik_tok_data_donation = DataDonation(
    "TikTok", "application/zip, text/plain, application/json", extract_tiktok_data
)


def process(session_id):
    progress = 0
    yield donate(f"{session_id}-tracking", '[{ "message": "user entered script" }]')
    yield from tik_tok_data_donation(session_id)
    yield render_end_page()


def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": "Unfortunately, we cannot process your data. Please make sure that you selected JSON as a file format when downloading your data from TikTok.",
            "nl": "Helaas kunnen we uw gegevens niet verwerken. Zorg ervoor dat u JSON heeft geselecteerd als bestandsformaat bij het downloaden van uw gegevens van TikTok.",
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_consent(id, data, meta_data):
    table_title = props.Translatable(
        {"en": "Zip file contents", "nl": "Inhoud zip bestand"}
    )

    log_title = props.Translatable({"en": "Log messages", "nl": "Log berichten"})

    data_frame = pd.DataFrame(data, columns=["filename", "compressed size", "size"])
    table = props.PropsUIPromptConsentFormTable("zip_content", table_title, data_frame)
    meta_frame = pd.DataFrame(meta_data, columns=["type", "message"])
    meta_table = props.PropsUIPromptConsentFormTable(
        "log_messages", log_title, meta_frame
    )
    return props.PropsUIPromptConsentForm([table], [meta_table])


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        print(extract_tiktok_data(sys.argv[1]))
    else:
        print("please provide a zip file as argument")
