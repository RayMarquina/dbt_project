
from dbt import version as dbt_version
from snowplow_tracker import Subject, Tracker, AsyncEmitter, logger as sp_logger
from snowplow_tracker import SelfDescribingJson
import platform
import uuid
import yaml
import os
import json

sp_logger.setLevel(30)


COLLECTOR_URL = "events.fivetran.com/snowplow/forgiving_ain"
COLLECTOR_PROTOCOL = "https"
COOKIE_PATH = os.path.join(os.path.expanduser('~'), '.dbt/.user.yml')
INVOCATION_SPEC = "https://s3.amazonaws.com/fishtown-events/schemas/com.fishtownanalytics/invocation_event.json"
PLATFORM_SPEC = "https://s3.amazonaws.com/fishtown-events/schemas/com.fishtownanalytics/platform_context.json"

emitter = AsyncEmitter(COLLECTOR_URL, protocol=COLLECTOR_PROTOCOL)
tracker = Tracker(emitter, namespace="cf", app_id="dbt")

def __write_user():
    user = {
        "id": str(uuid.uuid4())
    }

    cookie_dir = os.path.dirname(COOKIE_PATH)
    if not os.path.exists(cookie_dir):
        os.makedirs(cookie_dir)

    with open(COOKIE_PATH, "w") as fh:
        yaml.dump(user, fh)

    return user

def get_user():
    if os.path.isfile(COOKIE_PATH):
        with open(COOKIE_PATH, "r") as fh:
            try:
                user = yaml.safe_load(fh)
                if user is None:
                    user = __write_user()
            except yaml.reader.ReaderError as e:
                user = __write_user()
    else:
        user = __write_user()

    return user

def get_invocation_id():
    pass

def get_options(args):
    exclude = ['cls', 'target', 'profile']
    options = {k:v for (k, v) in args.__dict__.items() if k not in exclude}
    return json.dumps(options)

def get_run_type(args):
    if 'dry' in args:
        return 'dry'
    else:
        return 'regular'

def get_invocation_context(invocation_id, user, project, args):
    data = {
      "project_id"    : project.hashed_name(),
      "user_id"       : user.get("id", None),
      "invocation_id" : invocation_id,

      "command"       : args.which,
      "options"       : get_options(args),
      "version"       : dbt_version,

      "run_type"      : get_run_type(args),
    }

def get_invocation_start_context(invocation_id, user, project, args):
    base_data = get_invocation_context(invocation_id, user, project, args)

    start_data = {
        "progress"    : "start",
        "result_type" : None,
        "result"      : None
    }

    data = base_data.update(start_data)
    return SelfDescribingJson(INVOCATION_SPEC, data)

def get_invocation_end_context(invocation_id, user, project, args, result_type, result):
    base_data = get_invocation_context(invocation_id, user, project, args)

    start_data = {
        "progress"    : "start",
        "result_type" : result_type,
        "result"      : result,
    }

    data = base_data.update(start_data)
    return SelfDescribingJson(INVOCATION_SPEC, data)

def get_platform_context():
    data = {
        "platform"       : platform.platform(),
        "python"         : platform.python_version(),
        "python_version" : platform.python_implementation(),
    }

    return SelfDescribingJson(PLATFORM_SPEC, data)

invocation_id = str(uuid.uuid4())
platform_context = get_platform_context()

user = get_user()
subject = Subject()
subject.set_user_id(user.get("id", None))
tracker.set_subject(subject)

def track_invocation_start(project=None, args=None):
    invocation_context = get_invocation_start_context(invocation_id, user, project, args)
    context = [invocation_context, platform_context]
    tracker.track_struct_event(category="dbt", action='invocation', label='start', context=context)

# TODO : how do we get result_type and result?
def track_invocation_end(project=None, args=None, result_type=None, result=None):
    invocation_context = get_invocation_end_context(invocation_id, user, project, args, result_type, result)
    context = [invocation_context, platform_context]
    tracker.track_struct_event(category="dbt", action='invocation', label='end', context=context)
