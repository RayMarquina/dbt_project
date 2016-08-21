
from snowplow_tracker import Subject, Tracker, AsyncEmitter, logger as sp_logger
import platform


sp_logger.setLevel(30)

import hashlib

COLLECTOR_URL = "events.fivetran.com/snowplow/forgiving_ain"
COLLECTOR_PROTOCOL = "https"

emitter = AsyncEmitter(COLLECTOR_URL, protocol=COLLECTOR_PROTOCOL)
tracker = Tracker(emitter, namespace="cf", app_id="dbt")


def track_run(project_name, command_name, command_opts):
    project_name_hash = hashlib.md5(project_name).hexdigest()

    subject = Subject()
    subject.set_network_user_id(project_name_hash)
    tracker.set_subject(subject)

    sys_info = platform.platform()
    tracker.track_struct_event("dbt", command_name, command_opts, sys_info)
