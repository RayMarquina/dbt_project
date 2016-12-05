
create table pre_post_run_hooks_014.on_run_hook (
    "hook_type"        TEXT, -- start|end

    "target.dbname"    TEXT,
    "target.host"      TEXT,
    "target.name"      TEXT,
    "target.schema"    TEXT,
    "target.type"      TEXT,
    "target.user"      TEXT,
    "target.pass"      TEXT,
    "target.port"      INTEGER,
    "target.threads"   TEXT,

    "run_started_at"   TEXT,
    "invocation_id"    TEXT
);
