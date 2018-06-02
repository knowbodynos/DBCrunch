import sys, os, pprint, operator, functools, yaml
from datetime import datetime
from pytz import utc
from time import time, sleep

def unformat_duration(duration):
    if duration == None:
        return None
    if sys.version_info[0] < 3:
        assert_types = (str, unicode)
    else:
        assert_types = str
    assert isinstance(duration, assert_types)
    days = 0
    if "-" in duration:
        daysstr, duration = duration.split("-")
        days = int(daysstr)
    hours, minutes, seconds = [int(x) for x in duration.split(":")]
    hours += days * 24
    minutes += hours * 60
    seconds += minutes * 60
    return seconds

def format_duration(duration, form = "D-HH:MM:SS"):
    if duration == None:
        return None
    assert isinstance(duration, (int, float))
    fields = {}
    seconds = duration
    fields["D"] = int(seconds / (60 * 60 * 24))
    days_rem = seconds % (60 * 60 * 24)
    fields["H"] = int(days_rem / (60 * 60))
    hours_rem = days_rem % (60 * 60)
    fields["M"] = int(hours_rem / 60)
    minutes_rem = hours_rem % 60
    fields["S"] = int(minutes_rem)
    timestamp = form.upper()
    for field, val in fields.items():
        count = timestamp.count(field)
        if count > 0:
            timestamp = timestamp.replace(count * field, str(val).zfill(count))
    return timestamp

def unformat_mem(mem):
    if mem == None:
        return None
    if sys.version_info[0] < 3:
        assert_types = (str, unicode)
    else:
        assert_types = str
    assert isinstance(mem, assert_types)
    num = ""
    unit = ""
    for c in mem:
        if c.isdigit() or c == ".":
            num += c
        else:
            unit += c
    if unit.lower() == "" or unit.lower() == "b":
        return int(num)
    elif unit.lower() in ["k", "kb"]:
        return int(num) * 1024
    elif unit.lower() in ["m", "mb"]:
        return int(num) * (1024 ** 2)
    elif unit.lower() in ["g", "gb"]:
        return int(num) * (1024 ** 3)

def format_mem(mem, unit = "MB"):
    if mem == None:
        return None
    assert isinstance(mem, (int, float))
    mem = int(mem)
    if unit.lower() in ["k", "kb"]:
        return str(int(mem / 1024)) + unit
    elif unit.lower() in ["m", "mb"]:
        return str(int(mem / (1024 ** 2))) + unit
    elif unit.lower() in ["g", "gb"]:
        return str(int(mem / (1024 ** 3))) + unit

def dir_size(start_path = "."):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.stat(fp).st_blocks * 512
            except OSError:
                pass
    return total_size

class Config(object):
    class Objectify(object):
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                if isinstance(v, dict):
                    self.__dict__[k] = Config.Objectify(**v)
                elif isinstance(v, (list, tuple)):
                    l = []
                    for vv in v:
                        if isinstance(vv, dict):
                            l.append(Config.Objectify(**vv))
                        else:
                            l.append(vv)
                    self.__dict__[k] = l
                else:
                    self.__dict__[k] = v

        def __setattr__(self, key, value):
            self.__dict__[key] = value

        def __setitem__(self, key, value):
            self.__dict__[key] = value

        def __getattr__(self, key):
            if key in self.__dict__:
                return self.__dict__[key]

        def __getitem__(self, key):
            if key in self.__dict__:
                return self.__dict__[key]

        def __delattr__(self, key):
            if key in self.__dict__:
                del self.__dict__[key]

        def __delitem__(self, key):
            if key in self.__dict__:
                del self.__dict__[key]

        def __iter__(self):
            return iter(self.__dict__.keys())

        def __repr__(self):
            return pprint.pformat(self.__dict__)

        def keys(self):
            return self.__dict__.keys()

        def to_dict(self):
            d = {}
            for k, v in self.__dict__.items():
                if isinstance(v, Config.Objectify):
                    d[k] = v.to_dict()
                elif isinstance(v, (list, tuple)):
                    l = []
                    for vv in v:
                        if isinstance(vv, Config.Objectify):
                            l.append(vv.to_dict())
                        else:
                            l.append(vv)
                    d[k] = l
                else:
                    d[k] = v
            return d

    def __init__(self, **kwargs):
        self.starttime = time()

        if "controller_path" in kwargs:
            controller_path = kwargs["controller_path"]
        else:
            controller_path = "."
        controller_path = os.path.abspath(controller_path)

        if "controller_id" in kwargs:
            controller_id = kwargs["controller_id"]
        else:
            controller_id = None

        self.cluster = self.Objectify(root = os.environ['CRUNCH_ROOT'])
        self.controller = self.Objectify(path = controller_path, id = controller_id)

        self.__debug_stream = None

        self.reload()

        #self.__dict__["stat"] = self.Objectify()

        #if self.__dict__["crunch"]["workload-manager"] == "slurm":
        #    self.__dict__["stat"].wm = __import__("crunch_slurm")

        #self.__dict__["stat"].db = self.Objectify()
        #if self.__dict__["db"].type == "mongodb":
        #    self.__dict__["stat"].db.db = __import__("mongojoin")
        #    if "username" in self.__dict__["db"] and self.__dict__["db"].username:
        #        self.__dict__["stat"].db.client = self.__dict__["stat"].db.db.MongoClient("mongodb://" + self.__dict__["db"].username + ":" + self.__dict__["db"].password + "@" + str(self.__dict__["db"].host) + ":" + str(self.__dict__["db"].port) + "/" + self.__dict__["db"].name + "?authMechanism=SCRAM-SHA-1")
        #    else:
        #        self.__dict__["stat"].db.client = self.__dict__["stat"].db.db.MongoClient("mongodb://" + str(self.__dict__["db"].host) + ":" + str(self.__dict__["db"].port) + "/" + self.__dict__["db"].name)
        #    self.__dict__["stat"].db.indexes = self.__dict__["stat"].db.db.getintersectionindexes(self.__dict__["stat"].db.client[self.__dict__["db"].name], self.__dict__["db"].basecollection)
        #else:
        #    raise Exception("Only \"mongodb\" is currently supported.")

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]

    def __delattr__(self, key):
        if key in self.__dict__:
            del self.__dict__[key]

    def __delitem__(self, key):
        if key in self.__dict__:
            del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__.keys())

    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def keys(self):
        return self.__dict__.keys()

    def set_debug(self, stream = sys.stdout):
        self.__debug_stream = stream

    def debug(self):
        if self.__debug_stream:
            self.__debug_stream.write("\n-------- Begin config --------\n")
            self.__debug_stream.write(pprint.pformat(self.__dict__, indent = 2, depth = 10))
            self.__debug_stream.write("\n-------- End config --------\n\n")
            self.__debug_stream.flush()

    def reload(self):
        controller_path = self.controller.path
        module_name, controller_name = controller_path.split("/")[-2:]

        controller_id = self.controller.id

        with open(self.cluster.root + "/crunch.config", "r") as crunch_config_stream:
            crunch_config = yaml.load(crunch_config_stream)

        with open(controller_path + "/" + module_name + "_" + controller_name + ".config", "r") as controller_config_stream:
            controller_config = yaml.load(controller_config_stream)

        # Cluster
        self.cluster = self.Objectify(**crunch_config)

        ##     Import workload manager methods

        wm_api = __import__("crunch_wm_" + self.cluster.wm.api)

        ##     User
        self.cluster.user = os.environ['USER']

        ##     Root
        self.cluster.root = os.environ['CRUNCH_ROOT']
        
        ##     Path
        self.cluster.path = [os.environ['USER_LOCAL']]

        ##     Max Controller
        if not "controller" in self.cluster:
            self.cluster.controller = self.Objectify()

        ##         Jobs
        wm_min_jobs = 0
        wm_max_jobs = wm_api.get_max_jobs()
        cluster_min_jobs = 0
        cluster_max_jobs = None
        if not "jobs" in self.cluster.controller:
            self.cluster.controller.jobs = self.Objectify()
        if isinstance(self.cluster.controller.jobs, (dict, object)):
            if "min" in self.cluster.controller.jobs and isinstance(self.cluster.controller.jobs.min, int):
                cluster_min_jobs = self.cluster.controller.jobs.min
            if "max" in self.cluster.controller.jobs and isinstance(self.cluster.controller.jobs.max, int):
                cluster_max_jobs = self.cluster.controller.jobs.max
            else:
                cluster_max_jobs = wm_max_jobs
        else:
            self.cluster.controller.jobs = self.Objectify()
        self.cluster.controller.jobs.min = max(wm_min_jobs, cluster_min_jobs)
        if not wm_max_jobs:
            self.cluster.controller.jobs.max = cluster_max_jobs
        elif not cluster_max_jobs:
            self.cluster.controller.jobs.max = wm_max_jobs
        else:
            self.cluster.controller.jobs.max = min(wm_max_jobs, cluster_max_jobs)

        ##         Steps
        wm_min_steps = 0
        wm_max_steps = wm_api.get_max_steps()
        cluster_min_steps = 0
        cluster_max_steps = None
        if not "steps" in self.cluster.controller:
            self.cluster.controller.steps = self.Objectify()
        if isinstance(self.cluster.controller.steps, (dict, object)):
            if "min" in self.cluster.controller.steps and isinstance(self.cluster.controller.steps.min, int):
                cluster_min_steps = self.cluster.controller.steps.min
            if "max" in self.cluster.controller.steps and isinstance(self.cluster.controller.steps.max, int):
                cluster_max_steps = self.cluster.controller.steps.max
            else:
                cluster_max_steps = wm_max_steps
        self.cluster.controller.steps.min = max(wm_min_steps, cluster_min_steps)
        if not wm_max_steps:
            self.cluster.controller.steps.max = cluster_max_steps
        elif not cluster_max_steps:
            self.cluster.controller.steps.max = wm_max_steps
        else:
            self.cluster.controller.steps.max = min(wm_max_steps, cluster_max_steps)

        ##         Threads
        cluster_min_threads = 0
        cluster_max_threads = None
        if not "threads" in self.cluster.controller:
            self.cluster.controller.threads = self.Objectify()
        if isinstance(self.cluster.controller.threads, (dict, object)):
            if "min" in self.cluster.controller.threads and isinstance(self.cluster.controller.threads.min, int):
                cluster_min_threads = self.cluster.controller.threads.min
            if "max" in self.cluster.controller.threads and isinstance(self.cluster.controller.threads.max, int):
                cluster_max_threads = self.cluster.controller.threads.max
        self.cluster.controller.threads.min = cluster_min_threads
        self.cluster.controller.threads.max = cluster_max_threads

        ##     Max Job
        if not "job" in self.cluster:
            self.cluster.job = self.Objectify()

        ##         Jobs
        cluster_min_jobs = 0
        cluster_max_jobs = None
        if not "jobs" in self.cluster.job:
            self.cluster.job.jobs = self.Objectify()
        if isinstance(self.cluster.job.jobs, (dict, object)):
            if "min" in self.cluster.job.jobs and isinstance(self.cluster.job.jobs.min, int):
                cluster_min_jobs = self.cluster.job.jobs.min
            if "max" in self.cluster.job.jobs and isinstance(self.cluster.job.jobs.max, int):
                cluster_max_jobs = self.cluster.job.jobs.max
            else:
                cluster_max_jobs = wm_max_jobs
        self.cluster.job.jobs.min = max(wm_min_jobs, cluster_min_jobs)
        if not wm_max_jobs:
            self.cluster.job.jobs.max = cluster_max_jobs
        elif not cluster_max_jobs:
            self.cluster.job.jobs.max = wm_max_jobs
        else:
            self.cluster.job.jobs.max = min(wm_max_jobs, cluster_max_jobs)

        ##         Steps
        cluster_min_steps = 0
        cluster_max_steps = None
        if not "steps" in self.cluster.job:
            self.cluster.job.steps = self.Objectify()
        if isinstance(self.cluster.job.steps, (dict, object)):
            if "min" in self.cluster.job.steps and isinstance(self.cluster.job.steps.min, int):
                cluster_min_steps = self.cluster.job.steps.min
            if "max" in self.cluster.job.steps and isinstance(self.cluster.job.steps.max, int):
                cluster_max_steps = self.cluster.job.steps.max
            else:
                cluster_max_steps = wm_max_steps
        self.cluster.job.steps.min = max(wm_min_steps, cluster_min_steps)
        if not wm_max_steps:
            self.cluster.job.steps.max = cluster_max_steps
        elif not cluster_max_steps:
            self.cluster.job.steps.max = wm_max_steps
        else:
            self.cluster.job.steps.max = min(wm_max_steps, cluster_max_steps)

        ##         Threads
        cluster_min_threads = 0
        cluster_max_threads = None
        if not "threads" in self.cluster.job:
            self.cluster.job.threads = self.Objectify()
        if isinstance(self.cluster.job.threads, (dict, object)):
            if "min" in self.cluster.job.threads and isinstance(self.cluster.job.threads.min, int):
                cluster_min_threads = self.cluster.job.threads.min
            if "max" in self.cluster.job.threads and isinstance(self.cluster.job.threads.max, int):
                cluster_max_threads = self.cluster.job.threads.max
        self.cluster.job.threads.min = cluster_min_threads
        self.cluster.job.threads.max = cluster_max_threads

        # Controller
        self.controller = self.Objectify(**controller_config["controller"])

        ##     Path
        self.controller.path = controller_path

        ##     ID
        self.controller.id = controller_id

        ##     Partition
        self.controller.partition = None
        if self.controller.id:
            self.controller.partition = wm_api.get_partition(self.controller.id)

        ##     Max Jobs
        cluster_min_jobs = self.cluster.controller.jobs.min
        cluster_max_jobs = self.cluster.controller.jobs.max
        job_min_jobs = 0
        job_max_jobs = None
        if not "jobs" in self.controller:
            self.controller.jobs = self.Objectify()
        if isinstance(self.controller.jobs, (dict, object)):
            if "min" in self.controller.jobs and isinstance(self.controller.jobs.min, int):
                job_min_jobs = self.controller.jobs.min
            if "max" in self.controller.jobs and isinstance(self.controller.jobs.max, int):
                job_max_jobs = self.controller.jobs.max
            else:
                job_max_jobs = wm_max_jobs
        else:
            self.controller.jobs = self.Objectify()
        self.controller.jobs.min = max(cluster_min_jobs, job_min_jobs)
        if not cluster_max_jobs:
            self.controller.jobs.max = job_max_jobs
        elif not job_max_jobs:
            self.controller.jobs.max = cluster_max_jobs
        else:
            self.controller.jobs.max = min(cluster_max_jobs, job_max_jobs)

        ##     Max Steps
        cluster_min_steps = self.cluster.controller.steps.min
        cluster_max_steps = self.cluster.controller.steps.max
        job_min_steps = 0
        job_max_steps = None
        if not "steps" in self.controller:
            self.controller.steps = self.Objectify()
        if isinstance(self.controller.steps, (dict, object)):
            if "min" in self.controller.steps and isinstance(self.controller.steps.min, int):
                job_min_steps = self.controller.steps.min
            if "max" in self.controller.steps and isinstance(self.controller.steps.max, int):
                job_max_steps = self.controller.steps.max
            else:
                job_max_steps = wm_max_steps
        else:
            self.controller.steps = self.Objectify()
        self.controller.steps.min = max(cluster_min_steps, job_min_steps)
        if not cluster_max_steps:
            self.controller.steps.max = job_max_steps
        elif not job_max_steps:
            self.controller.steps.max = cluster_max_steps
        else:
            self.controller.steps.max = min(cluster_max_steps, job_max_steps)

        ##     Max Threads
        cluster_min_threads = self.cluster.controller.threads.min
        cluster_max_threads = self.cluster.controller.threads.max
        job_min_threads = 0
        job_max_threads = None
        if not "threads" in self.controller:
            self.controller.threads = self.Objectify()
        if isinstance(self.controller.threads, (dict, object)):
            if "min" in self.controller.threads and isinstance(self.controller.threads.min, int):
                job_min_threads = self.controller.threads.min
            if "max" in self.controller.threads and isinstance(self.controller.threads.max, int):
                job_max_threads = self.controller.threads.max
        else:
            self.controller.threads = self.Objectify()
        self.controller.threads.min = max(cluster_min_threads, job_min_threads)
        if not cluster_max_threads:
            self.controller.threads.max = job_max_threads
        elif not job_max_threads:
            self.controller.threads.max = cluster_max_threads
        else:
            self.controller.threads.max = min(cluster_max_threads, job_max_threads)

        ##     Time Max
        if self.controller.partition:
            partition_timelimit = wm_api.get_partition_time_limit(self.controller.partition)
            timelimit = unformat_duration(partition_timelimit)
        else:
            timelimit = None
        if "timelimit" in self.controller:
            controller_timelimit = unformat_duration(self.controller.timelimit)
            if isinstance(self.controller.timelimit, str) and ((not timelimit) or controller_timelimit < timelimit):
                timelimit = controller_timelimit
            elif isinstance(self.controller.timelimit, (int, float)) and ((not timelimit) or self.controller.timelimit < timelimit):
                timelimit = self.controller.timelimit
        self.controller.maxtime = timelimit
        self.controller.timelimit = format_duration(timelimit)

        buffertime = 0
        if "buffertime" in self.controller:
            if isinstance(self.controller.buffertime, str):
                buffertime = unformat_duration(self.controller.buffertime)
            elif isinstance(self.controller.buffertime, (int, float)):
                buffertime = self.controller.buffertime
        self.controller.buffertime = buffertime

        # DB
        # self.db = self.Objectify()
        # for key, val in controller_config["db"].items():
        #     self.db[key] = val

        self.db = self.Objectify()
        self.db.input = self.Objectify()
        self.db.output = self.Objectify()

        if "input" in controller_config["db"] and "output" in controller_config["db"]:
            for key, val in controller_config["db"]["input"].items():
                self.db.input[key] = val
            for key, val in controller_config["db"]["output"].items():
                self.db.output[key] = val
        else:
            for key, val in controller_config["db"].items():
                if key in ["api", "name", "host", "port", "username", "password", "writeconcern", "fsync", \
                           "collections", "query", "projection", "hint", "skip", "limit", "sort", "basecollection", "nprocsfield"]:
                    self.db.input[key] = val
                if key in ["api", "name", "host", "port", "username", "password", "writeconcern", "fsync"]:
                    self.db.output[key] = val

        ##     Input
        ##         Query
        if not "query" in self.db.input or not isinstance(self.db.input.query, dict):
            self.db.input.query = {}

        ##         Projection
        if not "projection" in self.db.input or not isinstance(self.db.input.projection, dict):
            self.db.input.projection = {}
        self.db.input.projection.update({"_id": 0})

        ##         Username
        if not "username" in self.db.input or not isinstance(self.db.input.username, str):
            self.db.input.username = ""

        ##         Password
        if not "password" in self.db.input or not isinstance(self.db.input.password, str):
            self.db.input.password = ""

        ##          Hint
        if not "hint" in self.db.input or not isinstance(self.db.input.hint, dict):
            self.db.input.hint = None

        ##          Skip
        if not "skip" in self.db.input or not isinstance(self.db.input.skip, int):
            self.db.input.skip = 0

        ##          Limit
        if not "limit" in self.db.input or not isinstance(self.db.input.limit, int):
            self.db.input.limit = 0

        ##          Sort
        if not "sort" in self.db.input or not isinstance(self.db.input.sort, dict):
            self.db.input.sort = None

        ##          Write Concern
        if not "writeconcern" in self.db.input or not isinstance(self.db.input.writeconcern, str):
            self.db.input.writeconcern = "majority"

        ##          FSync
        if not "fsync" in self.db.input or not isinstance(self.db.input.fsync, bool):
            self.db.input.fsync = True

        ##     Output
        ##         Username
        if not "username" in self.db.output or not isinstance(self.db.output.username, str):
            self.db.output.username = ""

        ##         Password
        if not "password" in self.db.output or not isinstance(self.db.output.password, str):
            self.db.output.password = ""

        ##          Write Concern
        if not "writeconcern" in self.db.output or not isinstance(self.db.output.writeconcern, str):
            self.db.output.writeconcern = "majority"

        ##          FSync
        if not "fsync" in self.db.output or not isinstance(self.db.output.fsync, bool):
            self.db.output.fsync = True

        # Job
        self.job = self.Objectify(**controller_config["job"])

        ##     Max Jobs
        cluster_min_jobs = self.cluster.job.jobs.min
        cluster_max_jobs = self.cluster.job.jobs.max
        job_min_jobs = 0
        job_max_jobs = None
        if not "jobs" in self.job:
            self.job.jobs = self.Objectify()
        if isinstance(self.job.jobs, (dict, object)):
            if "min" in self.job.jobs and isinstance(self.job.jobs.min, int):
                job_min_jobs = self.job.jobs.min
            if "max" in self.job.jobs and isinstance(self.job.jobs.max, int):
                job_max_jobs = self.job.jobs.max
            else:
                job_max_jobs = wm_max_jobs
        else:
            self.job.jobs = self.Objectify()
        self.job.jobs.min = max(cluster_min_jobs, job_min_jobs)
        self.job.jobs.max = min(cluster_max_jobs, job_max_jobs)

        ##     Max Steps
        cluster_min_steps = self.cluster.job.steps.min
        cluster_max_steps = self.cluster.job.steps.max
        job_min_steps = 0
        job_max_steps = None
        if not "steps" in self.job:
            self.job.steps = self.Objectify()
        if isinstance(self.job.steps, (dict, object)):
            if "min" in self.job.steps and isinstance(self.job.steps.min, int):
                job_min_steps = self.job.steps.min
            if "max" in self.job.steps and isinstance(self.job.steps.max, int):
                job_max_steps = self.job.steps.max
            else:
                job_max_steps = wm_max_steps
        else:
            self.job.steps = self.Objectify()
        self.job.steps.min = max(cluster_min_steps, job_min_steps)
        self.job.steps.max = min(cluster_max_steps, job_max_steps)

        ##     Max Threads
        cluster_min_threads = self.cluster.job.threads.min
        cluster_max_threads = self.cluster.job.threads.max
        job_min_threads = 0
        job_max_threads = None
        if not "threads" in self.job:
            self.job.threads = self.Objectify()
        if isinstance(self.job.threads, (dict, object)):
            if "min" in self.job.threads and isinstance(self.job.threads.min, int):
                job_min_threads = self.job.threads.min
            if "max" in self.job.threads and isinstance(self.job.threads.max, int):
                job_max_threads = self.job.threads.max
        else:
            self.job.threads = self.Objectify()
        self.job.threads.min = max(cluster_min_threads, job_min_threads)
        if not cluster_max_threads:
            self.job.threads.max = job_max_threads
        elif not job_max_threads:
            self.job.threads.max = cluster_max_threads
        else:
            self.job.threads.max = min(cluster_max_threads, job_max_threads)

        ##     Time Max
        if "partition" in self.job:
            partition_timelimit = wm_api.get_partition_time_limit(self.job.partition)
            timelimit = unformat_duration(partition_timelimit)
        else:
            timelimit = None
        if "timelimit" in self.job:
            job = unformat_duration(self.job.timelimit)
            if isinstance(self.job.timelimit, str) and ((not timelimit) or job < timelimit):
                timelimit = job
            elif isinstance(self.job.timelimit, (int, float)) and ((not timelimit) or self.job.timelimit < timelimit):
                timelimit = self.job.timelimit
        self.job.maxtime = timelimit
        self.job.timelimit = format_duration(timelimit)

        buffertime = 0
        if "buffertime" in self.job:
            if isinstance(self.job.buffertime, str):
                buffertime = unformat_duration(self.job.buffertime)
            elif isinstance(self.job.buffertime, (int, float)):
                buffertime = self.job.buffertime
        self.job.buffertime = buffertime

        # Module
        self.module = self.Objectify(**controller_config["module"])

        ##     Dependencies
        with open(self.cluster.root + "/modules/modules/" + self.module.name + "/dependencies", "r") as dependencies_stream:
            self.controller.dependencies = []
            for line in dependencies_stream:
                self.controller.dependencies += [line.rstrip("\n")]

        ##     Software
        if self.module.language and self.module.language in self.cluster.software:
            software = self.cluster.software[self.module.language]
        else:
            software = self.Objectify()

        for key in ["extension", "command", "flags", "licenses", "sublicenses", "ignore"]:
            val = None
            if key in software and len(software[key]) > 0:
                val = software[key]
            self.module[key] = val

        del self.cluster.software

        # Options
        self.options = self.Objectify(**controller_config["options"])

        ##     Markdone
        if not "markdone" in self.options or not self.options.markdone:
            self.options.markdone = ""

        ##     Cleanup
        if not "cleanup" in self.options:
            self.options.cleanup = None

        ##     NRefill
        if not "nrefill" in self.options:
            self.options.nrefill = None

        ##     NIters
        if not "niters" in self.options:
            self.options.niters = None

        ##     NBatch
        if not "nbatch" in self.options:
            self.options.nbatch = None

        ##     NWorkers
        if not "nworkers" in self.options:
            self.options.nworkers = None

        self.debug()