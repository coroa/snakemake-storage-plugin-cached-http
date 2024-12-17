__author__ = "Jonas Hörsch"
__copyright__ = (
    "Copyright 2023, Christopher Tomkins-Tinch, Johannes Köster, Jonas Hörsch"
)
__email__ = "jonas.hoersch@openenergytransition.org"
__license__ = "MIT"

import os
import subprocess
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Optional

import snakemake_storage_plugin_http as http_base
import sysrsync
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_common.logging import get_logger
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface, Mtime
from snakemake_interface_storage_plugins.storage_provider import (
    StorageQueryValidationResult,
)

logger = get_logger()


# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(http_base.StorageProviderSettings):
    cache: Optional[Path] = field(
        default=None,
        metadata={
            "help": "Cache directory for checking file existence",
            "env_var": False,  # Should think about True here
        },
    )
    update: Optional[bool] = field(
        default=False, metadata={"help": "Whether to check online files for mtimes."}
    )


# Patch the original StorageProvider away, so that there is no conflict
orig_valid_query = http_base.StorageProvider.is_valid_query
http_base.StorageProvider.is_valid_query = classmethod(
    lambda c, q: StorageQueryValidationResult(
        query=q, valid=False, reason="Deactivated in favour of cached-http"
    )
)


# Required:
# Implementation of your storage provider
class StorageProvider(http_base.StorageProvider):
    def __post_init__(self):
        if self.settings.cache is not None:
            self.cache_dir = Path(self.settings.cache).expanduser()
            self.cache_dir.mkdir(exist_ok=True, parents=True)
            logger.info(f"cached-http: Will cache http(s):// urls in {self.cache_dir}")
        else:
            logger.info(
                "cached-http: Deactivated (configure a cache directory in Snakefile)"
            )
            self.cache_dir = None

    def get_storage_object_cls(self):
        return http_base.StorageObject if self.cache_dir is None else StorageObject

    is_valid_query = orig_valid_query


@dataclass
class ObjectState:
    update: bool
    exists: bool
    mtime: float
    size: int


# Required:
# Implementation of storage object (also check out
# snakemake_interface_storage_plugins.storage_object for more base class options)
class StorageObject(http_base.StorageObject):
    def __post_init__(self):
        super().__post_init__()
        self.query_path = self.provider.cache_dir / self.local_suffix()
        self.query_path.parent.mkdir(exist_ok=True, parents=True)

    @cached_property
    def state(self) -> ObjectState:
        local_exists = self.query_path.exists()
        if local_exists:
            stat = self._stat()
            if self.query_path.is_symlink():
                # get symlink stat
                lstat = self._stat(follow_symlinks=False)
            else:
                lstat = stat
            local_mtime = self._stat_to_mtime(lstat)
            local_size = stat.st_size

            if not self.provider.settings.update:
                return ObjectState(
                    update=False, exists=True, mtime=local_mtime, size=local_size
                )
        else:
            local_mtime = -1

        with self.httpr(verb="HEAD") as httpr:
            res = http_base.ResponseHandler(httpr)
            remote_mtime = res.mtime()
            remote_exists = res.exists()
            remote_size = res.size()

        if local_exists and local_mtime >= remote_mtime:
            return ObjectState(
                update=False, exists=True, mtime=local_mtime, size=local_size
            )
        elif remote_exists:
            return ObjectState(
                update=True, exists=True, mtime=remote_mtime, size=remote_size
            )
        else:
            return ObjectState(update=False, exists=False, mtime=0, size=0)

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        key = self.cache_key()

        if key in cache.exists_in_storage:
            # already inventorized, stop here
            return

        state = self.state
        cache.exists_in_storage[key] = state.exists
        cache.mtime[key] = Mtime(storage=state.mtime)
        cache.size[key] = state.size

    def _stat(self, follow_symlinks: bool = True):
        # We don't want the cached variant (Path.stat), as we cache ourselves in
        # inventory and afterwards the information may change.
        return os.stat(self.query_path, follow_symlinks=follow_symlinks)

    def _stat_to_mtime(self, stat):
        if self.query_path.is_dir():
            # use the timestamp file if possible
            timestamp = self._timestamp_path
            if timestamp.exists():
                return os.stat(timestamp, follow_symlinks=False).st_mtime
        return stat.st_mtime

    def exists(self) -> bool:
        # return True if the object exists
        return self.state.exists

    def mtime(self) -> float:
        # return the modification time
        return self.state.mtime

    def size(self) -> int:
        # return the size in bytes
        return self.state.size

    def cleanup(self):
        # nothing to be done here
        pass

    def retrieve_object(self):
        if not self.state.update:
            # Ensure that the object is accessible locally under self.local_path()
            logger.info(
                f"Retrieving {self.query_path.name} from cache dir {self.provider.cache_dir}"
            )
            cmd = sysrsync.get_rsync_command(
                str(self.query_path), str(self.local_path()), options=["-av"]
            )
            self._run_cmd(cmd)
            return

        logger.info(f"Retrieving {self.query_path.name} from url")
        super().retrieve_object()
        cmd = sysrsync.get_rsync_command(
            str(self.local_path()), str(self.query_path), options=["-av"]
        )
        logger.info(
            f"Storing {self.query_path.name} in cache dir {self.provider.cache_dir}"
        )
        self._run_cmd(cmd)

    def _run_cmd(self, cmd: list[str]):
        try:
            subprocess.run(
                cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
        except subprocess.CalledProcessError as e:
            raise WorkflowError(e.stdout.decode())